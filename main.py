"""PulseNet dashboard API and local cluster process manager."""
import asyncio
import os
import socket
import subprocess
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

BASE_DIR = Path(__file__).resolve().parent
DASHBOARD_DIR = BASE_DIR / "dashboard"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
MIN_NODE_ID = 1
MAX_NODES = 10
NEXT_NODE_ID = 4  # Monotonic IDs: removing a node never silently reuses its identity.
BASE_PORT = 8000  # Node ports are BASE_PORT + node ID (8001, 8002, ...)
DASHBOARD_PORT = int(os.getenv("PULSENET_DASHBOARD_PORT", "8100"))
NODE_PROCESSES: dict[int, subprocess.Popen] = {}
NODE_PORTS: dict[int, int] = {}
MANAGER_EVENTS: list[dict] = []
CLUSTER_LOCK = asyncio.Lock()


def node_url(node_id: int) -> str:
    return f"http://127.0.0.1:{NODE_PORTS.get(node_id, BASE_PORT + node_id)}"


def record_event(node_id: int | None, message: str):
    MANAGER_EVENTS.append({
        "time": datetime.now(timezone.utc).isoformat(),
        "node_id": node_id or 0,
        "message": message,
    })
    del MANAGER_EVENTS[:-200]


def port_is_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.25)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def _launch_node(node_id: int, initial_leader: int) -> dict:
    port = BASE_PORT + node_id
    if port_is_open(port):
        raise HTTPException(
            status_code=409,
            detail=f"Port {port} is already in use. Stop the old PulseNet process first, then retry.",
        )

    env = os.environ.copy()
    env["NODE_ID"] = str(node_id)
    env["PORT"] = str(port)
    env["INITIAL_LEADER_ID"] = str(initial_leader)
    log_path = LOG_DIR / f"node_{node_id}.log"
    log_file = open(log_path, "a", encoding="utf-8")
    kwargs = {}
    if os.name == "nt":
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    try:
        process = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "core.node:app", "--host", "127.0.0.1", "--port", str(port)],
            cwd=str(BASE_DIR), env=env, stdin=subprocess.DEVNULL,
            stdout=log_file, stderr=subprocess.STDOUT, **kwargs,
        )
    except Exception:
        log_file.close()
        raise
    # Keep the file handle attached to the Popen object until the child exits.
    process._pulsenet_log_file = log_file
    NODE_PROCESSES[node_id] = process
    NODE_PORTS[node_id] = port
    record_event(node_id, f"Node {node_id} process started on port {port}")
    return {"id": node_id, "port": port, "pid": process.pid, "status": "starting"}


async def _wait_until_ready(node_id: int, timeout: float = 8.0) -> bool:
    deadline = asyncio.get_running_loop().time() + timeout
    async with httpx.AsyncClient(timeout=0.5) as client:
        while asyncio.get_running_loop().time() < deadline:
            process = NODE_PROCESSES.get(node_id)
            if process is None or process.poll() is not None:
                return False
            try:
                r = await client.get(f"{node_url(node_id)}/info")
                if r.status_code == 200:
                    return True
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.2)
    return False


async def _start_nodes(node_ids: list[int], initial_leader: int | None = None):
    if not node_ids:
        return []
    target_leader = initial_leader or max(node_ids)
    started = []
    for node_id in sorted(node_ids):
        proc = NODE_PROCESSES.get(node_id)
        if proc and proc.poll() is None:
            continue
        NODE_PROCESSES.pop(node_id, None)
        NODE_PORTS.pop(node_id, None)
        started.append(_launch_node(node_id, target_leader))
        await asyncio.sleep(0.15)
    for item in started:
        ready = await _wait_until_ready(item["id"])
        item["status"] = "healthy" if ready else "starting"
        if not ready:
            record_event(item["id"], f"Node {item['id']} did not become ready; check logs/node_{item['id']}.log")
    # Push a consistent coordinator state to all ready nodes.
    ready_ids = await _healthy_node_ids()
    if ready_ids:
        elected = max(ready_ids)
        await _broadcast_coordinator(elected, ready_ids)
    return started


async def _healthy_node_ids() -> list[int]:
    ids = []
    async with httpx.AsyncClient(timeout=0.6) as client:
        for node_id, proc in list(NODE_PROCESSES.items()):
            if proc.poll() is not None:
                continue
            try:
                r = await client.get(f"{node_url(node_id)}/info")
                if r.status_code == 200 and r.json().get("status") == "healthy":
                    ids.append(node_id)
            except (httpx.HTTPError, ValueError):
                pass
    return ids


async def _broadcast_coordinator(leader_id: int, node_ids: list[int] | None = None):
    ids = node_ids if node_ids is not None else list(NODE_PROCESSES)
    async with httpx.AsyncClient(timeout=1.0) as client:
        for node_id in ids:
            if node_id == leader_id:
                continue
            try:
                r = await client.post(f"{node_url(node_id)}/coordinator", json={"leader_id": leader_id})
                if r.is_success:
                    pass
            except httpx.HTTPError:
                pass


async def _stop_node_process(node_id: int, reason: str = "stopped") -> bool:
    proc = NODE_PROCESSES.get(node_id)
    if not proc:
        return False
    if proc.poll() is None:
        proc.terminate()
        try:
            await asyncio.to_thread(proc.wait, 3)
        except subprocess.TimeoutExpired:
            proc.kill()
            await asyncio.to_thread(proc.wait, 2)
    handle = getattr(proc, "_pulsenet_log_file", None)
    if handle:
        try:
            handle.close()
        except Exception:
            pass
    NODE_PROCESSES.pop(node_id, None)
    NODE_PORTS.pop(node_id, None)
    record_event(node_id, f"Node {node_id} process {reason}")
    return True


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    for node_id in list(NODE_PROCESSES):
        await _stop_node_process(node_id, "stopped during dashboard shutdown")


BUILD_VERSION = "vector-clock-monitor-2026.10.14.2"
app = FastAPI(title="PulseNet Dashboard", version=BUILD_VERSION, lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(DASHBOARD_DIR)), name="static")


@app.get("/api/build")
async def build_info():
    return {"build": BUILD_VERSION, "vector_clock_monitor": True}


@app.get("/")
async def dashboard():
    # Disable browser/proxy caching so a newly extracted project cannot silently
    # keep showing a previous dashboard's HTML. Static assets use versioned URLs.
    return FileResponse(
        DASHBOARD_DIR / "index.html",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0", "Pragma": "no-cache"},
    )


@app.get("/api/cluster")
async def cluster_status():
    node_items = []
    async with httpx.AsyncClient(timeout=0.8) as client:
        for node_id in sorted(NODE_PROCESSES):
            proc = NODE_PROCESSES[node_id]
            item = {"id": node_id, "port": NODE_PORTS[node_id], "pid": proc.pid, "status": "stopped" if proc.poll() is not None else "starting", "leader_id": None, "is_leader": False, "lamport_clock": None, "vector_clock": None}
            if proc.poll() is None:
                try:
                    response = await client.get(f"{node_url(node_id)}/info")
                    if response.status_code == 200:
                        info = response.json()
                        item.update({"status": info.get("status", "healthy"), "leader_id": info.get("leader_id"), "is_leader": info.get("is_leader", False), "lamport_clock": info.get("lamport_clock", 0), "vector_clock": info.get("vector_clock", [])})
                except (httpx.HTTPError, ValueError):
                    item["status"] = "starting"
            node_items.append(item)
    healthy = sum(1 for n in node_items if n["status"] == "healthy")
    leaders = [n for n in node_items if n["status"] == "healthy" and n["is_leader"]]
    leader_text = f"Node {leaders[0]['id']}" if len(leaders) == 1 else ("Conflict" if len(leaders) > 1 else "Unavailable")
    return {"running": bool(NODE_PROCESSES), "nodes": node_items, "total": len(node_items), "healthy": healthy, "leader": leader_text, "max_nodes": MAX_NODES}


@app.post("/api/cluster/start")
async def start_cluster():
    async with CLUSTER_LOCK:
        active = [i for i, proc in NODE_PROCESSES.items() if proc.poll() is None]
        if active:
            return {"message": "Cluster is already running", "started": [], "active_nodes": sorted(active)}
        # Default demo cluster; if a user previously used more nodes, start 1-3.
        started = await _start_nodes([1, 2, 3], initial_leader=3)
        record_event(None, "Cluster started from the web dashboard")
        return {"message": "Cluster start requested", "started": started}


@app.post("/api/cluster/stop")
async def stop_cluster():
    async with CLUSTER_LOCK:
        ids = list(NODE_PROCESSES)
        for node_id in ids:
            await _stop_node_process(node_id, "stopped from the dashboard")
        record_event(None, "Cluster stopped from the web dashboard")
        return {"message": "Cluster stopped", "stopped_nodes": ids}


@app.post("/api/nodes/add")
async def add_node():
    global NEXT_NODE_ID
    async with CLUSTER_LOCK:
        active_ids = [i for i, proc in NODE_PROCESSES.items() if proc.poll() is None]
        if not active_ids:
            raise HTTPException(status_code=409, detail="Start the cluster before adding nodes.")
        used = set(NODE_PROCESSES)
        node_id = next((i for i in range(NEXT_NODE_ID, MAX_NODES + 1) if i not in used), None)
        if node_id is None:
            raise HTTPException(status_code=409, detail=f"No new node IDs remain (maximum ID is {MAX_NODES}). Stop and restart the cluster to reset the demo, or increase MAX_NODES.")
        NEXT_NODE_ID = node_id + 1
        current_healthy = await _healthy_node_ids()
        leader_id = max(current_healthy) if current_healthy else max(active_ids)
        item = _launch_node(node_id, leader_id)
        ready = await _wait_until_ready(node_id)
        item["status"] = "healthy" if ready else "starting"
        if not ready:
            raise HTTPException(status_code=500, detail=f"Node {node_id} did not start. Check logs/node_{node_id}.log.")
        record_event(node_id, f"Node {node_id} added from the dashboard")
        # A newly joined higher-ID node starts an election, as expected in the Bully algorithm.
        try:
            async with httpx.AsyncClient(timeout=1.5) as client:
                await client.post(f"{node_url(node_id)}/start-election")
        except httpx.HTTPError:
            pass
        return {"message": f"Node {node_id} added", "node": item}


@app.post("/api/nodes/{node_id}/failure")
async def simulate_node_failure(node_id: int):
    if node_id not in NODE_PROCESSES or NODE_PROCESSES[node_id].poll() is not None:
        raise HTTPException(status_code=404, detail=f"Node {node_id} is not running.")
    try:
        async with httpx.AsyncClient(timeout=1.5) as client:
            response = await client.post(f"{node_url(node_id)}/simulate-failure")
            response.raise_for_status()
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Could not contact Node {node_id}.") from exc
    record_event(node_id, f"Node {node_id} failure simulated from the dashboard")
    return {"message": f"Node {node_id} marked unavailable. Leader election will run automatically if required."}


@app.post("/api/nodes/{node_id}/recover")
async def recover_node(node_id: int):
    if node_id not in NODE_PROCESSES or NODE_PROCESSES[node_id].poll() is not None:
        raise HTTPException(status_code=404, detail=f"Node {node_id} is not running.")
    try:
        async with httpx.AsyncClient(timeout=1.5) as client:
            response = await client.post(f"{node_url(node_id)}/recover")
            response.raise_for_status()
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Could not recover Node {node_id}.") from exc
    record_event(node_id, f"Node {node_id} recovered from the dashboard")
    # A recovered node should participate in a fresh Bully election.
    try:
        async with httpx.AsyncClient(timeout=1.5) as client:
            await client.post(f"{node_url(node_id)}/start-election")
    except httpx.HTTPError:
        pass
    return {"message": f"Node {node_id} recovered. Election requested."}


@app.delete("/api/nodes/{node_id}")
async def remove_node(node_id: int):
    async with CLUSTER_LOCK:
        if node_id not in NODE_PROCESSES:
            raise HTTPException(status_code=404, detail=f"Node {node_id} is not managed by this dashboard.")
        await _stop_node_process(node_id, "removed from the dashboard")
        record_event(node_id, f"Node {node_id} removed; remaining nodes will elect a leader if needed")
        # Request election from the highest remaining healthy node.
        remaining = await _healthy_node_ids()
        if remaining:
            candidate = max(remaining)
            try:
                async with httpx.AsyncClient(timeout=1.5) as client:
                    await client.post(f"{node_url(candidate)}/start-election")
            except httpx.HTTPError:
                pass
        return {"message": f"Node {node_id} removed. The remaining nodes will elect a leader if needed."}


@app.post("/api/election")
async def trigger_election():
    healthy = await _healthy_node_ids()
    if not healthy:
        raise HTTPException(status_code=409, detail="No healthy nodes are available to run an election.")
    candidate = max(healthy)
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            response = await client.post(f"{node_url(candidate)}/start-election")
            response.raise_for_status()
            data = response.json()
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail="Election request failed.") from exc
    record_event(candidate, "Manual Bully election triggered from the dashboard")
    return {"message": "Election request sent", "result": data}


@app.get("/api/events")
async def aggregate_events():
    gathered = list(MANAGER_EVENTS)
    async with httpx.AsyncClient(timeout=0.8) as client:
        async def fetch_events(node_id: int):
            try:
                response = await client.get(f"{node_url(node_id)}/events")
                response.raise_for_status()
                return response.json().get("events", [])
            except (httpx.HTTPError, ValueError):
                return []
        batches = await asyncio.gather(*(fetch_events(i) for i in list(NODE_PROCESSES)))
    for batch in batches:
        gathered.extend(batch)
    gathered.sort(key=lambda event: event.get("time", ""))
    # De-duplicate identical node events collected from manager and node logs.
    unique = []
    seen = set()
    for event in gathered:
        key = (event.get("time"), event.get("node_id"), event.get("message"))
        if key not in seen:
            seen.add(key)
            unique.append(event)
    return {"events": unique[-150:]}
