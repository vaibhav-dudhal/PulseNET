import os
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

NODE_ID = int(os.getenv("NODE_ID", "1"))
PORT = int(os.getenv("PORT", str(8000 + NODE_ID)))
INITIAL_LEADER_ID = int(os.getenv("INITIAL_LEADER_ID", "3"))
MAX_NODES = 10
NODES = {i: f"http://127.0.0.1:{8000 + i}" for i in range(1, MAX_NODES + 1)}
leader_id = INITIAL_LEADER_ID
is_available = True
events = []
election_lock = asyncio.Lock()
last_election_at = 0.0
election_cooldown = 1.0
lamport_clock = 0
# Vector-clock component i-1 represents Node i. Fixed width supports nodes 1..MAX_NODES.
vector_clock = [0] * MAX_NODES


def log_event(message):
    """Record a local event and advance this node's Lamport logical clock."""
    global lamport_clock
    lamport_clock += 1
    vector_clock[NODE_ID - 1] += 1
    event = {
        "time": datetime.now(timezone.utc).isoformat(),
        "node_id": NODE_ID,
        "message": message,
        "lamport_clock": lamport_clock,
        "vector_clock": vector_clock.copy(),
    }
    events.append(event)
    del events[:-200]
    print(f"[Node {NODE_ID}][L={lamport_clock}][V={vector_clock}] {message}", flush=True)
    return event


def receive_clock(remote_timestamp: int | None):
    """Merge a received Lamport timestamp; the next logged event ticks +1."""
    global lamport_clock
    if remote_timestamp is not None:
        lamport_clock = max(lamport_clock, remote_timestamp)


def receive_vector(remote_timestamp: list[int] | None):
    """Merge vector timestamp component-wise; log_event performs the receive tick."""
    if remote_timestamp is None:
        return
    for index in range(min(MAX_NODES, len(remote_timestamp))):
        vector_clock[index] = max(vector_clock[index], int(remote_timestamp[index]))


class ElectionMessage(BaseModel):
    sender_id: int
    lamport_timestamp: int | None = None
    vector_timestamp: list[int] | None = None


class CoordinatorMessage(BaseModel):
    leader_id: int
    lamport_timestamp: int | None = None
    vector_timestamp: list[int] | None = None


async def healthy_peer_ids(client):
    """Return IDs of currently healthy nodes, based on their health endpoints."""
    async def check_node(node_id: int):
        if node_id == NODE_ID:
            return None
        try:
            response = await client.get(f"{NODES[node_id]}/health")
            response.raise_for_status()
            return node_id
        except httpx.HTTPError:
            # A stopped or simulated-failed node is not an active cluster member.
            return None

    results = await asyncio.gather(
        *(check_node(node_id) for node_id in NODES)
    )
    return sorted(node_id for node_id in results if node_id is not None)


async def notify_coordinator(client, node_id):
    """Notify a peer already discovered as healthy; do not log absent IDs as errors."""
    if node_id == NODE_ID:
        return
    try:
        sent_event = log_event(f"Coordinator announcement sent to Node {node_id}")
        response = await client.post(
            f"{NODES[node_id]}/coordinator",
            json={"leader_id": NODE_ID, "lamport_timestamp": sent_event["lamport_clock"], "vector_timestamp": sent_event["vector_clock"]},
        )
        response.raise_for_status()
        payload = response.json()
        receive_clock(payload.get("lamport_timestamp"))
        receive_vector(payload.get("vector_timestamp"))
        log_event(f"Coordinator acknowledgement received from Node {node_id}")
    except httpx.HTTPError:
        # The peer may have failed between health discovery and notification.
        log_event(f"Node {node_id} became unavailable before coordinator notification")


async def run_election(force=False):
    global leader_id, last_election_at
    if not is_available:
        return {"message": "This node is unavailable"}

    async with election_lock:
        now = asyncio.get_running_loop().time()
        if not force and now - last_election_at < election_cooldown:
            return {"message": "Election recently attempted", "leader_id": leader_id}

        last_election_at = now
        log_event("Bully election started")

        async with httpx.AsyncClient(timeout=0.9) as client:
            # Discover actual healthy peers first. The configured address range is
            # only an address map; it does not mean every possible node exists.
            peers = await healthy_peer_ids(client)
            higher_peers = [node_id for node_id in peers if node_id > NODE_ID]

            async def contact_higher(node_id: int):
                try:
                    sent_event = log_event(f"Election request sent to Node {node_id}")
                    response = await client.post(
                        f"{NODES[node_id]}/election",
                        json={"sender_id": NODE_ID, "lamport_timestamp": sent_event["lamport_clock"], "vector_timestamp": sent_event["vector_clock"]},
                    )
                    response.raise_for_status()
                    payload = response.json()
                    receive_clock(payload.get("lamport_timestamp"))
                    receive_vector(payload.get("vector_timestamp"))
                    log_event(f"Election response received from Node {node_id}")
                    return node_id
                except httpx.HTTPError:
                    return None

            results = await asyncio.gather(
                *(contact_higher(node_id) for node_id in higher_peers)
            )
            higher_alive = sorted(node_id for node_id in results if node_id is not None)

            if higher_alive:
                log_event(
                    f"Higher nodes responded: {higher_alive}; waiting for the highest node to win"
                )
                return {
                    "message": "Higher node found; election continues",
                    "higher_nodes": higher_alive,
                    "leader_id": leader_id,
                }

            previous_leader = leader_id
            leader_id = NODE_ID
            log_event(f"Node {NODE_ID} elected as leader")

            # Notify only peers that were actually healthy during discovery.
            await asyncio.gather(
                *(notify_coordinator(client, node_id) for node_id in peers)
            )

            return {
                "message": "Election completed",
                "previous_leader": previous_leader,
                "leader_id": leader_id,
            }


async def monitor_leader():
    await asyncio.sleep(2.5)
    while True:
        await asyncio.sleep(2)
        if not is_available or leader_id == NODE_ID:
            continue
        leader_url = NODES.get(leader_id)
        if not leader_url:
            log_event(f"Unknown leader Node {leader_id}; starting election")
            await run_election(force=True)
            continue
        try:
            async with httpx.AsyncClient(timeout=0.8) as client:
                response = await client.get(f"{leader_url}/health")
                response.raise_for_status()
        except httpx.HTTPError:
            log_event(f"Leader Node {leader_id} is unavailable; starting election")
            await run_election(force=True)


async def startup_election():
    # Give peer processes time to bind their ports before the joining node challenges the leader.
    await asyncio.sleep(1.2)
    if is_available:
        await run_election(force=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    monitor_task = asyncio.create_task(monitor_leader())
    election_task = asyncio.create_task(startup_election())
    yield
    monitor_task.cancel()
    election_task.cancel()
    for task in (monitor_task, election_task):
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(title=f"PulseNet Node {NODE_ID}", lifespan=lifespan)


@app.get("/")
async def home():
    return {"message": "PulseNet node is running", "node_id": NODE_ID, "port": PORT}


@app.get("/health")
async def health():
    if not is_available:
        raise HTTPException(status_code=503, detail="Node unavailable")
    return {"node_id": NODE_ID, "node_name": f"Node-{NODE_ID}", "port": PORT, "status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/info")
async def info():
    return {"node_id": NODE_ID, "node_name": f"Node-{NODE_ID}", "port": PORT, "leader_id": leader_id, "is_leader": leader_id == NODE_ID, "status": "healthy" if is_available else "unhealthy", "lamport_clock": lamport_clock, "vector_clock": vector_clock.copy()}


@app.get("/state")
async def state():
    return {"node_id": NODE_ID, "leader_id": leader_id, "is_leader": leader_id == NODE_ID, "status": "healthy" if is_available else "unhealthy", "lamport_clock": lamport_clock, "vector_clock": vector_clock.copy(), "events": events[-50:]}


@app.get("/events")
async def get_events():
    return {"node_id": NODE_ID, "events": events[-100:]}


@app.post("/simulate-failure")
async def simulate_failure():
    global is_available
    is_available = False
    log_event("Node failure simulated from dashboard/API")
    return {"message": f"Node {NODE_ID} marked unavailable", "node_id": NODE_ID}


@app.post("/recover")
async def recover():
    global is_available
    was_unavailable = not is_available
    is_available = True
    if was_unavailable:
        log_event("Node recovered")
    asyncio.create_task(run_election(force=True))
    return {"message": f"Node {NODE_ID} is available", "node_id": NODE_ID, "leader_id": leader_id}


@app.post("/election")
async def election(message: ElectionMessage):
    if not is_available:
        raise HTTPException(status_code=503, detail="Node unavailable")
    if message.sender_id not in NODES:
        raise HTTPException(status_code=400, detail="Unknown sender node")
    receive_clock(message.lamport_timestamp)
    receive_vector(message.vector_timestamp)
    received = log_event(f"Election request received from Node {message.sender_id}")
    if NODE_ID > message.sender_id:
        asyncio.create_task(run_election(force=True))
    return {"message": f"Node {NODE_ID} acknowledged election request", "node_id": NODE_ID, "lamport_timestamp": received["lamport_clock"], "vector_timestamp": received["vector_clock"]}


@app.post("/start-election")
async def start_election():
    return await run_election(force=True)


@app.post("/coordinator")
async def coordinator(message: CoordinatorMessage):
    global leader_id
    if not is_available:
        raise HTTPException(status_code=503, detail="Node unavailable")
    receive_clock(message.lamport_timestamp)
    receive_vector(message.vector_timestamp)
    new_leader = message.leader_id
    if new_leader not in NODES:
        raise HTTPException(status_code=400, detail="Unknown coordinator")
    # Ignore a lower-ID coordinator only if the currently known higher leader
    # is still reachable. If it has failed, accepting the lower ID is necessary
    # for the Bully algorithm to converge after a leader crash.
    if new_leader < leader_id:
        try:
            async with httpx.AsyncClient(timeout=0.5) as client:
                response = await client.get(f"{NODES[leader_id]}/health")
                response.raise_for_status()
            ignored = log_event(f"Coordinator announcement from Node {new_leader} ignored; current leader is healthy")
            return {"message": "Coordinator update ignored; current leader is healthy", "leader_id": leader_id, "lamport_timestamp": ignored["lamport_clock"], "vector_timestamp": ignored["vector_clock"]}
        except httpx.HTTPError:
            log_event(f"Previously known leader Node {leader_id} is unreachable; accepting Node {new_leader}")
    previous_leader = leader_id
    leader_id = new_leader
    updated = log_event(f"Coordinator changed to Node {new_leader}") if previous_leader != new_leader else log_event(f"Coordinator announcement received from Node {new_leader}")
    return {"message": "Coordinator updated", "leader_id": leader_id, "lamport_timestamp": updated["lamport_clock"], "vector_timestamp": updated["vector_clock"]}
