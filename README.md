# PulseNet — Web-Controlled Self-Healing Distributed Network

PulseNet lets you demonstrate the Bully leader-election algorithm from a browser without typing cluster commands into a terminal.

## Start the demo

1. Make sure Python 3.10+ is installed.
2. Double-click `launch_pulsenet.bat` from this folder.
3. The dashboard opens at **http://127.0.0.1:8100**.

If dependencies are not installed, open a terminal in this folder once and run:

```powershell
python -m pip install -r requirements.txt
```

The launcher uses `.venv\Scripts\python.exe` if this project's virtual environment exists; otherwise it uses `python`.

## Demonstration controls

- **Start Cluster** — starts Nodes 1, 2, and 3.
- **Simulate Failure** — marks a node unavailable while leaving its process running, demonstrating health-check failure and automatic election.
- **Recover Node** — restores a simulated-failure node and triggers a new election.
- **Remove Process** — stops the selected node process completely and frees its port; remaining nodes elect a new coordinator.
- **Add Node** — starts the next available node ID. A higher-ID joining node can become leader.
- **Run Election** — explicitly triggers the Bully election.
- **Stop Cluster** — stops every node started/managed by the dashboard.

The page refreshes status and the event log automatically.

## Ports

- Dashboard: **8100** (avoids conflicting with Distributed Classroom on port 8000)
- Node 1: 8001
- Node 2: 8002
- Node 3: 8003
- Additional nodes: 8004–8010

If a node port is already occupied by an old process, PulseNet reports the conflict instead of killing an unrelated process. Stop only the old PulseNet process using that node port.

## Technical notes

- Dashboard API and process manager: `main.py`
- Node service and Bully election logic: `core/node.py`
- Web interface: `dashboard/index.html`, `dashboard/style.css`, `dashboard/app.js`
- Per-node logs: `logs/node_<id>.log`
- `run_cluster.py` remains available for terminal-based use, but is not needed for the web demonstration.

This is a local demo cluster bound to `127.0.0.1`; it is not intended to be exposed directly to a public network.

## Election reliability fix

Higher-node election requests are sent concurrently, rather than waiting sequentially for each unused node ID. The dashboard also allows enough time for an election response. This prevents a false “Election request failed” message when only some node IDs are active.

## Live topology panel

The Live Network Topology panel is rendered in the browser using SVG and the current `/api/cluster` status. It shows all managed nodes, marks the current coordinator, distinguishes healthy and unavailable nodes, and refreshes alongside the dashboard. The election-flow strip summarizes recent election events; it is an explanatory view of the event log, not a separate election engine.

## Clean launch

The ZIP intentionally excludes Python bytecode caches and old node logs. Fresh logs are created in `logs/` when you start the cluster.


## Lamport logical clocks

PulseNet includes per-node Lamport logical clocks integrated with the Bully election message flow.

- Each node increments its logical counter when recording a local event.
- Election requests and coordinator announcements carry the sender's `lamport_timestamp`.
- On receipt, a node merges the remote timestamp and advances on the receive event, implementing `C = max(C, T) + 1`.
- `/info` exposes each node's current `lamport_clock`; `/state` and `/events` expose event-level clock values.
- The dashboard's **Lamport Logical Clock Monitor and Vector Clock Monitor** shows the current counter for each node, and the System Event Log displays `L=<counter>` for node events.

Logical timestamps represent event ordering, not real-world time. They preserve the happened-before constraint for message-related events; timestamps alone do not establish causality between independent concurrent events. Manager-only dashboard events do not have a node Lamport timestamp.

## Demonstrating Lamport clocks

1. Start the cluster and watch the logical counters on the node cards and Lamport Clock Monitor.
2. Click **Run Election** and observe counters increase as election requests and coordinator announcements are sent and received.
3. Simulate a node failure, recover it, or add a node. The event log should show logical timestamps alongside node events.
4. Compare a receiving node's clock before and after a message; its receive event must advance to at least the incoming timestamp plus one.
