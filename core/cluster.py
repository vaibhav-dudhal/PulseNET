
import asyncio
from datetime import datetime, timezone


class ClusterManager:
    def __init__(self, node_ids=(1, 2, 3)):
        self.node_ids = set(node_ids)
        self.simulated_failures = set()
        self.lock = asyncio.Lock()
        self.events = []

    def _record(self, node_id, message):
        self.events.append({
            "time": datetime.now(timezone.utc).isoformat(),
            "node_id": node_id,
            "message": message,
        })
        self.events = self.events[-100:]

    async def simulate_failure(self, node_id):
        async with self.lock:
            if node_id not in self.node_ids:
                return False, "Unknown node"

            if node_id in self.simulated_failures:
                return True, f"Node {node_id} is already marked failed"

            self.simulated_failures.add(node_id)
            self._record(node_id, f"Node {node_id} failure simulated")
            return True, f"Node {node_id} failure simulated"

    async def recover_node(self, node_id):
        async with self.lock:
            if node_id not in self.node_ids:
                return False, "Unknown node"

            self.simulated_failures.discard(node_id)
            self._record(node_id, f"Node {node_id} recovery requested")
            return True, f"Node {node_id} marked recovered"

    async def is_simulated_failed(self, node_id):
        async with self.lock:
            return node_id in self.simulated_failures

    async def get_events(self):
        async with self.lock:
            return list(self.events)


cluster_manager = ClusterManager()
