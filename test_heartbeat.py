
import asyncio
from core.heartbeat import check_all_nodes

async def main():
    results = await check_all_nodes()

    print("\nPULSENET NODE HEALTH")
    print("-" * 30)

    for node in results:
        print(
            f"Node {node['node_id']}: "
            f"{node['status'].upper()}"
        )

if __name__ == "__main__":
    asyncio.run(main())
