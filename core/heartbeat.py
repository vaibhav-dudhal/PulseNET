
import asyncio
import httpx

NODES = {
    1: "http://127.0.0.1:8001",
    2: "http://127.0.0.1:8002",
    3: "http://127.0.0.1:8003",
}

async def check_node(node_id: int, url: str) -> dict:
    try:
        async with httpx.AsyncClient(timeout=1.5) as client:
            response = await client.get(f"{url}/health")
            response.raise_for_status()

            return {
                "node_id": node_id,
                "url": url,
                "status": "healthy"
            }

    except (httpx.HTTPError, OSError):
        return {
            "node_id": node_id,
            "url": url,
            "status": "unhealthy"
        }

async def check_all_nodes() -> list:
    tasks = [
        check_node(node_id, url)
        for node_id, url in NODES.items()
    ]
    return await asyncio.gather(*tasks)
