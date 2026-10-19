
import os
import subprocess
import sys
import time

NODES = [
    {"id": 1, "port": 8001},
    {"id": 2, "port": 8002},
    {"id": 3, "port": 8003},
]

processes = []

try:
    for node in NODES:
        env = os.environ.copy()
        env["NODE_ID"] = str(node["id"])
        env["PORT"] = str(node["port"])

        process = subprocess.Popen([
            sys.executable,
            "-m", "uvicorn",
            "core.node:app",
            "--host", "127.0.0.1",
            "--port", str(node["port"]),
        ], env=env)

        processes.append(process)
        print(
            f"Started Node {node['id']} "
            f"on port {node['port']} "
            f"(PID: {process.pid})"
        )

    print("\nPulseNet nodes are starting.")
    print("Press Ctrl+C to stop all nodes.")

    while True:
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopping PulseNet nodes...")

finally:
    for process in processes:
        if process.poll() is None:
            process.terminate()

    for process in processes:
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()

    print("All nodes stopped.")
