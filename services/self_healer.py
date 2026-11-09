import os
import time
import threading
import logging
import requests
import docker
from fastapi import FastAPI
from pydantic import BaseModel

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://gateway:8000")
DOCKER_NETWORK = os.getenv("DOCKER_NETWORK", "distributed_banking_bank_net")
IMAGE_NAME = os.getenv("IMAGE_NAME", "distributed_banking_app:latest")
AUTO_HEAL_ENABLED = os.getenv("AUTO_HEAL_ENABLED", "false").lower() == "true"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SELF-HEALER] %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Autonomous Self-Healing Controller")

SHARD_CONFIGS = {
    "ShardA": {
        "desired_replicas": 2,
        "db_dsn": "postgresql://bankuser:bankpassword@db_a:5432/shard_a_db",
        "start_acc": 1,
        "end_acc": 1000
    },
    "ShardB": {
        "desired_replicas": 2,
        "db_dsn": "postgresql://bankuser:bankpassword@db_b:5432/shard_b_db",
        "start_acc": 1001,
        "end_acc": 2000
    },
    "ShardC": {
        "desired_replicas": 2,
        "db_dsn": "postgresql://bankuser:bankpassword@db_c:5432/shard_c_db",
        "start_acc": 0,
        "end_acc": 0
    }
}

healer_state = {
    "enabled": AUTO_HEAL_ENABLED,
    "events": []
}


def check_container_http_health(container_name: str) -> bool:
    try:
        r = requests.get(f"http://{container_name}:8000/health", timeout=1.5)
        return r.status_code == 200
    except Exception:
        return False


def reconcile_and_heal():
    """
    Ground-truth reconciliation loop:
    1. Queries Docker daemon for all containers with label `app=bank_replica`.
    2. Identifies healthy running containers vs stopped/failed containers.
    3. Deregisters dead containers from the Gateway and removes them.
    4. If healthy count < desired_replicas, spawns a new replacement container,
       waits for its `/health` endpoint to respond, and registers it with the Gateway.
    """
    client = docker.from_env()
    all_containers = client.containers.list(all=True, filters={"label": "app=bank_replica"})

    by_shard = {"ShardA": [], "ShardB": [], "ShardC": []}
    for c in all_containers:
        s_id = c.labels.get("shard_id")
        if s_id in by_shard:
            by_shard[s_id].append(c)

    for shard_id, cfg in SHARD_CONFIGS.items():
        containers = by_shard.get(shard_id, [])
        healthy_containers = []
        failed_containers = []

        for c in containers:
            c.reload()
            if c.status == "running" and check_container_http_health(c.name):
                healthy_containers.append(c)
                # Ensure Gateway knows about this healthy replica (handles controller restart reconciliation)
                try:
                    requests.post(f"{GATEWAY_URL}/admin/replicas/register", json={
                        "shard_id": shard_id,
                        "replica_id": c.name,
                        "url": f"http://{c.name}:8000"
                    }, timeout=2)
                except Exception:
                    pass
            else:
                failed_containers.append(c)

        # Clean up failed containers and deregister from Gateway
        for dead in failed_containers:
            logger.warning(f"Detected failed/stopped replica container {dead.name} (status={dead.status}) on {shard_id}")
            try:
                requests.post(f"{GATEWAY_URL}/admin/replicas/deregister", params={
                    "shard_id": shard_id, "replica_id": dead.name
                }, timeout=2)
            except Exception:
                pass
            try:
                dead.remove(force=True)
            except Exception as e:
                logger.warning(f"Could not remove dead container {dead.name}: {e}")

        # Spawn replacement replicas if below desired count (without creating duplicates)
        deficit = cfg["desired_replicas"] - len(healthy_containers)
        for _ in range(deficit):
            new_name = f"bank_{shard_id[-1].lower()}_healed_{int(time.time() * 1000) % 100000}"
            logger.info(f"Spawning replacement container {new_name} for {shard_id}...")
            new_c = client.containers.run(
                IMAGE_NAME,
                command="uvicorn services.shard_server:app --host 0.0.0.0 --port 8000",
                name=new_name,
                detach=True,
                network=DOCKER_NETWORK,
                environment={
                    "SHARD_ID": shard_id,
                    "REPLICA_ID": new_name,
                    "DB_DSN": cfg["db_dsn"],
                    "INITIAL_START_ACC": str(cfg["start_acc"]),
                    "INITIAL_END_ACC": str(cfg["end_acc"]),
                    "INITIAL_BALANCE": "0"
                },
                labels={
                    "app": "bank_replica",
                    "shard_id": shard_id,
                    "replica_id": new_name
                }
            )
            # Wait for new replica to become healthy before registering with Gateway
            for _ in range(15):
                if check_container_http_health(new_name):
                    break
                time.sleep(0.5)

            requests.post(f"{GATEWAY_URL}/admin/replicas/register", json={
                "shard_id": shard_id,
                "replica_id": new_name,
                "url": f"http://{new_name}:8000"
            }, timeout=3)

            event = {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "shard_id": shard_id,
                "spawned_replica": new_name
            }
            healer_state["events"].append(event)
            logger.info(f"SELF-HEALING COMPLETE: Registered {new_name} with Gateway for {shard_id}")


def healer_loop():
    while True:
        if healer_state["enabled"]:
            try:
                reconcile_and_heal()
            except Exception as e:
                logger.warning(f"Self-healer loop error: {e}")
        time.sleep(2.0)


@app.on_event("startup")
def on_startup():
    t = threading.Thread(target=healer_loop, daemon=True)
    t.start()


class ToggleRequest(BaseModel):
    enabled: bool


@app.post("/toggle")
def toggle_healer(req: ToggleRequest):
    healer_state["enabled"] = req.enabled
    logger.info(f"Self-Healer enabled set to {req.enabled}")
    return {"status": "ok", "enabled": healer_state["enabled"]}


@app.get("/status")
def status():
    return healer_state