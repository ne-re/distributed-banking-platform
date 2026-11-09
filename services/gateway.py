import os
import time
import threading
import logging
from typing import Dict, List, Optional
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

META_DB_DSN = os.getenv("META_DB_DSN", "postgresql://bankuser:bankpassword@db_meta:5432/meta_db")
COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://coordinator:8000")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [GATEWAY] %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Shard-Aware Banking Gateway")

gateway_lock = threading.Lock()

REPLICA_REGISTRY: Dict[str, Dict[str, dict]] = {
    "ShardA": {
        "bank_a1": {"url": "http://bank_a1:8000", "healthy": True},
        "bank_a2": {"url": "http://bank_a2:8000", "healthy": True},
    },
    "ShardB": {
        "bank_b1": {"url": "http://bank_b1:8000", "healthy": True},
        "bank_b2": {"url": "http://bank_b2:8000", "healthy": True},
    },
    "ShardC": {
        "bank_c1": {"url": "http://bank_c1:8000", "healthy": True},
        "bank_c2": {"url": "http://bank_c2:8000", "healthy": True},
    }
}

RR_COUNTERS: Dict[str, int] = {"ShardA": 0, "ShardB": 0, "ShardC": 0}

SHARD_MAP_CACHE: List[dict] = [
    {"start_acc": 1, "end_acc": 1000, "shard_id": "ShardA", "epoch": 1},
    {"start_acc": 1001, "end_acc": 2000, "shard_id": "ShardB", "epoch": 1},
]
CURRENT_EPOCH: int = 1


def parse_account_num(account_id: str) -> int:
    if not account_id or not account_id.startswith("A") or not account_id[1:].isdigit():
        raise HTTPException(status_code=400, detail=f"Invalid account ID: {account_id}")
    return int(account_id[1:])


def refresh_shard_map_from_db():
    global SHARD_MAP_CACHE, CURRENT_EPOCH
    try:
        conn = psycopg2.connect(META_DB_DSN)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT current_epoch FROM shard_map_meta WHERE id = 1;")
        meta_row = cur.fetchone()
        if meta_row:
            CURRENT_EPOCH = meta_row["current_epoch"]
        cur.execute("SELECT start_acc, end_acc, shard_id, epoch FROM shard_map ORDER BY start_acc ASC;")
        rows = cur.fetchall()
        if rows:
            SHARD_MAP_CACHE = [dict(r) for r in rows]
        cur.close()
        conn.close()
    except Exception as e:
        logger.debug(f"Shard map refresh skipped: {e}")


def resolve_shard_for_account(account_id: str) -> tuple[str, int]:
    acc_num = parse_account_num(account_id)
    for entry in SHARD_MAP_CACHE:
        if entry["start_acc"] <= acc_num <= entry["end_acc"]:
            return entry["shard_id"], CURRENT_EPOCH
    raise HTTPException(status_code=404, detail=f"Account {account_id} does not belong to any configured shard range")


def select_healthy_replica(shard_id: str) -> tuple[str, str]:
    with gateway_lock:
        replicas = REPLICA_REGISTRY.get(shard_id, {})
        healthy_list = [(rid, info["url"]) for rid, info in sorted(replicas.items()) if info["healthy"]]
        if not healthy_list:
            raise HTTPException(status_code=503, detail=f"No healthy replicas available for {shard_id}")
        idx = RR_COUNTERS.get(shard_id, 0) % len(healthy_list)
        RR_COUNTERS[shard_id] = idx + 1
        return healthy_list[idx]


def forward_to_shard(shard_id: str, method: str, path: str, json_payload: dict = None, params: dict = None, account_for_reroute: str = None):
    """
    Routes a request to a healthy replica via Round-Robin.
    Handles both replica failover AND live re-sharding cutover pauses (409/503) transparently.
    """
    current_shard = shard_id
    for attempt in range(6):
        replica_id, replica_url = select_healthy_replica(current_shard)
        try:
            if method == "GET":
                resp = requests.get(f"{replica_url}{path}", params=params, timeout=4)
            else:
                resp = requests.post(f"{replica_url}{path}", json=json_payload, params=params, timeout=4)

            # If paused for brief cutover (503) or fenced after ownership move (409), refresh Shard Map and retry
            if resp.status_code in (409, 503) and account_for_reroute:
                time.sleep(0.15)
                refresh_shard_map_from_db()
                new_shard, new_epoch = resolve_shard_for_account(account_for_reroute)
                current_shard = new_shard
                if json_payload is not None and "epoch" in json_payload:
                    json_payload["epoch"] = new_epoch
                if params is not None and "epoch" in params:
                    params["epoch"] = new_epoch
                continue

            if resp.status_code >= 400:
                detail = resp.json().get("detail", resp.text) if "application/json" in resp.headers.get("content-type", "") else resp.text
                raise HTTPException(status_code=resp.status_code, detail=detail)

            return resp.json()
        except HTTPException:
            raise
        except Exception as e:
            logger.warning(f"Replica {replica_id} ({replica_url}) failed during request: {e}. Failing over!")
            with gateway_lock:
                if current_shard in REPLICA_REGISTRY and replica_id in REPLICA_REGISTRY[current_shard]:
                    REPLICA_REGISTRY[current_shard][replica_id]["healthy"] = False

    raise HTTPException(status_code=503, detail=f"All replicas for {current_shard} failed or busy during cutover")


def health_check_worker():
    while True:
        refresh_shard_map_from_db()
        with gateway_lock:
            snapshot = {
                s: {rid: info["url"] for rid, info in reps.items()}
                for s, reps in REPLICA_REGISTRY.items()
            }
        for shard_id, reps in snapshot.items():
            for rid, url in reps.items():
                is_ok = False
                try:
                    r = requests.get(f"{url}/health", timeout=1.2)
                    is_ok = (r.status_code == 200)
                except Exception:
                    is_ok = False
                with gateway_lock:
                    if shard_id in REPLICA_REGISTRY and rid in REPLICA_REGISTRY[shard_id]:
                        prev = REPLICA_REGISTRY[shard_id][rid]["healthy"]
                        if prev != is_ok:
                            logger.info(f"HEALTH CHANGE: {shard_id}/{rid} healthy={is_ok}")
                        REPLICA_REGISTRY[shard_id][rid]["healthy"] = is_ok
        time.sleep(1.5)


@app.on_event("startup")
def on_startup():
    refresh_shard_map_from_db()
    t = threading.Thread(target=health_check_worker, daemon=True)
    t.start()


class ClientAmountRequest(BaseModel):
    account_id: str
    amount: int
    request_id: Optional[str] = None


class ClientTransferRequest(BaseModel):
    from_account: str
    to_account: str
    amount: int
    request_id: Optional[str] = None
    pause_after_commit_decide_sec: float = 0.0
    simulate_crash_after_commit_decide: bool = False


@app.get("/balance/{account_id}")
def client_balance(account_id: str):
    shard_id, epoch = resolve_shard_for_account(account_id)
    return forward_to_shard(shard_id, "GET", f"/balance/{account_id}", params={"epoch": epoch}, account_for_reroute=account_id)


@app.post("/deposit")
def client_deposit(req: ClientAmountRequest):
    shard_id, epoch = resolve_shard_for_account(req.account_id)
    return forward_to_shard(shard_id, "POST", "/deposit", {
        "account_id": req.account_id,
        "amount": req.amount,
        "epoch": epoch,
        "request_id": req.request_id
    }, account_for_reroute=req.account_id)


@app.post("/withdraw")
def client_withdraw(req: ClientAmountRequest):
    shard_id, epoch = resolve_shard_for_account(req.account_id)
    return forward_to_shard(shard_id, "POST", "/withdraw", {
        "account_id": req.account_id,
        "amount": req.amount,
        "epoch": epoch,
        "request_id": req.request_id
    }, account_for_reroute=req.account_id)


@app.post("/transfer")
def client_transfer(req: ClientTransferRequest):
    for attempt in range(4):
        from_shard, epoch1 = resolve_shard_for_account(req.from_account)
        to_shard, epoch2 = resolve_shard_for_account(req.to_account)
        epoch = max(epoch1, epoch2)

        if from_shard == to_shard:
            logger.info(f"Routing SAME-SHARD transfer {req.from_account}->{req.to_account} to {from_shard}")
            try:
                return forward_to_shard(from_shard, "POST", "/local_transfer", {
                    "from_account": req.from_account,
                    "to_account": req.to_account,
                    "amount": req.amount,
                    "epoch": epoch,
                    "request_id": req.request_id
                })
            except HTTPException as he:
                if he.status_code in (409, 503) and attempt < 3:
                    time.sleep(0.2)
                    refresh_shard_map_from_db()
                    continue
                raise
        else:
            logger.info(f"Routing CROSS-SHARD transfer {req.from_account} ({from_shard}) -> {req.to_account} ({to_shard}) via Coordinator 2PC")
            try:
                r = requests.post(f"{COORDINATOR_URL}/2pc/transfer", json={
                    "from_account": req.from_account,
                    "to_account": req.to_account,
                    "from_shard": from_shard,
                    "to_shard": to_shard,
                    "amount": req.amount,
                    "epoch": epoch,
                    "request_id": req.request_id,
                    "pause_after_commit_decide_sec": req.pause_after_commit_decide_sec,
                    "simulate_crash_after_commit_decide": req.simulate_crash_after_commit_decide
                }, timeout=15)
            except Exception as e:
                raise HTTPException(
                    status_code=503,
                    detail=f"Coordinator interrupted or unreachable during 2PC: {e}. Query /transactions status."
                )

            if r.status_code >= 400:
                detail = r.json().get("detail", r.text) if "application/json" in r.headers.get("content-type", "") else r.text
                if ("FENCED" in str(detail) or "CUTOVER" in str(detail) or "STALE_EPOCH" in str(detail)) and attempt < 3:
                    time.sleep(0.2)
                    refresh_shard_map_from_db()
                    continue
                raise HTTPException(status_code=r.status_code, detail=detail)
            return r.json()


@app.get("/transactions/{tx_id}")
def query_tx_status(tx_id: str):
    try:
        r = requests.get(f"{COORDINATOR_URL}/transactions/{tx_id}", timeout=4)
        return r.json()
    except Exception as e:
        return {"tx_id": tx_id, "query_status": "PENDING", "note": f"Coordinator currently unreachable: {e}"}


@app.get("/internal/select_replica/{shard_id}")
def internal_select_replica(shard_id: str):
    rid, url = select_healthy_replica(shard_id)
    return {"shard_id": shard_id, "replica_id": rid, "replica_url": url}


@app.post("/admin/refresh_map")
def admin_refresh_map():
    refresh_shard_map_from_db()
    return {"status": "refreshed", "epoch": CURRENT_EPOCH, "shard_map": SHARD_MAP_CACHE}


class ReplicaRegisterRequest(BaseModel):
    shard_id: str
    replica_id: str
    url: str


@app.post("/admin/replicas/register")
def register_replica(req: ReplicaRegisterRequest):
    with gateway_lock:
        if req.shard_id not in REPLICA_REGISTRY:
            REPLICA_REGISTRY[req.shard_id] = {}
        REPLICA_REGISTRY[req.shard_id][req.replica_id] = {"url": req.url, "healthy": True}
    logger.info(f"REGISTERED REPLICA: {req.shard_id}/{req.replica_id} at {req.url}")
    return {"status": "registered", "replicas": REPLICA_REGISTRY[req.shard_id]}


@app.post("/admin/replicas/deregister")
def deregister_replica(shard_id: str, replica_id: str):
    with gateway_lock:
        if shard_id in REPLICA_REGISTRY and replica_id in REPLICA_REGISTRY[shard_id]:
            del REPLICA_REGISTRY[shard_id][replica_id]
    logger.info(f"DEREGISTERED REPLICA: {shard_id}/{replica_id}")
    return {"status": "deregistered"}


@app.get("/admin/status")
def gateway_status():
    refresh_shard_map_from_db()
    with gateway_lock:
        return {
            "epoch": CURRENT_EPOCH,
            "shard_map": SHARD_MAP_CACHE,
            "replicas": REPLICA_REGISTRY
        }