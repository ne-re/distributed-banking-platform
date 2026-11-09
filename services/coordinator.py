import os
import uuid
import time
import threading
import logging
from typing import Optional
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

META_DB_DSN = os.getenv("META_DB_DSN", "postgresql://bankuser:bankpassword@db_meta:5432/meta_db")
GATEWAY_URL = os.getenv("GATEWAY_URL", "http://gateway:8000")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [COORDINATOR] %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Distributed Transaction & Migration Coordinator")


def get_meta_conn(autocommit: bool = False):
    conn = psycopg2.connect(META_DB_DSN)
    conn.autocommit = autocommit
    return conn


# =====================================================================
# INITIALIZE COORDINATOR & SHARD MAP METADATA TABLES
# =====================================================================
def init_meta_tables():
    for attempt in range(15):
        try:
            conn = get_meta_conn(autocommit=False)
            cur = conn.cursor()
            cur.execute("SELECT pg_advisory_xact_lock(888222);")

            # 1. Shard Map Epoch tracker
            cur.execute("""
                CREATE TABLE IF NOT EXISTS shard_map_meta (
                    id INT PRIMARY KEY DEFAULT 1,
                    current_epoch INT NOT NULL DEFAULT 1
                );
            """)
            cur.execute("INSERT INTO shard_map_meta (id, current_epoch) VALUES (1, 1) ON CONFLICT DO NOTHING;")

            # 2. Shard Map Ranges
            cur.execute("""
                CREATE TABLE IF NOT EXISTS shard_map (
                    range_id SERIAL PRIMARY KEY,
                    start_acc INT NOT NULL,
                    end_acc INT NOT NULL,
                    shard_id VARCHAR(32) NOT NULL,
                    epoch INT NOT NULL DEFAULT 1
                );
            """)
            cur.execute("SELECT COUNT(*) FROM shard_map;")
            if cur.fetchone()[0] == 0:
                cur.execute("""
                    INSERT INTO shard_map (start_acc, end_acc, shard_id, epoch) VALUES
                    (1, 1000, 'ShardA', 1),
                    (1001, 2000, 'ShardB', 1);
                """)

            # 3. Durable 2PC Transactions Table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS distributed_transactions (
                    tx_id VARCHAR(64) PRIMARY KEY,
                    request_id VARCHAR(128) UNIQUE,
                    from_account VARCHAR(16) NOT NULL,
                    to_account VARCHAR(16) NOT NULL,
                    from_shard VARCHAR(32) NOT NULL,
                    to_shard VARCHAR(32) NOT NULL,
                    amount BIGINT NOT NULL,
                    epoch INT NOT NULL DEFAULT 1,
                    status VARCHAR(32) NOT NULL,
                    from_acked BOOLEAN NOT NULL DEFAULT FALSE,
                    to_acked BOOLEAN NOT NULL DEFAULT FALSE,
                    error_reason TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            # 4. Bonus I: Crash-Consistent Live Migration State Machine Table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS migration_state (
                    migration_id VARCHAR(64) PRIMARY KEY,
                    source_shard VARCHAR(32) NOT NULL,
                    target_shard VARCHAR(32) NOT NULL,
                    start_acc INT NOT NULL,
                    end_acc INT NOT NULL,
                    phase VARCHAR(32) NOT NULL, -- SNAPSHOT, DELTA_REPLAY, FENCING, MAP_ACTIVATED, CLEANUP, COMPLETED
                    cursor_acc INT NOT NULL DEFAULT 0,
                    last_seq_id BIGINT NOT NULL DEFAULT 0,
                    target_epoch INT NOT NULL DEFAULT 2,
                    cutover_decided BOOLEAN NOT NULL DEFAULT FALSE,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            conn.commit()
            cur.close()
            conn.close()
            logger.info("Coordinator metadata tables initialized.")
            return
        except Exception as e:
            logger.warning(f"Meta DB init attempt {attempt + 1} failed: {e}")
            time.sleep(1)


# =====================================================================
# HELPER: CALL SHARD VIA GATEWAY REPLICA RESOLVER
# =====================================================================
def get_healthy_replica_url(shard_id: str) -> str:
    """Queries the Gateway for a healthy replica URL for `shard_id`."""
    resp = requests.get(f"{GATEWAY_URL}/internal/select_replica/{shard_id}", timeout=3)
    if resp.status_code != 200:
        raise RuntimeError(f"No healthy replica available for {shard_id}: {resp.text}")
    return resp.json()["replica_url"]


def call_shard_with_retry(shard_id: str, method: str, path: str, json_payload: dict = None, params: dict = None):
    """Calls a healthy replica of `shard_id`, retrying across replicas if a container just failed."""
    last_err = None
    for _ in range(3):
        try:
            url = get_healthy_replica_url(shard_id)
            if method == "GET":
                r = requests.get(f"{url}{path}", params=params, timeout=4)
            else:
                r = requests.post(f"{url}{path}", json=json_payload, params=params, timeout=4)
            return r
        except Exception as e:
            last_err = e
            time.sleep(0.3)
    raise RuntimeError(f"Failed calling {shard_id}{path} after retries: {last_err}")


# =====================================================================
# 2PC PROTOCOL EXECUTION & RECOVERY
# =====================================================================
class CrossShardTransferRequest(BaseModel):
    from_account: str
    to_account: str
    from_shard: str
    to_shard: str
    amount: int
    epoch: int = 1
    request_id: Optional[str] = None
    # Experiment 6 helper flags:
    pause_after_commit_decide_sec: float = 0.0
    simulate_crash_after_commit_decide: bool = False


def update_tx_status(tx_id: str, status: str, from_acked: Optional[bool] = None, to_acked: Optional[bool] = None, error_reason: Optional[str] = None):
    """Durably records a 2PC state transition in PostgreSQL before any network message is sent."""
    conn = get_meta_conn(autocommit=True)
    try:
        cur = conn.cursor()
        fields = ["status = %s", "updated_at = NOW()"]
        vals = [status]
        if from_acked is not None:
            fields.append("from_acked = %s")
            vals.append(from_acked)
        if to_acked is not None:
            fields.append("to_acked = %s")
            vals.append(to_acked)
        if error_reason is not None:
            fields.append("error_reason = %s")
            vals.append(error_reason)
        vals.append(tx_id)
        cur.execute(f"UPDATE distributed_transactions SET {', '.join(fields)} WHERE tx_id = %s;", tuple(vals))
    finally:
        conn.close()


def drive_commit_phase(tx: dict) -> bool:
    """
    Sends COMMIT to both participants for a transaction in COMMIT_DECIDED state.
    Returns True only when BOTH participants have confirmed COMMIT.
    Never replaces COMMIT_DECIDED with ABORT even if a participant is temporarily unreachable.
    """
    tx_id = tx["tx_id"]
    from_acked = tx["from_acked"]
    to_acked = tx["to_acked"]

    if not from_acked:
        try:
            r1 = call_shard_with_retry(
                tx["from_shard"], "POST", "/2pc/commit",
                {"tx_id": tx_id, "account_id": tx["from_account"]}
            )
            if r1.status_code == 200:
                from_acked = True
                update_tx_status(tx_id, "COMMIT_DECIDED", from_acked=True)
        except Exception as e:
            logger.warning(f"Commit ack pending from {tx['from_shard']} for {tx_id}: {e}")

    if not to_acked:
        try:
            r2 = call_shard_with_retry(
                tx["to_shard"], "POST", "/2pc/commit",
                {"tx_id": tx_id, "account_id": tx["to_account"]}
            )
            if r2.status_code == 200:
                to_acked = True
                update_tx_status(tx_id, "COMMIT_DECIDED", to_acked=True)
        except Exception as e:
            logger.warning(f"Commit ack pending from {tx['to_shard']} for {tx_id}: {e}")

    if from_acked and to_acked:
        update_tx_status(tx_id, "COMPLETED", from_acked=True, to_acked=True)
        logger.info(f"2PC Transaction {tx_id} COMPLETED successfully.")
        return True
    return False


def drive_abort_phase(tx: dict) -> bool:
    """Safely sends ABORT to both participants for a transaction in ABORT_DECIDED state."""
    tx_id = tx["tx_id"]
    from_acked = tx["from_acked"]
    to_acked = tx["to_acked"]

    if not from_acked:
        try:
            r1 = call_shard_with_retry(
                tx["from_shard"], "POST", "/2pc/abort",
                {"tx_id": tx_id, "account_id": tx["from_account"]}
            )
            if r1.status_code == 200:
                from_acked = True
                update_tx_status(tx_id, "ABORT_DECIDED", from_acked=True)
        except Exception as e:
            logger.warning(f"Abort ack pending from {tx['from_shard']} for {tx_id}: {e}")

    if not to_acked:
        try:
            r2 = call_shard_with_retry(
                tx["to_shard"], "POST", "/2pc/abort",
                {"tx_id": tx_id, "account_id": tx["to_account"]}
            )
            if r2.status_code == 200:
                to_acked = True
                update_tx_status(tx_id, "ABORT_DECIDED", to_acked=True)
        except Exception as e:
            logger.warning(f"Abort ack pending from {tx['to_shard']} for {tx_id}: {e}")

    if from_acked and to_acked:
        update_tx_status(tx_id, "COMPLETED", from_acked=True, to_acked=True)
        logger.info(f"2PC Transaction {tx_id} ABORTED and cleaned up across all participants.")
        return True
    return False


@app.post("/2pc/transfer")
def execute_cross_shard_transfer(req: CrossShardTransferRequest):
    """
    Executes a Two-Phase Commit (2PC) cross-shard transfer:
      1. Durably logs INITIATED -> PREPARING.
      2. Sends PREPARE (-amount) to source shard and PREPARE (+amount) to destination shard.
      3. If both vote YES: durably logs PREPARED -> COMMIT_DECIDED, then sends COMMIT.
      4. If either votes NO or fails: durably logs ABORT_DECIDED, then sends ABORT.
    """
    conn = get_meta_conn(autocommit=True)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        # Check client request_id idempotency
        if req.request_id:
            cur.execute("SELECT * FROM distributed_transactions WHERE request_id = %s;", (req.request_id,))
            existing = cur.fetchone()
            if existing:
                if existing["status"] == "COMPLETED" and not existing["error_reason"]:
                    return {"status": "success", "tx_id": existing["tx_id"], "state": "COMPLETED", "idempotent_replay": True}
                elif existing["status"] in ("INITIATED", "PREPARING", "PREPARED", "COMMIT_DECIDED"):
                    return {"status": "PENDING", "tx_id": existing["tx_id"], "state": existing["status"]}
                else:
                    raise HTTPException(status_code=400, detail=existing["error_reason"] or "Transaction aborted")

        tx_id = uuid.uuid4().hex[:16]
        cur.execute("""
            INSERT INTO distributed_transactions
            (tx_id, request_id, from_account, to_account, from_shard, to_shard, amount, epoch, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'INITIATED');
        """, (tx_id, req.request_id, req.from_account, req.to_account, req.from_shard, req.to_shard, req.amount, req.epoch))
    finally:
        conn.close()

    # Phase 1: PREPARE
    update_tx_status(tx_id, "PREPARING")
    logger.info(f"2PC [{tx_id}] Phase 1 PREPARING: {req.from_account} ({req.from_shard}) -> {req.to_account} ({req.to_shard}) amount={req.amount}")

    vote_from = {"vote": "NO", "reason": "Not reached"}
    vote_to = {"vote": "NO", "reason": "Not reached"}

    try:
        r_from = call_shard_with_retry(
            req.from_shard, "POST", "/2pc/prepare",
            {"tx_id": tx_id, "account_id": req.from_account, "delta": -req.amount, "epoch": req.epoch}
        )
        vote_from = r_from.json()
    except Exception as e:
        vote_from = {"vote": "NO", "reason": str(e)}

    if vote_from.get("vote") == "YES":
        try:
            r_to = call_shard_with_retry(
                req.to_shard, "POST", "/2pc/prepare",
                {"tx_id": tx_id, "account_id": req.to_account, "delta": req.amount, "epoch": req.epoch}
            )
            vote_to = r_to.json()
        except Exception as e:
            vote_to = {"vote": "NO", "reason": str(e)}

    # Evaluate Votes
    if vote_from.get("vote") == "YES" and vote_to.get("vote") == "YES":
        # Both participants durably prepared!
        update_tx_status(tx_id, "PREPARED")
        # Record durable COMMIT_DECIDED before sending any COMMIT messages
        update_tx_status(tx_id, "COMMIT_DECIDED")
        logger.info(f"2PC [{tx_id}] Both shards voted YES. Durable state -> COMMIT_DECIDED.")

        # Support Experiment 6: Interrupt Coordinator after durable COMMIT_DECIDED but before all acks!
        if req.simulate_crash_after_commit_decide:
            # Commit only the first participant so one ack is received and the other is still prepared!
            try:
                r1 = call_shard_with_retry(
                    req.from_shard, "POST", "/2pc/commit",
                    {"tx_id": tx_id, "account_id": req.from_account}
                )
                if r1.status_code == 200:
                    update_tx_status(tx_id, "COMMIT_DECIDED", from_acked=True)
            except Exception:
                pass
            logger.critical(f"EXPERIMENT 6 CRASH SIMULATION: Coordinator stopping after durable COMMIT_DECIDED for tx={tx_id}!")
            os._exit(1)

        if req.pause_after_commit_decide_sec > 0:
            logger.warning(f"Pausing {req.pause_after_commit_decide_sec}s after COMMIT_DECIDED for tx={tx_id}...")
            time.sleep(req.pause_after_commit_decide_sec)

        tx_record = {
            "tx_id": tx_id,
            "from_account": req.from_account,
            "to_account": req.to_account,
            "from_shard": req.from_shard,
            "to_shard": req.to_shard,
            "from_acked": False,
            "to_acked": False
        }
        all_committed = drive_commit_phase(tx_record)
        if not all_committed:
            # Do NOT report success before all participants confirm COMMIT!
            return {
                "status": "PENDING",
                "tx_id": tx_id,
                "state": "COMMIT_DECIDED",
                "message": "COMMIT decision recorded durably; waiting for remaining participant acks"
            }

        return {
            "status": "success",
            "type": "cross_shard_2pc",
            "tx_id": tx_id,
            "state": "COMPLETED",
            "from_account": req.from_account,
            "to_account": req.to_account,
            "amount": req.amount
        }
    else:
        # Scenario A: One or both rejected -> Safely abort all participants
        reason = vote_from.get("reason") if vote_from.get("vote") != "YES" else vote_to.get("reason")
        logger.warning(f"2PC [{tx_id}] Prepare rejected ({reason}). Durable state -> ABORT_DECIDED.")
        update_tx_status(tx_id, "ABORT_DECIDED", error_reason=reason)
        tx_record = {
            "tx_id": tx_id,
            "from_account": req.from_account,
            "to_account": req.to_account,
            "from_shard": req.from_shard,
            "to_shard": req.to_shard,
            "from_acked": False,
            "to_acked": False
        }
        drive_abort_phase(tx_record)
        raise HTTPException(status_code=400, detail=f"2PC Aborted: {reason}")


@app.get("/transactions/{tx_id}")
def get_transaction_status(tx_id: str):
    """
    Exposes a queryable transaction status by transaction ID.
    Unresolved transactions expose PENDING status along with their internal 2PC state.
    """
    conn = get_meta_conn(autocommit=True)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM distributed_transactions WHERE tx_id = %s;", (tx_id,))
        row = cur.fetchone()
        if not row:
            return {"tx_id": tx_id, "query_status": "UNKNOWN", "internal_state": "UNKNOWN"}

        unresolved_states = {"INITIATED", "PREPARING", "PREPARED", "COMMIT_DECIDED", "ABORT_DECIDED"}
        query_status = "PENDING" if row["status"] in unresolved_states else (
            "ABORTED" if row["error_reason"] else "COMPLETED"
        )
        return {
            "tx_id": row["tx_id"],
            "query_status": query_status,
            "internal_state": row["status"],
            "from_account": row["from_account"],
            "to_account": row["to_account"],
            "amount": row["amount"],
            "from_acked": row["from_acked"],
            "to_acked": row["to_acked"],
            "error_reason": row["error_reason"]
        }
    finally:
        conn.close()


# =====================================================================
# BACKGROUND RECOVERY WORKER (Scenarios B & C + Migration Recovery)
# =====================================================================
def recover_unresolved_transactions():
    """
    Rebuilds and resolves any unfinished 2PC transactions after a Coordinator restart:
    - Scenario C (COMMIT_DECIDED): Completes the recorded COMMIT; never replaces with ABORT.
    - Scenario B (PREPARED): All participants voted YES before crash; advances to COMMIT_DECIDED and commits.
    - Scenario A/B (INITIATED / PREPARING / ABORT_DECIDED): Safely aborts all participants.
    """
    conn = get_meta_conn(autocommit=True)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT * FROM distributed_transactions
            WHERE status NOT IN ('COMPLETED')
              AND EXTRACT(EPOCH FROM (NOW() - updated_at)) > 1.5;
        """)
        unresolved = cur.fetchall()
        for tx in unresolved:
            st = tx["status"]
            tx_id = tx["tx_id"]
            logger.info(f"RECOVERY: Found unresolved transaction {tx_id} in state {st}")
            if st == "COMMIT_DECIDED":
                drive_commit_phase(tx)
            elif st == "PREPARED":
                update_tx_status(tx_id, "COMMIT_DECIDED")
                drive_commit_phase(tx)
            elif st in ("INITIATED", "PREPARING", "ABORT_DECIDED"):
                if st != "ABORT_DECIDED":
                    update_tx_status(tx_id, "ABORT_DECIDED", error_reason="Coordinator recovery abort before commit decision")
                drive_abort_phase(tx)
    except Exception as e:
        logger.warning(f"Recovery scan error: {e}")
    finally:
        conn.close()


# =====================================================================
# BONUS I: CRASH-CONSISTENT LIVE RE-SHARDING STATE MACHINE
# =====================================================================
class StartMigrationRequest(BaseModel):
    migration_id: str = "mig_B_to_C_1501_2000"
    source_shard: str = "ShardB"
    target_shard: str = "ShardC"
    start_acc: int = 1501
    end_acc: int = 2000
    # Optional fault-injection parameter: "SNAPSHOT", "DELTA_REPLAY", "FENCING", "AFTER_ACTIVATION"
    simulate_crash_at_phase: Optional[str] = None


def persist_migration_checkpoint(migration_id: str, **kwargs):
    conn = get_meta_conn(autocommit=True)
    try:
        cur = conn.cursor()
        sets = ["updated_at = NOW()"]
        vals = []
        for k, v in kwargs.items():
            sets.append(f"{k} = %s")
            vals.append(v)
        vals.append(migration_id)
        cur.execute(f"UPDATE migration_state SET {', '.join(sets)} WHERE migration_id = %s;", tuple(vals))
    finally:
        conn.close()


def execute_migration_state_machine(migration_id: str, simulate_crash_at_phase: Optional[str] = None):
    """
    Executes or resumes the live migration state machine across 5 crash-consistent phases:
    1. SNAPSHOT: Online batched copy of A1501-A2000 from ShardB to ShardC with durable cursor_acc checkpoints.
    2. DELTA_REPLAY: Replays ordered change_log writes made during snapshot copy.
    3. FENCING: Briefly pauses A1501-A2000 on ShardB, waits for in-flight 2PC to drain, replays final delta,
       and fences writes on ShardB with new epoch E+1.
    4. MAP_ACTIVATED: Atomically updates Shard Map in db_meta to route A1501-A2000 -> ShardC (epoch E+1).
    5. CLEANUP: Removes migrated rows from ShardB after verified activation and marks COMPLETED.
    """
    conn = get_meta_conn(autocommit=True)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM migration_state WHERE migration_id = %s;", (migration_id,))
    mig = cur.fetchone()
    conn.close()
    if not mig or mig["phase"] == "COMPLETED":
        return mig

    src = mig["source_shard"]
    dst = mig["target_shard"]
    start_acc = mig["start_acc"]
    end_acc = mig["end_acc"]
    target_epoch = mig["target_epoch"]

    # Ensure target shard allows writes for [start_acc, end_acc] at target_epoch
    call_shard_with_retry(dst, "POST", "/migration/fencing", {
        "start_acc": start_acc,
        "end_acc": end_acc,
        "min_epoch": 1,
        "is_fenced": False,
        "paused_for_cutover": False
    })

    # PHASE 1: ONLINE SNAPSHOT COPY
    if mig["phase"] == "SNAPSHOT":
        cursor_acc = mig["cursor_acc"]
        last_seq_id = mig["last_seq_id"]
        logger.info(f"MIGRATION [{migration_id}] Phase 1 SNAPSHOT starting from cursor={cursor_acc}")

        while cursor_acc < end_acc:
            snap_resp = call_shard_with_retry(src, "GET", "/migration/snapshot", params={
                "start_acc": start_acc,
                "end_acc": end_acc,
                "cursor_acc": cursor_acc,
                "batch_size": 250
            }).json()

            if last_seq_id == 0:
                last_seq_id = snap_resp["max_seq_id"]

            batch = snap_resp["accounts"]
            if not batch:
                break

            call_shard_with_retry(dst, "POST", "/migration/apply_snapshot", {"items": batch})
            cursor_acc = int(batch[-1]["account_id"][1:])
            persist_migration_checkpoint(migration_id, cursor_acc=cursor_acc, last_seq_id=last_seq_id)

            if simulate_crash_at_phase == "SNAPSHOT":
                logger.critical("BONUS I CRASH INJECTION: Crashing Coordinator mid-SNAPSHOT!")
                os._exit(1)

        persist_migration_checkpoint(migration_id, phase="DELTA_REPLAY")
        mig["phase"] = "DELTA_REPLAY"
        mig["last_seq_id"] = last_seq_id

    # PHASE 2: ONLINE DELTA CATCH-UP REPLAY
    if mig["phase"] == "DELTA_REPLAY":
        last_seq_id = mig["last_seq_id"]
        logger.info(f"MIGRATION [{migration_id}] Phase 2 DELTA_REPLAY catching up from seq_id={last_seq_id}")
        while True:
            chg_resp = call_shard_with_retry(src, "GET", "/migration/changes", params={
                "start_acc": start_acc,
                "end_acc": end_acc,
                "since_seq_id": last_seq_id,
                "limit": 500
            }).json()
            changes = chg_resp["changes"]
            if not changes:
                break
            call_shard_with_retry(dst, "POST", "/migration/apply_changes", {"items": changes})
            last_seq_id = changes[-1]["seq_id"]
            persist_migration_checkpoint(migration_id, last_seq_id=last_seq_id)

            if simulate_crash_at_phase == "DELTA_REPLAY":
                logger.critical("BONUS I CRASH INJECTION: Crashing Coordinator during DELTA_REPLAY!")
                os._exit(1)

        persist_migration_checkpoint(migration_id, phase="FENCING")
        mig["phase"] = "FENCING"
        mig["last_seq_id"] = last_seq_id

    # PHASE 3: BRIEF CUTOVER PAUSE, IN-FLIGHT 2PC DRAIN, FINAL DELTA & OLD SHARD FENCING
    if mig["phase"] == "FENCING":
        logger.info(f"MIGRATION [{migration_id}] Phase 3 FENCING & CUTOVER")
        # 3a. Briefly pause writes on migrating range on source shard
        call_shard_with_retry(src, "POST", "/migration/fencing", {
            "start_acc": start_acc,
            "end_acc": end_acc,
            "min_epoch": 1,
            "is_fenced": False,
            "paused_for_cutover": True
        })

        # 3b. Wait for any in-flight prepared 2PC transactions on [start_acc, end_acc] to finish
        for _ in range(20):
            recover_unresolved_transactions()
            inflight = call_shard_with_retry(src, "GET", "/2pc/in_flight", params={
                "start_acc": start_acc, "end_acc": end_acc
            }).json()
            if inflight["in_flight_count"] == 0:
                break
            time.sleep(0.2)

        # 3c. Replay any final delta changes written right before pause
        last_seq_id = mig["last_seq_id"]
        chg_resp = call_shard_with_retry(src, "GET", "/migration/changes", params={
            "start_acc": start_acc,
            "end_acc": end_acc,
            "since_seq_id": last_seq_id,
            "limit": 1000
        }).json()
        changes = chg_resp["changes"]
        if changes:
            call_shard_with_retry(dst, "POST", "/migration/apply_changes", {"items": changes})
            last_seq_id = changes[-1]["seq_id"]

        # 3d. Permanently fence old owner (ShardB) for A1501-A2000 and enforce target_epoch on ShardC
        call_shard_with_retry(src, "POST", "/migration/fencing", {
            "start_acc": start_acc,
            "end_acc": end_acc,
            "min_epoch": target_epoch,
            "is_fenced": True,
            "paused_for_cutover": False
        })
        call_shard_with_retry(dst, "POST", "/migration/fencing", {
            "start_acc": start_acc,
            "end_acc": end_acc,
            "min_epoch": target_epoch,
            "is_fenced": False,
            "paused_for_cutover": False
        })

        if simulate_crash_at_phase == "FENCING":
            logger.critical("BONUS I CRASH INJECTION: Crashing Coordinator during FENCING!")
            os._exit(1)

        persist_migration_checkpoint(
            migration_id,
            phase="MAP_ACTIVATED",
            last_seq_id=last_seq_id,
            cutover_decided=True
        )
        mig["phase"] = "MAP_ACTIVATED"

    # PHASE 4: ATOMIC SHARD MAP OWNERSHIP ACTIVATION
    if mig["phase"] == "MAP_ACTIVATED":
        logger.info(f"MIGRATION [{migration_id}] Phase 4 MAP_ACTIVATED (Epoch -> {target_epoch})")
        conn = get_meta_conn(autocommit=False)
        try:
            cur = conn.cursor()
            cur.execute("UPDATE shard_map_meta SET current_epoch = %s WHERE id = 1;", (target_epoch,))
            # Split ShardB (1001..2000) into ShardB (1001..1500) and ShardC (1501..2000)
            cur.execute("DELETE FROM shard_map WHERE start_acc >= 1001 AND end_acc <= 2000;")
            cur.execute("""
                INSERT INTO shard_map (start_acc, end_acc, shard_id, epoch) VALUES
                (1001, %s, %s, %s),
                (%s, %s, %s, %s);
            """, (start_acc - 1, src, target_epoch, start_acc, end_acc, dst, target_epoch))
            conn.commit()
        finally:
            conn.close()

        # Notify Gateway to refresh its cached Shard Map immediately
        try:
            requests.post(f"{GATEWAY_URL}/admin/refresh_map", timeout=2)
        except Exception:
            pass

        if simulate_crash_at_phase == "AFTER_ACTIVATION":
            logger.critical("BONUS I CRASH INJECTION: Crashing Coordinator after MAP_ACTIVATED before CLEANUP!")
            os._exit(1)

        persist_migration_checkpoint(migration_id, phase="CLEANUP")
        mig["phase"] = "CLEANUP"

    # PHASE 5: VERIFIED SOURCE CLEANUP
    if mig["phase"] == "CLEANUP":
        logger.info(f"MIGRATION [{migration_id}] Phase 5 CLEANUP on {src}")
        call_shard_with_retry(src, "POST", "/migration/cleanup", params={
            "start_acc": start_acc, "end_acc": end_acc
        })
        persist_migration_checkpoint(migration_id, phase="COMPLETED")
        mig["phase"] = "COMPLETED"
        logger.info(f"MIGRATION [{migration_id}] COMPLETED! A{start_acc:04d}-A{end_acc:04d} now live on {dst} (Epoch {target_epoch}).")

    conn = get_meta_conn(autocommit=True)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM migration_state WHERE migration_id = %s;", (migration_id,))
    final_state = cur.fetchone()
    conn.close()
    return final_state


@app.post("/migration/start")
def start_live_migration(req: StartMigrationRequest):
    """Starts or resumes the Bonus I Live Re-Sharding migration from ShardB to ShardC."""
    conn = get_meta_conn(autocommit=True)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT current_epoch FROM shard_map_meta WHERE id = 1;")
        cur_epoch = cur.fetchone()["current_epoch"]
        target_epoch = max(cur_epoch + 1, 2)

        cur.execute("SELECT * FROM migration_state WHERE migration_id = %s;", (req.migration_id,))
        existing = cur.fetchone()
        if not existing:
            cur.execute("""
                INSERT INTO migration_state
                (migration_id, source_shard, target_shard, start_acc, end_acc, phase, cursor_acc, last_seq_id, target_epoch)
                VALUES (%s, %s, %s, %s, %s, 'SNAPSHOT', 0, 0, %s);
            """, (req.migration_id, req.source_shard, req.target_shard, req.start_acc, req.end_acc, target_epoch))
    finally:
        conn.close()

    state = execute_migration_state_machine(req.migration_id, req.simulate_crash_at_phase)
    return {"status": "success", "migration_state": state}


@app.get("/migration/status/{migration_id}")
def get_migration_status(migration_id: str):
    conn = get_meta_conn(autocommit=True)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM migration_state WHERE migration_id = %s;", (migration_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Migration not found")
        return row
    finally:
        conn.close()


def background_recovery_loop():
    """Continuously runs 2PC crash recovery and resumes any interrupted live migration."""
    while True:
        try:
            recover_unresolved_transactions()
            # Also check if any migration was interrupted mid-flight and resume it automatically
            conn = get_meta_conn(autocommit=True)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT migration_id FROM migration_state
                WHERE phase != 'COMPLETED'
                  AND EXTRACT(EPOCH FROM (NOW() - updated_at)) > 2.0;
            """)
            migs = cur.fetchall()
            conn.close()
            for m in migs:
                logger.info(f"RECOVERY: Resuming interrupted migration {m['migration_id']}...")
                execute_migration_state_machine(m["migration_id"])
        except Exception as e:
            logger.warning(f"Background loop warning: {e}")
        time.sleep(1.5)


@app.on_event("startup")
def on_startup():
    init_meta_tables()
    t = threading.Thread(target=background_recovery_loop, daemon=True)
    t.start()


@app.get("/health")
def health():
    return {"status": "healthy", "service": "coordinator"}