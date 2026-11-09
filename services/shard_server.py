import os
import time
import logging
from typing import Optional, List
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

# Configure logging to clearly show which replica handled each request
SHARD_ID = os.getenv("SHARD_ID", "ShardA")
REPLICA_ID = os.getenv("REPLICA_ID", "bank_a1")
DB_DSN = os.getenv("DB_DSN", "postgresql://bankuser:bankpassword@localhost:5432/shard_a_db")
INITIAL_START_ACC = int(os.getenv("INITIAL_START_ACC", "1"))
INITIAL_END_ACC = int(os.getenv("INITIAL_END_ACC", "1000"))
INITIAL_BALANCE = int(os.getenv("INITIAL_BALANCE", "1000"))

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [{SHARD_ID}:{REPLICA_ID}] %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(title=f"Banking Replica {REPLICA_ID} ({SHARD_ID})")


def get_db_conn(autocommit: bool = False):
    """Creates a new connection to this shard's persistent PostgreSQL database."""
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = autocommit
    return conn


def parse_account_num(account_id: str) -> int:
    """Extracts the integer index from account IDs like 'A0500' -> 500."""
    if not account_id or not account_id.startswith("A") or not account_id[1:].isdigit():
        raise HTTPException(status_code=400, detail=f"Invalid account format: {account_id}")
    return int(account_id[1:])


# =====================================================================
# DATABASE INITIALIZATION & SEEDING
# =====================================================================
@app.on_event("startup")
def startup_init_db():
    """
    Initializes the shard database schema and seeds initial accounts.
    Uses a PostgreSQL advisory lock so that if multiple replicas (e.g., bank_a1 and bank_a2)
    start simultaneously, they do not race during DDL creation and seeding.
    """
    for attempt in range(15):
        try:
            conn = get_db_conn(autocommit=False)
            cur = conn.cursor()
            # Acquire transaction-level advisory lock to serialize startup across replicas
            cur.execute("SELECT pg_advisory_xact_lock(999111);")

            # 1. Accounts table (Authoritative balance storage for this shard)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    account_id VARCHAR(16) PRIMARY KEY,
                    balance BIGINT NOT NULL CHECK (balance >= 0),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            # 2. 2PC Participant Transaction State table (Ensures idempotent COMMIT/ABORT)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS participant_tx_state (
                    gid VARCHAR(128) PRIMARY KEY,
                    tx_id VARCHAR(64) NOT NULL,
                    account_id VARCHAR(16) NOT NULL,
                    delta BIGINT NOT NULL,
                    status VARCHAR(32) NOT NULL, -- PREPARED, COMMITTED, ABORTED
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            # 3. Client Request Idempotency table (Exactly-once semantics for retries)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS idempotency_log (
                    request_id VARCHAR(128) PRIMARY KEY,
                    response_balance BIGINT,
                    status VARCHAR(32) NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            # 4. Change Capture Log for Bonus I (Live Re-Sharding Online Copy + Replay)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS change_log (
                    seq_id BIGSERIAL PRIMARY KEY,
                    op_id VARCHAR(128) UNIQUE NOT NULL,
                    account_id VARCHAR(16) NOT NULL,
                    new_balance BIGINT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            # 5. Applied Migration Ops on Target Shard (Idempotent delta replay)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS applied_migration_ops (
                    op_id VARCHAR(128) PRIMARY KEY,
                    applied_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            # 6. Shard Fencing & Epoch Config (Bonus I: Enforces epoch & range fencing at write path)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS shard_fencing_rules (
                    rule_id SERIAL PRIMARY KEY,
                    start_acc INT NOT NULL,
                    end_acc INT NOT NULL,
                    min_epoch INT NOT NULL DEFAULT 1,
                    is_fenced BOOLEAN NOT NULL DEFAULT FALSE,
                    paused_for_cutover BOOLEAN NOT NULL DEFAULT FALSE
                );
            """)

            # Seed default ownership rule if empty
            cur.execute("SELECT COUNT(*) FROM shard_fencing_rules;")
            if cur.fetchone()[0] == 0 and INITIAL_END_ACC > 0:
                cur.execute("""
                    INSERT INTO shard_fencing_rules (start_acc, end_acc, min_epoch, is_fenced, paused_for_cutover)
                    VALUES (%s, %s, 1, FALSE, FALSE);
                """, (INITIAL_START_ACC, INITIAL_END_ACC))

            # Seed accounts assigned to this shard only
            if INITIAL_END_ACC > 0 and INITIAL_START_ACC > 0:
                cur.execute("SELECT COUNT(*) FROM accounts;")
                existing_count = cur.fetchone()[0]
                if existing_count == 0:
                    logger.info(f"Seeding accounts A{INITIAL_START_ACC:04d} to A{INITIAL_END_ACC:04d}...")
                    args = [
                        (f"A{i:04d}", INITIAL_BALANCE)
                        for i in range(INITIAL_START_ACC, INITIAL_END_ACC + 1)
                    ]
                    cur.executemany(
                        "INSERT INTO accounts (account_id, balance) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                        args
                    )
            conn.commit()
            cur.close()
            conn.close()
            logger.info("Database initialization complete.")
            return
        except Exception as e:
            logger.warning(f"Startup DB init attempt {attempt + 1} failed: {e}")
            time.sleep(1)


# =====================================================================
# HELPER: WRITE-PATH EPOCH & FENCING VALIDATION
# =====================================================================
def verify_write_fencing(cur, account_id: str, request_epoch: int):
    """
    Enforces shard ownership and monotonically increasing epochs directly at the shard write path.
    Even if a stale Gateway or attacker sends a write directly to an old shard after cutover,
    the shard rejects it with HTTP 409.
    """
    acc_num = parse_account_num(account_id)
    cur.execute("""
        SELECT min_epoch, is_fenced, paused_for_cutover
        FROM shard_fencing_rules
        WHERE %s BETWEEN start_acc AND end_acc
        ORDER BY rule_id DESC LIMIT 1;
    """, (acc_num,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(
            status_code=409,
            detail=f"FENCED_OR_NOT_OWNED: Account {account_id} is not owned by {SHARD_ID}"
        )
    min_epoch, is_fenced, paused_for_cutover = row
    if is_fenced:
        raise HTTPException(
            status_code=409,
            detail=f"SHARD_FENCED: Range for {account_id} on {SHARD_ID} has been permanently fenced out (min_epoch={min_epoch})"
        )
    if paused_for_cutover:
        raise HTTPException(
            status_code=503,
            detail=f"CUTOVER_PAUSE: Account {account_id} is briefly paused for atomic ownership cutover"
        )
    if request_epoch < min_epoch:
        raise HTTPException(
            status_code=409,
            detail=f"STALE_EPOCH: Request epoch {request_epoch} < Shard required min_epoch {min_epoch}"
        )


def record_change_log(cur, op_id: str, account_id: str, new_balance: int):
    """Records an ordered state change inside the same local transaction for live migration."""
    cur.execute("""
        INSERT INTO change_log (op_id, account_id, new_balance)
        VALUES (%s, %s, %s)
        ON CONFLICT (op_id) DO NOTHING;
    """, (op_id, account_id, new_balance))


# =====================================================================
# PYDANTIC MODELS
# =====================================================================
class AmountRequest(BaseModel):
    account_id: str
    amount: int
    epoch: int = 1
    request_id: Optional[str] = None


class LocalTransferRequest(BaseModel):
    from_account: str
    to_account: str
    amount: int
    epoch: int = 1
    request_id: Optional[str] = None


class PrepareRequest(BaseModel):
    tx_id: str
    account_id: str
    delta: int  # Negative for debit, positive for credit
    epoch: int = 1


class DecisionRequest(BaseModel):
    tx_id: str
    account_id: str


class FencingUpdateRequest(BaseModel):
    start_acc: int
    end_acc: int
    min_epoch: int
    is_fenced: bool
    paused_for_cutover: bool = False


class ReplayBatchRequest(BaseModel):
    items: List[dict]


# =====================================================================
# 1. HEALTH CHECK & METADATA
# =====================================================================
@app.get("/health")
def health_check():
    """Used by the Gateway and Self-Healer to verify replica & DB health."""
    try:
        conn = get_db_conn(autocommit=True)
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.close()
        conn.close()
        return {"status": "healthy", "shard_id": SHARD_ID, "replica_id": REPLICA_ID}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================================
# 2. LOCAL BANKING OPERATIONS (Balance, Deposit, Withdraw)
# =====================================================================
@app.get("/balance/{account_id}")
def get_balance(account_id: str, epoch: int = 1):
    """Returns the authoritative balance for an account from this shard's database."""
    conn = get_db_conn(autocommit=False)
    try:
        cur = conn.cursor()
        verify_write_fencing(cur, account_id, epoch)
        cur.execute("SELECT balance FROM accounts WHERE account_id = %s;", (account_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Account {account_id} not found on {SHARD_ID}")
        balance = row[0]
        conn.commit()
        logger.info(f"BALANCE {account_id} -> {balance}")
        return {
            "account_id": account_id,
            "balance": balance,
            "shard_id": SHARD_ID,
            "replica_id": REPLICA_ID
        }
    finally:
        conn.close()


@app.post("/deposit")
def deposit(req: AmountRequest):
    """
    Atomically deposits money into an account using row-level locking (SELECT ... FOR UPDATE).
    Prevents lost updates under high concurrency across multiple replicas.
    """
    if req.amount <= 0:
        raise HTTPException(status_code=400, detail="Deposit amount must be positive")

    conn = get_db_conn(autocommit=False)
    try:
        cur = conn.cursor()
        verify_write_fencing(cur, req.account_id, req.epoch)

        # Check client request_id idempotency if provided
        if req.request_id:
            cur.execute("SELECT response_balance FROM idempotency_log WHERE request_id = %s;", (req.request_id,))
            idem = cur.fetchone()
            if idem:
                conn.commit()
                return {
                    "status": "success",
                    "account_id": req.account_id,
                    "balance": idem[0],
                    "shard_id": SHARD_ID,
                    "replica_id": REPLICA_ID,
                    "idempotent_replay": True
                }

        # Lock the account row exclusively for the duration of this transaction
        cur.execute("SELECT balance FROM accounts WHERE account_id = %s FOR UPDATE;", (req.account_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Account {req.account_id} not found on {SHARD_ID}")

        new_balance = row[0] + req.amount
        cur.execute(
            "UPDATE accounts SET balance = %s, updated_at = NOW() WHERE account_id = %s;",
            (new_balance, req.account_id)
        )

        op_id = req.request_id or f"dep_{req.account_id}_{time.time_ns()}"
        record_change_log(cur, op_id, req.account_id, new_balance)

        if req.request_id:
            cur.execute(
                "INSERT INTO idempotency_log (request_id, response_balance, status) VALUES (%s, %s, 'COMPLETED');",
                (req.request_id, new_balance)
            )

        conn.commit()
        logger.info(f"DEPOSIT {req.account_id} +{req.amount} -> {new_balance}")
        return {
            "status": "success",
            "account_id": req.account_id,
            "balance": new_balance,
            "shard_id": SHARD_ID,
            "replica_id": REPLICA_ID
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@app.post("/withdraw")
def withdraw(req: AmountRequest):
    """
    Atomically withdraws money from an account using SELECT ... FOR UPDATE.
    Ensures balance never drops below zero even when multiple replicas withdraw concurrently.
    """
    if req.amount <= 0:
        raise HTTPException(status_code=400, detail="Withdraw amount must be positive")

    conn = get_db_conn(autocommit=False)
    try:
        cur = conn.cursor()
        verify_write_fencing(cur, req.account_id, req.epoch)

        if req.request_id:
            cur.execute("SELECT response_balance FROM idempotency_log WHERE request_id = %s;", (req.request_id,))
            idem = cur.fetchone()
            if idem:
                conn.commit()
                return {
                    "status": "success",
                    "account_id": req.account_id,
                    "balance": idem[0],
                    "shard_id": SHARD_ID,
                    "replica_id": REPLICA_ID,
                    "idempotent_replay": True
                }

        # Acquire row-level lock before checking balance
        cur.execute("SELECT balance FROM accounts WHERE account_id = %s FOR UPDATE;", (req.account_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Account {req.account_id} not found on {SHARD_ID}")

        if row[0] < req.amount:
            raise HTTPException(status_code=400, detail=f"Insufficient funds in {req.account_id}")

        new_balance = row[0] - req.amount
        cur.execute(
            "UPDATE accounts SET balance = %s, updated_at = NOW() WHERE account_id = %s;",
            (new_balance, req.account_id)
        )

        op_id = req.request_id or f"wth_{req.account_id}_{time.time_ns()}"
        record_change_log(cur, op_id, req.account_id, new_balance)

        if req.request_id:
            cur.execute(
                "INSERT INTO idempotency_log (request_id, response_balance, status) VALUES (%s, %s, 'COMPLETED');",
                (req.request_id, new_balance)
            )

        conn.commit()
        logger.info(f"WITHDRAW {req.account_id} -{req.amount} -> {new_balance}")
        return {
            "status": "success",
            "account_id": req.account_id,
            "balance": new_balance,
            "shard_id": SHARD_ID,
            "replica_id": REPLICA_ID
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# =====================================================================
# 3. SAME-SHARD ATOMIC TRANSFER
# =====================================================================
@app.post("/local_transfer")
def local_transfer(req: LocalTransferRequest):
    """
    Executes a same-shard transfer as a single atomic database transaction.
    Locks accounts in deterministic sorted order (first_acc, second_acc) to prevent deadlocks.
    """
    if req.amount <= 0:
        raise HTTPException(status_code=400, detail="Transfer amount must be positive")
    if req.from_account == req.to_account:
        raise HTTPException(status_code=400, detail="Source and destination accounts must differ")

    conn = get_db_conn(autocommit=False)
    try:
        cur = conn.cursor()
        verify_write_fencing(cur, req.from_account, req.epoch)
        verify_write_fencing(cur, req.to_account, req.epoch)

        if req.request_id:
            cur.execute("SELECT response_balance FROM idempotency_log WHERE request_id = %s;", (req.request_id,))
            idem = cur.fetchone()
            if idem:
                conn.commit()
                return {
                    "status": "success",
                    "type": "same_shard",
                    "from_account": req.from_account,
                    "to_account": req.to_account,
                    "amount": req.amount,
                    "shard_id": SHARD_ID,
                    "replica_id": REPLICA_ID,
                    "idempotent_replay": True
                }

        # Deterministic lock ordering prevents deadlocks when A1->A2 and A2->A1 run concurrently
        first_acc, second_acc = sorted([req.from_account, req.to_account])
        cur.execute("SELECT account_id, balance FROM accounts WHERE account_id = %s FOR UPDATE;", (first_acc,))
        r1 = cur.fetchone()
        cur.execute("SELECT account_id, balance FROM accounts WHERE account_id = %s FOR UPDATE;", (second_acc,))
        r2 = cur.fetchone()

        if not r1 or not r2:
            raise HTTPException(status_code=404, detail="One or both accounts not found on this shard")

        balances = {r1[0]: r1[1], r2[0]: r2[1]}
        if balances[req.from_account] < req.amount:
            raise HTTPException(status_code=400, detail=f"Insufficient funds in {req.from_account}")

        new_from_bal = balances[req.from_account] - req.amount
        new_to_bal = balances[req.to_account] + req.amount

        cur.execute("UPDATE accounts SET balance = %s, updated_at = NOW() WHERE account_id = %s;", (new_from_bal, req.from_account))
        cur.execute("UPDATE accounts SET balance = %s, updated_at = NOW() WHERE account_id = %s;", (new_to_bal, req.to_account))

        base_op = req.request_id or f"ltx_{time.time_ns()}"
        record_change_log(cur, f"{base_op}_from", req.from_account, new_from_bal)
        record_change_log(cur, f"{base_op}_to", req.to_account, new_to_bal)

        if req.request_id:
            cur.execute(
                "INSERT INTO idempotency_log (request_id, response_balance, status) VALUES (%s, %s, 'COMPLETED');",
                (req.request_id, new_from_bal)
            )

        conn.commit()
        logger.info(f"LOCAL_TRANSFER {req.from_account} -> {req.to_account} ({req.amount})")
        return {
            "status": "success",
            "type": "same_shard",
            "from_account": req.from_account,
            "to_account": req.to_account,
            "from_balance": new_from_bal,
            "to_balance": new_to_bal,
            "shard_id": SHARD_ID,
            "replica_id": REPLICA_ID
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# =====================================================================
# 4. TWO-PHASE COMMIT (2PC) PARTICIPANT ENDPOINTS
# =====================================================================
def make_gid(tx_id: str, account_id: str) -> str:
    """Builds a deterministic PostgreSQL Global Transaction Identifier (GID)."""
    return f"tx_{tx_id}_{account_id}"


@app.post("/2pc/prepare")
def prepare_2pc(req: PrepareRequest):
    """
    Phase 1 of 2PC:
    - Validates fencing/epoch and account existence.
    - Checks sufficient funds (if delta < 0).
    - Applies the provisional update and executes PostgreSQL `PREPARE TRANSACTION '<gid>'`.
    - Returns vote YES or NO.
    """
    gid = make_gid(req.tx_id, req.account_id)

    # First check if already prepared or committed/aborted (idempotency)
    check_conn = get_db_conn(autocommit=True)
    try:
        cur = check_conn.cursor()
        cur.execute("SELECT gid FROM pg_prepared_xacts WHERE gid = %s;", (gid,))
        if cur.fetchone():
            return {"vote": "YES", "gid": gid, "shard_id": SHARD_ID, "replica_id": REPLICA_ID, "note": "already_prepared"}
        cur.execute("SELECT status FROM participant_tx_state WHERE gid = %s;", (gid,))
        existing = cur.fetchone()
        if existing:
            if existing[0] == "COMMITTED":
                return {"vote": "YES", "gid": gid, "shard_id": SHARD_ID, "replica_id": REPLICA_ID}
            if existing[0] == "ABORTED":
                return {"vote": "NO", "reason": "Transaction already aborted", "shard_id": SHARD_ID}
    finally:
        check_conn.close()

    # Begin transactional prepare
    conn = get_db_conn(autocommit=False)
    try:
        cur = conn.cursor()
        verify_write_fencing(cur, req.account_id, req.epoch)

        # Lock the target account row
        cur.execute("SELECT balance FROM accounts WHERE account_id = %s FOR UPDATE NOWAIT;", (req.account_id,))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return {"vote": "NO", "reason": f"Account {req.account_id} not found on {SHARD_ID}", "shard_id": SHARD_ID}

        current_balance = row[0]
        new_balance = current_balance + req.delta
        if new_balance < 0:
            conn.rollback()
            logger.warning(f"2PC PREPARE NO (Insufficient funds) tx={req.tx_id} acc={req.account_id}")
            return {"vote": "NO", "reason": f"Insufficient funds in {req.account_id}", "shard_id": SHARD_ID}

        # Stage the balance update, participant_tx_state record, and change_log inside the prepared tx
        cur.execute(
            "UPDATE accounts SET balance = %s, updated_at = NOW() WHERE account_id = %s;",
            (new_balance, req.account_id)
        )
        cur.execute("""
            INSERT INTO participant_tx_state (gid, tx_id, account_id, delta, status)
            VALUES (%s, %s, %s, %s, 'COMMITTED')
            ON CONFLICT (gid) DO UPDATE SET status = 'COMMITTED';
        """, (gid, req.tx_id, req.account_id, req.delta))
        record_change_log(cur, gid, req.account_id, new_balance)

        # Persist the prepared transaction to PostgreSQL WAL disk storage
        cur.execute(f"PREPARE TRANSACTION '{gid}';")
        conn.commit()
        logger.info(f"2PC PREPARE YES tx={req.tx_id} acc={req.account_id} delta={req.delta}")
        return {"vote": "YES", "gid": gid, "shard_id": SHARD_ID, "replica_id": REPLICA_ID}
    except HTTPException as he:
        conn.rollback()
        return {"vote": "NO", "reason": he.detail, "shard_id": SHARD_ID}
    except Exception as e:
        conn.rollback()
        logger.warning(f"2PC PREPARE NO (Lock/DB error) tx={req.tx_id} acc={req.account_id}: {e}")
        return {"vote": "NO", "reason": str(e), "shard_id": SHARD_ID}
    finally:
        conn.close()


@app.post("/2pc/commit")
def commit_2pc(req: DecisionRequest):
    """
    Phase 2 of 2PC (COMMIT):
    - Checks if `gid` is in `pg_prepared_xacts`. If so, runs `COMMIT PREPARED '<gid>'`.
    - Handles repeated COMMIT calls idempotently by checking `participant_tx_state`.
    """
    gid = make_gid(req.tx_id, req.account_id)
    conn = get_db_conn(autocommit=True)
    try:
        cur = conn.cursor()
        cur.execute("SELECT gid FROM pg_prepared_xacts WHERE gid = %s;", (gid,))
        if cur.fetchone():
            cur.execute(f"COMMIT PREPARED '{gid}';")
            logger.info(f"2PC COMMITTED {gid}")
            return {"status": "COMMITTED", "gid": gid, "shard_id": SHARD_ID, "replica_id": REPLICA_ID}

        # Not in pg_prepared_xacts: check if already committed previously (idempotent retry)
        cur.execute("SELECT status FROM participant_tx_state WHERE gid = %s;", (gid,))
        row = cur.fetchone()
        if row and row[0] == "COMMITTED":
            logger.info(f"2PC COMMIT (Idempotent Repeat) {gid}")
            return {"status": "COMMITTED", "gid": gid, "shard_id": SHARD_ID, "replica_id": REPLICA_ID, "idempotent": True}

        raise HTTPException(status_code=404, detail=f"Prepared transaction {gid} not found on {SHARD_ID}")
    finally:
        conn.close()


@app.post("/2pc/abort")
def abort_2pc(req: DecisionRequest):
    """
    Phase 2 of 2PC (ABORT):
    - Checks if `gid` is in `pg_prepared_xacts`. If so, runs `ROLLBACK PREPARED '<gid>'`.
    - Safely records ABORTED state so repeated ABORT messages succeed idempotently.
    """
    gid = make_gid(req.tx_id, req.account_id)
    conn = get_db_conn(autocommit=True)
    try:
        cur = conn.cursor()
        cur.execute("SELECT gid FROM pg_prepared_xacts WHERE gid = %s;", (gid,))
        if cur.fetchone():
            cur.execute(f"ROLLBACK PREPARED '{gid}';")
            logger.info(f"2PC ROLLED BACK {gid}")

        cur.execute("""
            INSERT INTO participant_tx_state (gid, tx_id, account_id, delta, status)
            VALUES (%s, %s, %s, 0, 'ABORTED')
            ON CONFLICT (gid) DO UPDATE SET status = 'ABORTED', updated_at = NOW();
        """, (gid, req.tx_id, req.account_id))

        return {"status": "ABORTED", "gid": gid, "shard_id": SHARD_ID, "replica_id": REPLICA_ID}
    finally:
        conn.close()


@app.get("/2pc/in_flight")
def get_in_flight_prepared(start_acc: int = 1, end_acc: int = 9999):
    """Returns count and list of currently prepared (in-flight) 2PC transactions on this shard."""
    conn = get_db_conn(autocommit=True)
    try:
        cur = conn.cursor()
        cur.execute("SELECT gid FROM pg_prepared_xacts;")
        gids = [r[0] for r in cur.fetchall()]
        matching = []
        for g in gids:
            parts = g.split("_")
            if len(parts) >= 3:
                acc_id = parts[-1]
                acc_num = parse_account_num(acc_id)
                if start_acc <= acc_num <= end_acc:
                    matching.append(g)
        return {"in_flight_count": len(matching), "gids": matching}
    finally:
        conn.close()


# =====================================================================
# 5. BONUS I: LIVE RE-SHARDING ENDPOINTS (Snapshot, Delta Stream, Fencing)
# =====================================================================
@app.get("/migration/snapshot")
def take_migration_snapshot(start_acc: int, end_acc: int, cursor_acc: int = 0, batch_size: int = 200):
    """
    Takes a repeatable-read snapshot batch of accounts in [max(start_acc, cursor_acc+1) .. end_acc]
    along with the current maximum change_log sequence ID (high-water mark).
    """
    conn = get_db_conn(autocommit=False)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;")
        cur.execute("SELECT COALESCE(MAX(seq_id), 0) AS max_seq_id FROM change_log;")
        max_seq_id = cur.fetchone()["max_seq_id"]

        lower_bound = max(start_acc, cursor_acc + 1)
        cur.execute("""
            SELECT account_id, balance
            FROM accounts
            WHERE CAST(SUBSTRING(account_id FROM 2) AS INT) BETWEEN %s AND %s
            ORDER BY account_id ASC
            LIMIT %s;
        """, (lower_bound, end_acc, batch_size))
        rows = cur.fetchall()
        conn.commit()
        return {"max_seq_id": max_seq_id, "accounts": rows}
    finally:
        conn.close()


@app.get("/migration/changes")
def get_migration_changes(start_acc: int, end_acc: int, since_seq_id: int, limit: int = 500):
    """Fetches ordered change capture log entries since `since_seq_id` for the migrating account range."""
    conn = get_db_conn(autocommit=True)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT seq_id, op_id, account_id, new_balance
            FROM change_log
            WHERE seq_id > %s
              AND CAST(SUBSTRING(account_id FROM 2) AS INT) BETWEEN %s AND %s
            ORDER BY seq_id ASC
            LIMIT %s;
        """, (since_seq_id, start_acc, end_acc, limit))
        return {"changes": cur.fetchall()}
    finally:
        conn.close()


@app.post("/migration/apply_snapshot")
def apply_snapshot_batch(req: ReplayBatchRequest):
    """Idempotently inserts snapshot accounts into the target shard (Shard C)."""
    conn = get_db_conn(autocommit=False)
    try:
        cur = conn.cursor()
        for item in req.items:
            cur.execute("""
                INSERT INTO accounts (account_id, balance, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (account_id) DO UPDATE SET balance = EXCLUDED.balance;
            """, (item["account_id"], item["balance"]))
        conn.commit()
        return {"applied": len(req.items)}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@app.post("/migration/apply_changes")
def apply_delta_changes(req: ReplayBatchRequest):
    """Idempotently replays ordered change log entries onto the target shard (Shard C)."""
    conn = get_db_conn(autocommit=False)
    try:
        cur = conn.cursor()
        applied_count = 0
        for chg in req.items:
            op_id = chg["op_id"]
            cur.execute("SELECT 1 FROM applied_migration_ops WHERE op_id = %s;", (op_id,))
            if cur.fetchone():
                continue
            cur.execute("""
                INSERT INTO accounts (account_id, balance, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (account_id) DO UPDATE SET balance = EXCLUDED.balance, updated_at = NOW();
            """, (chg["account_id"], chg["new_balance"]))
            cur.execute("INSERT INTO applied_migration_ops (op_id) VALUES (%s);", (op_id,))
            applied_count += 1
        conn.commit()
        return {"applied": applied_count}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@app.post("/migration/fencing")
def update_shard_fencing(req: FencingUpdateRequest):
    """Updates durable epoch and range fencing rules on this shard's database."""
    conn = get_db_conn(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO shard_fencing_rules (start_acc, end_acc, min_epoch, is_fenced, paused_for_cutover)
            VALUES (%s, %s, %s, %s, %s);
        """, (req.start_acc, req.end_acc, req.min_epoch, req.is_fenced, req.paused_for_cutover))
        conn.commit()
        logger.info(
            f"FENCING RULE UPDATED: A{req.start_acc:04d}-A{req.end_acc:04d} "
            f"min_epoch={req.min_epoch} fenced={req.is_fenced} paused={req.paused_for_cutover}"
        )
        return {"status": "updated"}
    finally:
        conn.close()


@app.post("/migration/cleanup")
def cleanup_migrated_range(start_acc: int, end_acc: int):
    """Removes old account rows from the previous owner shard only after verified migration completion."""
    conn = get_db_conn(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM accounts
            WHERE CAST(SUBSTRING(account_id FROM 2) AS INT) BETWEEN %s AND %s;
        """, (start_acc, end_acc))
        deleted = cur.rowcount
        conn.commit()
        logger.info(f"CLEANUP: Removed {deleted} migrated accounts (A{start_acc:04d}-A{end_acc:04d}) from {SHARD_ID}")
        return {"deleted_accounts": deleted}
    finally:
        conn.close()