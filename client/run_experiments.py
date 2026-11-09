import sys
import time
import argparse
import threading
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
import requests

GATEWAY_URL = "http://localhost:8000"
COORDINATOR_URL = "http://localhost:8001"
HEALER_URL = "http://localhost:8002"

DB_A_DSN = "postgresql://bankuser:bankpassword@localhost:5432/shard_a_db"
DB_B_DSN = "postgresql://bankuser:bankpassword@localhost:5433/shard_b_db"
DB_C_DSN = "postgresql://bankuser:bankpassword@localhost:5434/shard_c_db"


def banner(title: str):
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def run_cmd(cmd: list[str]):
    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


# =====================================================================
# EXPERIMENT 1: SHARD DISTRIBUTION
# =====================================================================
def exp1_shard_distribution():
    banner("EXPERIMENT 1: Shard Distribution & Gateway Routing")
    conn_a = psycopg2.connect(DB_A_DSN)
    cur_a = conn_a.cursor()
    cur_a.execute("SELECT COUNT(*), MIN(account_id), MAX(account_id) FROM accounts;")
    cnt_a, min_a, max_a = cur_a.fetchone()
    conn_a.close()

    conn_b = psycopg2.connect(DB_B_DSN)
    cur_b = conn_b.cursor()
    cur_b.execute("SELECT COUNT(*), MIN(account_id), MAX(account_id) FROM accounts;")
    cnt_b, min_b, max_b = cur_b.fetchone()
    conn_b.close()

    print(f"[DB Check] Database A (ShardA): count={cnt_a}, range={min_a}..{max_a}")
    print(f"[DB Check] Database B (ShardB): count={cnt_b}, range={min_b}..{max_b}")
    assert cnt_a == 1000 and min_a == "A0001" and max_a == "A1000", "ShardA DB partition mismatch!"
    assert min_b == "A1001", "ShardB DB partition mismatch!"

    r_a = requests.get(f"{GATEWAY_URL}/balance/A0010").json()
    r_b = requests.get(f"{GATEWAY_URL}/balance/A1010").json()
    print(f"[Routing] Gateway routed A0010 -> shard={r_a['shard_id']}, replica={r_a['replica_id']}, balance={r_a['balance']}")
    print(f"[Routing] Gateway routed A1010 -> shard={r_b['shard_id']}, replica={r_b['replica_id']}, balance={r_b['balance']}")
    assert r_a["shard_id"] == "ShardA" and r_b["shard_id"] == "ShardB"
    print("[PASS] Experiment 1 verified.")


# =====================================================================
# EXPERIMENT 2: REPLICA LOAD DISTRIBUTION
# =====================================================================
def exp2_replica_load_distribution():
    banner("EXPERIMENT 2: Replica Load Distribution (Round-Robin)")
    replicas_seen = {}
    for i in range(1, 21):
        r = requests.get(f"{GATEWAY_URL}/balance/A0001").json()
        rep = r["replica_id"]
        replicas_seen[rep] = replicas_seen.get(rep, 0) + 1
        print(f"  Request #{i:02d} for A0001 handled by -> {rep}")

    print(f"\nSummary of 20 requests to ShardA: {replicas_seen}")
    assert len(replicas_seen) >= 2, "Expected requests to be spread across multiple ShardA replicas!"
    print("[PASS] Experiment 2 verified.")


# =====================================================================
# EXPERIMENT 3: CONCURRENT REPLICA OPERATIONS
# =====================================================================
def exp3_concurrent_replica_operations():
    banner("EXPERIMENT 3: Concurrent Operations Across Replicas of One Shard")
    acc = "A0050"
    initial = requests.get(f"{GATEWAY_URL}/balance/{acc}").json()["balance"]
    print(f"Initial balance of {acc}: {initial}")
    print("Launching 40 concurrent operations (20 deposits of +10, 20 withdrawals of -5)...")

    def op_worker(idx: int):
        if idx % 2 == 0:
            return requests.post(f"{GATEWAY_URL}/deposit", json={"account_id": acc, "amount": 10}).json()
        else:
            return requests.post(f"{GATEWAY_URL}/withdraw", json={"account_id": acc, "amount": 5}).json()

    replicas_used = set()
    with ThreadPoolExecutor(max_workers=16) as ex:
        futures = [ex.submit(op_worker, i) for i in range(40)]
        for f in as_completed(futures):
            res = f.result()
            replicas_used.add(res["replica_id"])

    final = requests.get(f"{GATEWAY_URL}/balance/{acc}").json()["balance"]
    expected = initial + (20 * 10) - (20 * 5)
    print(f"Replicas that processed concurrent requests: {replicas_used}")
    print(f"Final balance of {acc}: {final} (Expected: {expected})")
    assert final == expected, f"Lost update detected! {final} != {expected}"
    print("[PASS] Experiment 3 verified (Zero lost updates under concurrency).")


# =====================================================================
# EXPERIMENT 4: REPLICA INTERRUPTION & GATEWAY FAILOVER
# =====================================================================
def exp4_replica_interruption():
    banner("EXPERIMENT 4: Replica Interruption Under Load")
    requests.post(f"{HEALER_URL}/toggle", json={"enabled": False})

    print("1. Stopping replica container `bank_a2` while sending traffic to ShardA...")
    run_cmd(["docker", "stop", "bank_a2"])
    time.sleep(2.0)

    replicas_after_stop = set()
    for i in range(1, 11):
        r = requests.get(f"{GATEWAY_URL}/balance/A0002")
        assert r.status_code == 200, "Gateway failed to route around stopped replica!"
        rep = r.json()["replica_id"]
        replicas_after_stop.add(rep)
        print(f"  Request #{i:02d} routed to healthy replica -> {rep}")

    print(f"Replicas selected while `bank_a2` was stopped: {replicas_after_stop}")
    assert "bank_a2" not in replicas_after_stop

    print("2. Restarting `bank_a2` and waiting for Gateway health check to detect recovery...")
    run_cmd(["docker", "start", "bank_a2"])
    time.sleep(3.0)
    print("[PASS] Experiment 4 verified.")


# =====================================================================
# EXPERIMENT 5: LOCAL AND CROSS-SHARD TRANSFERS
# =====================================================================
def exp5_local_and_cross_shard_transfers():
    banner("EXPERIMENT 5: Same-Shard Local Transfer vs. Cross-Shard 2PC Transfer")
    # 1. Same-Shard Transfer
    b1_before = requests.get(f"{GATEWAY_URL}/balance/A0100").json()["balance"]
    b2_before = requests.get(f"{GATEWAY_URL}/balance/A0101").json()["balance"]
    res_local = requests.post(f"{GATEWAY_URL}/transfer", json={
        "from_account": "A0100", "to_account": "A0101", "amount": 200
    }).json()
    print(f"[Same-Shard Transfer A0100 -> A0101 (200)]: {res_local}")
    assert res_local["type"] == "same_shard"
    assert res_local["from_balance"] == b1_before - 200 and res_local["to_balance"] == b2_before + 200

    # 2. Cross-Shard Transfer (2PC)
    a500_before = requests.get(f"{GATEWAY_URL}/balance/A0500").json()["balance"]
    a1500_before = requests.get(f"{GATEWAY_URL}/balance/A1500").json()["balance"]
    res_2pc = requests.post(f"{GATEWAY_URL}/transfer", json={
        "from_account": "A0500", "to_account": "A1500", "amount": 300
    }).json()
    print(f"[Cross-Shard 2PC Transfer A0500 -> A1500 (300)]: {res_2pc}")
    a500_after = requests.get(f"{GATEWAY_URL}/balance/A0500").json()["balance"]
    a1500_after = requests.get(f"{GATEWAY_URL}/balance/A1500").json()["balance"]
    print(f"Balances: A0500 ({a500_before} -> {a500_after}), A1500 ({a1500_before} -> {a1500_after})")
    assert a500_after == a500_before - 300 and a1500_after == a1500_before + 300

    # 3. Scenario A: One shard rejects (Insufficient funds) -> Abort safely
    r_fail = requests.post(f"{GATEWAY_URL}/transfer", json={
        "from_account": "A0500", "to_account": "A1500", "amount": 9999999
    })
    print(f"[Scenario A - Insufficient Funds 2PC Abort]: HTTP {r_fail.status_code} -> {r_fail.json()}")
    assert r_fail.status_code == 400
    print("[PASS] Experiment 5 verified.")


# =====================================================================
# EXPERIMENT 6: INTERRUPTED DISTRIBUTED TRANSACTION & RECOVERY
# =====================================================================
def exp6_interrupted_distributed_transaction():
    banner("EXPERIMENT 6: Coordinator Crash After Durable COMMIT_DECIDED & Recovery")
    a600_before = requests.get(f"{GATEWAY_URL}/balance/A0600").json()["balance"]
    a1600_before = requests.get(f"{GATEWAY_URL}/balance/A1600").json()["balance"]
    print(f"Balances before interrupted 2PC: A0600={a600_before}, A1600={a1600_before}")

    print("1. Sending Cross-Shard Transfer (A0600 -> A1600, amount=250) with crash after COMMIT_DECIDED...")
    r = requests.post(f"{GATEWAY_URL}/transfer", json={
        "from_account": "A0600",
        "to_account": "A1600",
        "amount": 250,
        "simulate_crash_after_commit_decide": True
    })
    print(f"Gateway response when Coordinator crashed mid-commit: HTTP {r.status_code} - {r.json()}")

    # Check pg_prepared_xacts on Database B to show the transaction is durably prepared and holding lock
    conn_b = psycopg2.connect(DB_B_DSN)
    cur_b = conn_b.cursor()
    cur_b.execute("SELECT gid, prepared FROM pg_prepared_xacts;")
    prepared_list = cur_b.fetchall()
    conn_b.close()
    print(f"2. Database B `pg_prepared_xacts` while Coordinator is down: {prepared_list}")

    print("3. Restarting `coordinator` container to trigger durable state recovery...")
    run_cmd(["docker", "start", "coordinator"])
    time.sleep(4.5)

    a600_after = requests.get(f"{GATEWAY_URL}/balance/A0600").json()["balance"]
    a1600_after = requests.get(f"{GATEWAY_URL}/balance/A1600").json()["balance"]
    print(f"4. Balances after Coordinator recovery: A0600={a600_after}, A1600={a1600_after}")
    assert a600_after == a600_before - 250 and a1600_after == a1600_before + 250, "2PC recovery failed!"
    print("[PASS] Experiment 6 verified (Durable COMMIT_DECIDED completed upon restart).")


# =====================================================================
# EXPERIMENT 7: CONCURRENT CROSS-SHARD TRANSFERS & CONSERVATION OF MONEY
# =====================================================================
def exp7_concurrent_cross_shard_transfers():
    banner("EXPERIMENT 7: Concurrent Cross-Shard Transfers & Conservation of Money")
    accounts = ["A0701", "A0702", "A0703", "A1701", "A1702", "A1703"]
    before_balances = {a: requests.get(f"{GATEWAY_URL}/balance/{a}").json()["balance"] for a in accounts}
    total_before = sum(before_balances.values())
    print(f"Balances BEFORE concurrent transfers: {before_balances}")
    print(f"Total Money in System BEFORE: {total_before}")

    transfers = [
        ("A0701", "A1701", 50),
        ("A1701", "A0702", 30),
        ("A0702", "A1702", 40),
        ("A1702", "A0703", 25),
        ("A0703", "A1703", 60),
        ("A1703", "A0701", 35),
        ("A0701", "A1702", 20),
        ("A1701", "A0703", 15),
    ]

    def tx_worker(t):
        src, dst, amt = t
        for attempt in range(5):
            r = requests.post(f"{GATEWAY_URL}/transfer", json={
                "from_account": src, "to_account": dst, "amount": amt
            })
            if r.status_code == 200:
                return True
            time.sleep(0.12 * (attempt + 1))
        return False

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(tx_worker, transfers))

    after_balances = {a: requests.get(f"{GATEWAY_URL}/balance/{a}").json()["balance"] for a in accounts}
    total_after = sum(after_balances.values())
    print(f"Balances AFTER concurrent transfers:  {after_balances}")
    print(f"Total Money in System AFTER:  {total_after}")
    assert total_before == total_after, f"Conservation of money violated! {total_before} != {total_after}"
    print("[PASS] Experiment 7 verified (100% Conservation of Money preserved).")


# =====================================================================
# BONUS I (+12%): LIVE RE-SHARDING UNDER ADVERSARIAL FAILURES
# =====================================================================
def bonus1_live_resharding():
    banner("BONUS I (+12%): Live Re-Sharding Under Adversarial Failures (ShardB -> ShardC)")
    stop_traffic = False
    deposit_ok = 0
    tx_ok = 0

    # 1. Run continuous concurrent deposits AND transfers on migrating accounts (A1505, A1506)
    def background_load():
        nonlocal deposit_ok, tx_ok
        i = 0
        while not stop_traffic:
            i += 1
            try:
                r1 = requests.post(f"{GATEWAY_URL}/deposit", json={
                    "account_id": "A1505",
                    "amount": 1,
                    "request_id": f"mig_dep_{i}_{ int(time.time()*1000) }"
                }, timeout=4)
                if r1.status_code == 200:
                    deposit_ok += 1

                r2 = requests.post(f"{GATEWAY_URL}/transfer", json={
                    "from_account": "A0200",
                    "to_account": "A1506",
                    "amount": 1,
                    "request_id": f"mig_tx_{i}_{ int(time.time()*1000) }"
                }, timeout=6)
                if r2.status_code == 200:
                    tx_ok += 1
            except Exception:
                pass
            time.sleep(0.08)

    t = threading.Thread(target=background_load, daemon=True)
    t.start()

    # 2. Test Crash Recovery across all 4 required phases:
    #    (i) SNAPSHOT, (ii) DELTA_REPLAY, (iii) FENCING, (iv) AFTER_ACTIVATION
    for crash_phase in ["SNAPSHOT", "DELTA_REPLAY", "FENCING", "AFTER_ACTIVATION"]:
        print(f"\n---> Injecting Coordinator crash during migration phase: {crash_phase}")
        try:
            requests.post(f"{COORDINATOR_URL}/migration/start", json={
                "migration_id": "mig_B_to_C_1501_2000",
                "simulate_crash_at_phase": crash_phase
            }, timeout=6)
        except Exception:
            print(f"     Coordinator terminated abruptly during {crash_phase}! Restarting container...")

        run_cmd(["docker", "start", "coordinator"])
        time.sleep(1.5)

    # Now let the Coordinator background recovery finish the CLEANUP -> COMPLETED phase
    print("\nWaiting for Coordinator automatic recovery to converge to COMPLETED...")
    for _ in range(25):
        time.sleep(1.0)
        try:
            st = requests.get(f"{COORDINATOR_URL}/migration/status/mig_B_to_C_1501_2000", timeout=2).json()
            print(f"  [Migration State Machine] phase={st['phase']}, cursor={st['cursor_acc']}, epoch={st['target_epoch']}")
            if st["phase"] == "COMPLETED":
                break
        except Exception:
            pass

    stop_traffic = True
    t.join()
    print(f"\nConcurrent operations completed during migration: deposits={deposit_ok}, 2PC_transfers={tx_ok}")

    # 3. Verify A1505 and A1506 are now owned and served by ShardC
    r_c = requests.get(f"{GATEWAY_URL}/balance/A1505").json()
    print(f"Account A1505 is now served by: shard={r_c['shard_id']}, replica={r_c['replica_id']}, balance={r_c['balance']}")
    assert r_c["shard_id"] == "ShardC", "A1505 should now be owned by ShardC!"

    # 4. Prove Stale-Route Fencing at the Shard Write Path:
    #    Deliberately send a stale request (epoch=1) directly to old owner `bank_b1` inside the network
    print("\nProving Stale-Route Fencing: Deliberately sending stale write (epoch=1) directly to `bank_b1`...")
    stale_check = subprocess.run(
        [
            "docker", "exec", "gateway", "python", "-c",
            "import requests; r = requests.post('http://bank_b1:8000/deposit', json={'account_id':'A1505','amount':50,'epoch':1}); print(r.status_code, r.text)"
        ],
        capture_output=True, text=True, check=True
    )
    print(f"Direct write to old owner `bank_b1` returned: {stale_check.stdout.strip()}")
    assert "409" in stale_check.stdout and "SHARD_FENCED" in stale_check.stdout

    # 5. Verify old data was cleaned up from ShardB database after completion
    conn_b = psycopg2.connect(DB_B_DSN)
    cur_b = conn_b.cursor()
    cur_b.execute("SELECT COUNT(*) FROM accounts WHERE CAST(SUBSTRING(account_id FROM 2) AS INT) BETWEEN 1501 AND 2000;")
    leftover_in_b = cur_b.fetchone()[0]
    conn_b.close()
    print(f"Leftover A1501-A2000 rows in ShardB DB after verified cleanup: {leftover_in_b}")
    assert leftover_in_b == 0
    print("[PASS] Bonus I verified 100%.")


# =====================================================================
# BONUS II (+10%): AUTONOMOUS SELF-HEALING CONTROLLER
# =====================================================================
def bonus2_autonomous_self_healing():
    banner("BONUS II (+10%): Autonomous Self-Healing Controller")
    requests.post(f"{HEALER_URL}/toggle", json={"enabled": True})

    # 1. Start concurrent banking load on ShardA while injecting a replica crash
    stop_load = False
    success_reqs = 0
    failed_reqs = 0

    def load_worker():
        nonlocal success_reqs, failed_reqs
        while not stop_load:
            try:
                r = requests.post(f"{GATEWAY_URL}/deposit", json={"account_id": "A0088", "amount": 1}, timeout=4)
                if r.status_code == 200:
                    success_reqs += 1
                else:
                    failed_reqs += 1
            except Exception:
                failed_reqs += 1
            time.sleep(0.05)

    t = threading.Thread(target=load_worker, daemon=True)
    t.start()

    print("1. Killing `bank_a1` container under active concurrent banking load...")
    run_cmd(["docker", "kill", "bank_a1"])

    healed = False
    for _ in range(15):
        time.sleep(1.5)
        st = requests.get(f"{HEALER_URL}/status").json()
        if st["events"]:
            print(f"2. Self-Healer spawned & registered replacement: {st['events'][-1]}")
            healed = True
            break

    stop_load = True
    t.join()
    assert healed, "Self-Healer did not spawn a replacement container!"
    print(f"Concurrent requests during crash & healing: succeeded={success_reqs}, failed={failed_reqs}")

    # 3. Test Controller Crash Recovery (Must NOT create duplicate replicas!)
    print("3. Testing Self-Healer controller crash recovery (`docker restart self_healer`)...")
    run_cmd(["docker", "restart", "self_healer"])
    time.sleep(2.5)
    requests.post(f"{HEALER_URL}/toggle", json={"enabled": True})
    time.sleep(3.0)

    gw_status = requests.get(f"{GATEWAY_URL}/admin/status").json()
    shard_a_replicas = gw_status["replicas"]["ShardA"]
    print(f"4. Gateway ShardA active replicas after controller restart: {list(shard_a_replicas.keys())}")
    assert len(shard_a_replicas) == 2, f"Expected exactly 2 replicas, found {len(shard_a_replicas)}"
    print("[PASS] Bonus II verified 100% (Zero duplicate replicas after controller crash).")


EXPERIMENTS = {
    "1": ("Experiment 1: Shard Distribution", exp1_shard_distribution),
    "2": ("Experiment 2: Replica Load Distribution", exp2_replica_load_distribution),
    "3": ("Experiment 3: Concurrent Replica Operations", exp3_concurrent_replica_operations),
    "4": ("Experiment 4: Replica Interruption", exp4_replica_interruption),
    "5": ("Experiment 5: Local & Cross-Shard Transfers", exp5_local_and_cross_shard_transfers),
    "6": ("Experiment 6: Interrupted 2PC & Recovery", exp6_interrupted_distributed_transaction),
    "7": ("Experiment 7: Concurrent Cross-Shard Transfers", exp7_concurrent_cross_shard_transfers),
    "bonus1": ("Bonus I (+12%): Live Re-Sharding", bonus1_live_resharding),
    "bonus2": ("Bonus II (+10%): Autonomous Self-Healing", bonus2_autonomous_self_healing),
}


def main():
    parser = argparse.ArgumentParser(description="Modular Experiment Runner for Demo Video")
    parser.add_argument(
        "target",
        nargs="?",
        default="menu",
        help="Which experiment to run: 1, 2, 3, 4, 5, 6, 7, bonus1, bonus2, all, or menu (default: menu)"
    )
    parser.add_argument(
        "--step",
        action="store_true",
        help="Pause for Enter key between experiments when running 'all'"
    )
    args = parser.parse_args()

    if args.target == "all":
        for key, (title, fn) in EXPERIMENTS.items():
            fn()
            if args.step:
                input(f"\n>>> [{title}] finished. Press ENTER to continue to the next experiment...")
    elif args.target in EXPERIMENTS:
        EXPERIMENTS[args.target][1]()
    else:
        while True:
            print("\n" + "#" * 65)
            print("   DISTRIBUTED BANKING PLATFORM - DEMO VIDEO RUNNER MENU")
            print("#" * 65)
            for k, (title, _) in EXPERIMENTS.items():
                print(f"  [{k:6s}] -> {title}")
            print("  [all   ] -> Run all experiments sequentially")
            print("  [q     ] -> Quit")
            choice = input("\nSelect an experiment to run: ").strip().lower()
            if choice in ("q", "quit", "exit"):
                break
            elif choice == "all":
                for _, (title, fn) in EXPERIMENTS.items():
                    fn()
                    input(f"\n>>> Press ENTER for next experiment...")
            elif choice in EXPERIMENTS:
                EXPERIMENTS[choice][1]()
            else:
                print("Invalid choice. Try again.")


if __name__ == "__main__":
    main()