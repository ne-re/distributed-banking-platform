import sys
import argparse
import json
import requests

GATEWAY_URL = "http://localhost:8000"


def main():
    parser = argparse.ArgumentParser(description="Distributed Banking Platform Client (Talks only to Gateway)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # 1. Balance inquiry
    p_bal = sub.add_parser("balance", help="Check account balance")
    p_bal.add_argument("account_id", help="e.g. A0001 or A1500")

    # 2. Deposit
    p_dep = sub.add_parser("deposit", help="Deposit funds into an account")
    p_dep.add_argument("account_id", help="e.g. A0001")
    p_dep.add_argument("amount", type=int, help="Positive integer amount")

    # 3. Withdraw
    p_wth = sub.add_parser("withdraw", help="Withdraw funds from an account")
    p_wth.add_argument("account_id", help="e.g. A0001")
    p_wth.add_argument("amount", type=int, help="Positive integer amount")

    # 4. Transfer (Same-shard or Cross-shard 2PC)
    p_tx = sub.add_parser("transfer", help="Transfer funds between any two accounts")
    p_tx.add_argument("from_account", help="Source account, e.g. A0500")
    p_tx.add_argument("to_account", help="Destination account, e.g. A1500")
    p_tx.add_argument("amount", type=int, help="Positive integer amount")

    # 5. Query Transaction Status
    p_st = sub.add_parser("tx_status", help="Query 2PC transaction status by ID")
    p_st.add_argument("tx_id", help="Transaction ID")

    args = parser.parse_args()

    try:
        if args.cmd == "balance":
            r = requests.get(f"{GATEWAY_URL}/balance/{args.account_id}")
        elif args.cmd == "deposit":
            r = requests.post(f"{GATEWAY_URL}/deposit", json={"account_id": args.account_id, "amount": args.amount})
        elif args.cmd == "withdraw":
            r = requests.post(f"{GATEWAY_URL}/withdraw", json={"account_id": args.account_id, "amount": args.amount})
        elif args.cmd == "transfer":
            r = requests.post(f"{GATEWAY_URL}/transfer", json={
                "from_account": args.from_account,
                "to_account": args.to_account,
                "amount": args.amount
            })
        elif args.cmd == "tx_status":
            r = requests.get(f"{GATEWAY_URL}/transactions/{args.tx_id}")

        print(f"HTTP {r.status_code}:")
        print(json.dumps(r.json(), indent=2))
    except Exception as e:
        print(f"Error communicating with Gateway: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
    