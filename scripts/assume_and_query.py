"""Prove Lake Formation actually restricts, by running the SAME query as
two different roles.

Run in CloudShell:

    python3 assume_and_query.py --role maple-analyst-role \
        --sql "SELECT * FROM maple_curated.customer_pii LIMIT 5"

    python3 assume_and_query.py --role maple-fraud-role \
        --sql "SELECT * FROM maple_curated.customer_pii LIMIT 5"

    # side-by-side, the money shot for your screenshots:
    python3 assume_and_query.py --compare

Why this exists: as an admin you see everything, so testing the policy as
yourself proves nothing. sts:AssumeRole gives you a session that IS the
analyst, and Lake Formation decides what that session may see.

> 📝 EXAM: this is the trust-policy / AssumeRole pattern from Day 1, used in
  anger. The roles trust your ACCOUNT (so a permitted principal in it can
  become them); their permission policies grant Athena + Glue + the results
  bucket, and notably NOT s3:GetObject on the PII prefix - under Lake
  Formation, S3 access is VENDED by LF at query time. Granting a role direct
  S3 access to the data would let it bypass the column filters entirely,
  which is the most common Lake Formation mistake.
"""

from __future__ import annotations

import argparse
import sys
import time

import boto3

REGION = "ca-central-1"
DATABASE = "maple_curated"
DEFAULT_SQL = "SELECT * FROM maple_curated.customer_pii LIMIT 5"


def session_for_role(role_name: str) -> boto3.Session:
    account = boto3.client("sts").get_caller_identity()["Account"]
    arn = f"arn:aws:iam::{account}:role/{role_name}"
    creds = boto3.client("sts").assume_role(
        RoleArn=arn, RoleSessionName=f"lf-test-{int(time.time())}"
    )["Credentials"]
    return boto3.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name=REGION,
    )


def run_query(session: boto3.Session, sql: str, workgroup: str):
    """Returns (state, rows, message). A denial IS a result here, so this
    never raises for an access failure."""
    athena = session.client("athena")
    try:
        qid = athena.start_query_execution(
            QueryString=sql,
            WorkGroup=workgroup,
            QueryExecutionContext={"Database": DATABASE},
        )["QueryExecutionId"]
    except Exception as exc:
        return "START_DENIED", [], str(exc)

    while True:
        info = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = info["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)

    if state != "SUCCEEDED":
        return state, [], info["Status"].get("StateChangeReason", "")

    result = athena.get_query_results(QueryExecutionId=qid, MaxResults=20)
    rows = [[c.get("VarCharValue", "") for c in r["Data"]]
            for r in result["ResultSet"]["Rows"]]
    return state, rows, ""


def show(role: str, sql: str, workgroup: str) -> None:
    print("=" * 78)
    print(f"ROLE: {role}")
    print(f"SQL : {sql}")
    print("=" * 78)
    state, rows, message = run_query(session_for_role(role), sql, workgroup)

    if state != "SUCCEEDED":
        print(f"  {state}")
        print(f"  {message[:400]}")
        if "Insufficient Lake Formation permission" in message:
            print("\n  ^ Lake Formation refused the columns. That is the control working.")
        print()
        return

    if not rows:
        print("  (no rows)\n")
        return

    header, data = rows[0], rows[1:]
    widths = [max(len(str(r[i])) for r in rows) for i in range(len(header))]
    print("  " + " | ".join(h.ljust(w) for h, w in zip(header, widths)))
    print("  " + "-+-".join("-" * w for w in widths))
    for row in data:
        print("  " + " | ".join(str(v).ljust(w) for v, w in zip(row, widths)))
    print(f"\n  columns visible to this role: {len(header)}  ({', '.join(header)})")
    print()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--role", help="role name to assume, e.g. maple-analyst-role")
    p.add_argument("--sql", default=DEFAULT_SQL)
    p.add_argument("--workgroup", default="maple-wg")
    p.add_argument("--compare", action="store_true",
                   help="run the standard demo queries as analyst AND fraud")
    args = p.parse_args()

    if args.compare:
        for sql in (
            "SELECT * FROM maple_curated.customer_pii LIMIT 5",
            "SELECT customer_id, email FROM maple_curated.customer_pii LIMIT 5",
            "SELECT province, count(*) AS rows_visible FROM maple_curated.customer_pii GROUP BY province",
        ):
            for role in ("maple-analyst-role", "maple-fraud-role"):
                show(role, sql, args.workgroup)
    elif args.role:
        show(args.role, args.sql, args.workgroup)
    else:
        sys.exit("Pass --role <name> or --compare")


if __name__ == "__main__":
    main()
