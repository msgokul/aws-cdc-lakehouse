"""Generate a SYNTHETIC PII layer for Maple & Co.'s customers.

Everything produced here is fabricated from word lists and a seeded RNG -
no real personal data is used, generated, or stored. That is deliberate:
Lake Formation column/row security, S3 Object Lambda redaction and Macie
discovery are only meaningful when there is something PII-shaped to protect,
and a portfolio project must never handle real personal data to prove it.

Run in CloudShell (boto3 + credentials are already there):

    python3 generate_pii.py --bucket maple-sprint-curated --workgroup maple-wg
    python3 generate_pii.py --bucket maple-sprint-curated --workgroup maple-wg --limit 5000

What it does:
  1. asks Athena for the customer_ids in maple_curated.dim_customer
     (using the same async start -> poll -> fetch pattern as the Step
     Functions workflow, in ~20 lines of Python)
  2. fabricates a name, email, phone, postal code and SIN-shaped id per
     customer, deterministically seeded from the customer_id so re-runs
     produce identical data
  3. writes one CSV to s3://<bucket>/pii/customer_pii.csv
  4. creates the Athena/Glue table maple_curated.customer_pii over it and
     reports the row count (pass --skip-table to only print the DDL)

> 📝 EXAM: the *reason* this table sits in its own S3 prefix is that both
  masking mechanisms need a boundary to act on - Lake Formation registers a
  LOCATION and grants column/row access on the catalog table; S3 Object
  Lambda attaches to an access point over a prefix. Mixing PII into an
  existing table's prefix makes both harder.
"""

from __future__ import annotations

import argparse
import csv
import io
import random
import sys
import time

import boto3

REGION = "ca-central-1"
DATABASE = "maple_curated"

FIRST = ["Aiden", "Priya", "Marc", "Chloe", "Devon", "Amara", "Liam", "Sofia",
         "Noah", "Jasleen", "Owen", "Mei", "Ethan", "Fatima", "Lucas", "Nia",
         "Hugo", "Ines", "Ravi", "Elena", "Caleb", "Yuki", "Malik", "Rosa"]
LAST = ["Tremblay", "Singh", "Nguyen", "Patel", "Lefebvre", "Okafor", "Brown",
        "Silva", "Gagnon", "Kaur", "Chen", "Roy", "Diallo", "Martin", "Ali",
        "Cote", "Wong", "Bouchard", "Haddad", "Novak", "Ferreira", "Ivanov"]
DOMAINS = ["example.com", "example.net", "mailinator.test", "sample.invalid"]
PROVINCE_LETTERS = {"ON": "M", "QC": "H", "BC": "V", "AB": "T", "MB": "R",
                    "SK": "S", "NS": "B", "NB": "E", "NL": "A", "PE": "C",
                    "YT": "Y", "NT": "X"}


# --------------------------------------------------------------------------
# Athena: start -> poll -> fetch  (the Step Functions pattern, in Python)
# --------------------------------------------------------------------------
def athena_run(sql: str, workgroup: str) -> str:
    """Start a query, poll to completion, return its execution id."""
    athena = boto3.client("athena", region_name=REGION)
    qid = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=workgroup,
        QueryExecutionContext={"Database": DATABASE},
    )["QueryExecutionId"]

    while True:
        info = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = info["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(3)

    if state != "SUCCEEDED":
        sys.exit(f"Athena query {state}: "
                 f"{info['Status'].get('StateChangeReason', 'no reason given')}")
    return qid


def athena_query(sql: str, workgroup: str) -> list[list[str]]:
    athena = boto3.client("athena", region_name=REGION)
    qid = athena_run(sql, workgroup)

    rows: list[list[str]] = []
    paginator = athena.get_paginator("get_query_results")
    first_page = True
    for page in paginator.paginate(QueryExecutionId=qid):
        for row in page["ResultSet"]["Rows"]:
            if first_page:            # first row of the first page is the header
                first_page = False
                continue
            rows.append([c.get("VarCharValue", "") for c in row["Data"]])
    return rows


# --------------------------------------------------------------------------
# Synthetic PII, deterministic per customer_id
# --------------------------------------------------------------------------
def fabricate(customer_id: str, province: str) -> dict:
    rng = random.Random(customer_id)          # same id -> same fake person
    first, last = rng.choice(FIRST), rng.choice(LAST)
    letter = PROVINCE_LETTERS.get(province, "M")
    postal = (f"{letter}{rng.randint(1,9)}{rng.choice('ABCEGHJKLMNPRSTVXY')} "
              f"{rng.randint(0,9)}{rng.choice('ABCEGHJKLMNPRSTVXY')}{rng.randint(0,9)}")
    return {
        "customer_id": customer_id,
        "full_name": f"{first} {last}",
        "email": f"{first.lower()}.{last.lower()}{rng.randint(1, 999)}@{rng.choice(DOMAINS)}",
        "phone": f"({rng.randint(200, 999)}) {rng.randint(200, 999)}-{rng.randint(1000, 9999)}",
        "postal_code": postal,
        # SIN-shaped, deliberately from the 000-prefixed test range so it can
        # never collide with a real Social Insurance Number:
        "sin_test": f"000-{rng.randint(100, 999)}-{rng.randint(100, 999)}",
        "province": province,
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", required=True, help="curated bucket, e.g. maple-sprint-curated")
    p.add_argument("--workgroup", default="maple-wg")
    p.add_argument("--prefix", default="pii")
    p.add_argument("--limit", type=int, default=0, help="0 = all customers")
    p.add_argument("--skip-table", action="store_true",
                   help="only print the DDL instead of creating the table")
    args = p.parse_args()

    limit = f" LIMIT {args.limit}" if args.limit else ""
    sql = ("SELECT customer_id, province FROM maple_curated.dim_customer "
           f"WHERE is_current = true ORDER BY customer_id{limit}")
    print(f"Querying customer ids via Athena (workgroup {args.workgroup}) ...")
    rows = athena_query(sql, args.workgroup)
    print(f"  {len(rows):,} customers")
    if not rows:
        sys.exit("No customers returned - is maple_curated.dim_customer populated?")

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=[
        "customer_id", "full_name", "email", "phone",
        "postal_code", "sin_test", "province"])
    writer.writeheader()
    for customer_id, province in rows:
        writer.writerow(fabricate(customer_id, province or "ON"))

    key = f"{args.prefix}/customer_pii.csv"
    boto3.client("s3", region_name=REGION).put_object(
        Bucket=args.bucket, Key=key, Body=buf.getvalue().encode()
    )
    size_mb = len(buf.getvalue()) / 1024 / 1024
    print(f"Wrote s3://{args.bucket}/{key}  ({size_mb:.1f} MB, {len(rows):,} rows)")

    ddl = f"""CREATE EXTERNAL TABLE IF NOT EXISTS maple_curated.customer_pii (
  customer_id  string,
  full_name    string,
  email        string,
  phone        string,
  postal_code  string,
  sin_test     string,
  province     string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
LOCATION 's3://{args.bucket}/{args.prefix}/'
TBLPROPERTIES ('skip.header.line.count' = '1')"""

    if args.skip_table:
        print("\n--skip-table given; register it yourself in Athena:\n")
        print(ddl + ";")
    else:
        print("\nRegistering maple_curated.customer_pii in the Glue catalog ...")
        athena_run(ddl, args.workgroup)
        count = athena_query(
            "SELECT count(*) FROM maple_curated.customer_pii", args.workgroup
        )
        print(f"  table created; it reports {count[0][0]} rows")
        print("\nVerify:  SELECT * FROM maple_curated.customer_pii LIMIT 5;")

    print("\nAll data above is SYNTHETIC. Never point this at real customer data.")


if __name__ == "__main__":
    main()
