"""Give the CloudWatch dashboard something real to show.

Two jobs, both honest — nothing here fabricates a datapoint:

  load     drives REAL activity through the platform (Athena queries, Lambda
           invocations including deliberate failures, DynamoDB writes,
           optionally a Step Functions execution) so the AWS service metrics
           actually have data.

  metrics  MEASURES the platform as it currently is (row counts, data quality
           score, minutes since the last successful pipeline run, bytes
           scanned and the resulting Athena spend) and publishes those to the
           custom namespace `MapleAnalytics`.

Run in CloudShell:

    python3 dashboard_data.py metrics                  # one measurement now
    python3 dashboard_data.py load                     # one burst of activity
    python3 dashboard_data.py both --minutes 20        # load + measure, every
                                                       # 60s for 20 minutes
                                                       # -> the dashboard gets
                                                       #    a LINE, not a dot

Leave `both --minutes 20` running while you take screenshots.

> 📝 EXAM: `PutMetricData` into a custom namespace is how you get *business*
  and *pipeline* KPIs into CloudWatch — freshness, records processed, quality
  score — alongside the service metrics AWS publishes for you. Custom metrics
  are billed per metric per month (the free tier covers 10), datapoints can be
  up to 2 weeks old or 2 hours in the future, and high-resolution metrics
  (StorageResolution=1) allow sub-minute alarms. This is the answer to "how do
  we alarm on data freshness / row counts?" — CloudWatch has no idea what a
  row is until you tell it.
"""

from __future__ import annotations

import argparse
import json
import random
import time
from datetime import datetime, timezone

import boto3

REGION = "ca-central-1"
NAMESPACE = "MapleAnalytics"
DATABASE = "maple_curated"
WORKGROUP = "maple-analytics-wg"
STATE_MACHINE = "maple-daily-pipeline"
DDB_TABLE = "maple-store-sales"
DQ_RULESET = "maple-fact-quality"
LAMBDA_OK = "maple-redact-extract"      # reads the PII CSV; succeeds
LAMBDA_ERR = "maple-stream-aggregator"  # given a missing key; fails on purpose

athena = boto3.client("athena", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)
sfn = boto3.client("stepfunctions", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
sts = boto3.client("sts")

ACCOUNT = sts.get_caller_identity()["Account"]


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------
def run_query(sql: str) -> tuple[str, int, int]:
    """Returns (state, bytes_scanned, millis). Never raises."""
    try:
        qid = athena.start_query_execution(
            QueryString=sql,
            WorkGroup=WORKGROUP,
            QueryExecutionContext={"Database": DATABASE},
        )["QueryExecutionId"]
    except Exception as exc:
        print(f"    query failed to start: {exc}")
        return "START_FAILED", 0, 0

    while True:
        info = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = info["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)

    stats = info.get("Statistics", {})
    return (state,
            stats.get("DataScannedInBytes", 0),
            stats.get("TotalExecutionTimeInMillis", 0))


def scalar(sql: str):
    """Run a query and return its single value, or None."""
    state, _, _ = run_query(sql)
    if state != "SUCCEEDED":
        return None
    qid = athena.list_query_executions(WorkGroup=WORKGROUP, MaxResults=1)["QueryExecutionIds"][0]
    rows = athena.get_query_results(QueryExecutionId=qid, MaxResults=2)["ResultSet"]["Rows"]
    if len(rows) < 2:
        return None
    return rows[1]["Data"][0].get("VarCharValue")


def put(name: str, value: float, unit: str = "None", dims: list | None = None) -> None:
    cw.put_metric_data(
        Namespace=NAMESPACE,
        MetricData=[{
            "MetricName": name,
            "Value": float(value),
            "Unit": unit,
            "Timestamp": datetime.now(timezone.utc),
            **({"Dimensions": dims} if dims else {}),
        }],
    )
    print(f"    {name:<26} {value:>14,.2f} {unit}")


# --------------------------------------------------------------------------
# metrics: measure what is actually there
# --------------------------------------------------------------------------
def cmd_metrics() -> None:
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] measuring ...")

    # 1. Row counts per curated table - one query, four numbers.
    sql = """
        SELECT 'fact_order_item' AS t, count(*) AS n FROM maple_curated.fact_order_item
        UNION ALL SELECT 'dim_customer', count(*) FROM maple_curated.dim_customer
        UNION ALL SELECT 'dim_product', count(*) FROM maple_curated.dim_product
        UNION ALL SELECT 'dim_date', count(*) FROM maple_curated.dim_date
    """
    state, scanned, millis = run_query(sql)
    if state == "SUCCEEDED":
        qid = athena.list_query_executions(WorkGroup=WORKGROUP, MaxResults=1)["QueryExecutionIds"][0]
        rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"][1:]
        total = 0
        for r in rows:
            table = r["Data"][0]["VarCharValue"]
            n = int(r["Data"][1]["VarCharValue"])
            total += n
            put("CuratedRowCount", n, "Count", [{"Name": "Table", "Value": table}])
        put("CuratedRowsTotal", total, "Count")
        put("QueryExecutionTime", millis, "Milliseconds")

    # 2. SCD2 health: how many customers carry more than one version.
    versions = scalar("""
        SELECT count(*) FROM (
          SELECT customer_id FROM maple_curated.dim_customer
          GROUP BY customer_id HAVING count(*) > 1)
    """)
    if versions is not None:
        put("Scd2VersionedCustomers", int(versions), "Count")

    # 3. Referential integrity: fact rows with no matching dimension version.
    orphans = scalar("""
        SELECT count(*) FROM maple_curated.fact_order_item f
        LEFT JOIN maple_curated.dim_customer d ON d.customer_key = f.customer_key
        WHERE d.customer_key IS NULL
    """)
    if orphans is not None:
        put("OrphanFactRows", int(orphans), "Count")

    # 4. Latest Glue Data Quality score.
    try:
        results = glue.list_data_quality_results(
            Filter={"DataSource": {"GlueTable": {"DatabaseName": DATABASE,
                                                 "TableName": "fact_order_item"}}}
        ).get("Results", [])
        if results:
            latest = sorted(results, key=lambda r: r["StartedOn"], reverse=True)[0]
            detail = glue.get_data_quality_result(ResultId=latest["ResultId"])
            score = detail.get("Score", 0)
            failed = sum(1 for r in detail.get("RuleResults", [])
                         if r.get("Result") != "PASSED")
            put("DataQualityScore", score * 100, "Percent")
            put("DataQualityRulesFailed", failed, "Count")
    except Exception as exc:
        print(f"    (no DQ results yet: {type(exc).__name__})")

    # 5. Pipeline freshness: minutes since the last SUCCEEDED execution.
    try:
        arn = f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:{STATE_MACHINE}"
        execs = sfn.list_executions(stateMachineArn=arn, statusFilter="SUCCEEDED",
                                    maxResults=1).get("executions", [])
        if execs:
            stopped = execs[0]["stopDate"]
            age_min = (datetime.now(timezone.utc) - stopped).total_seconds() / 60
            put("MinutesSinceLastRefresh", age_min, "Count")
            started = execs[0]["startDate"]
            put("PipelineDurationSeconds", (stopped - started).total_seconds(), "Seconds")
    except Exception as exc:
        print(f"    (no pipeline executions yet: {type(exc).__name__})")

    # 6. Athena spend to date, from real query history ($5 per TB scanned).
    try:
        total_bytes = 0
        qids = athena.list_query_executions(WorkGroup=WORKGROUP,
                                            MaxResults=50)["QueryExecutionIds"]
        if qids:
            for chunk_start in range(0, len(qids), 50):
                batch = athena.batch_get_query_execution(
                    QueryExecutionIds=qids[chunk_start:chunk_start + 50])
                for q in batch["QueryExecutions"]:
                    total_bytes += q.get("Statistics", {}).get("DataScannedInBytes", 0)
            put("AthenaBytesScanned", total_bytes, "Bytes")
            put("AthenaEstimatedCostUSD", total_bytes / 1_099_511_627_776 * 5.0, "None")
    except Exception as exc:
        print(f"    (query history unavailable: {type(exc).__name__})")

    # 7. Curated zone size, counted directly.
    try:
        paginator = s3.get_paginator("list_objects_v2")
        objects = size = 0
        for page in paginator.paginate(Bucket="maple-sprint-curated"):
            for obj in page.get("Contents", []):
                objects += 1
                size += obj["Size"]
        put("CuratedObjectCount", objects, "Count")
        put("CuratedBytes", size, "Bytes")
    except Exception as exc:
        print(f"    (bucket scan skipped: {type(exc).__name__})")


# --------------------------------------------------------------------------
# load: make the AWS service metrics real
# --------------------------------------------------------------------------
QUERIES = [
    "SELECT count(*) FROM maple_curated.fact_order_item",
    """SELECT order_status, round(sum(line_total),2) FROM maple_curated.fact_order_item
       GROUP BY order_status""",
    """SELECT p.category, count(*) AS units FROM maple_curated.fact_order_item f
       JOIN maple_curated.dim_product p ON p.product_id = f.product_id
       GROUP BY p.category ORDER BY units DESC LIMIT 10""",
    """SELECT d.province, round(sum(f.line_total),2) AS revenue
       FROM maple_curated.fact_order_item f
       JOIN maple_curated.dim_customer d ON d.customer_key = f.customer_key
       GROUP BY d.province ORDER BY revenue DESC""",
    """SELECT count(*) FROM maple_curated.fact_order_item
       WHERE order_ts >= timestamp '2018-06-01 00:00:00'
         AND order_ts <  timestamp '2018-07-01 00:00:00'""",
]


def cmd_load(with_pipeline: bool = False) -> None:
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] generating load ...")

    # Athena: real queries -> AWS/Athena ProcessedBytes, TotalExecutionTime
    for sql in random.sample(QUERIES, k=3):
        state, scanned, millis = run_query(sql)
        print(f"    athena {state:<10} {scanned:>10,} bytes  {millis:>6} ms")

    # Lambda successes -> Invocations, Duration
    for _ in range(3):
        try:
            lam.invoke(FunctionName=LAMBDA_OK, InvocationType="Event",
                       Payload=json.dumps({"source": "dashboard-load"}))
            print(f"    invoked {LAMBDA_OK}")
        except Exception as exc:
            print(f"    {LAMBDA_OK}: {type(exc).__name__}")

    # Lambda failure -> Errors (this is what the alarm watches)
    try:
        lam.invoke(
            FunctionName=LAMBDA_ERR, InvocationType="Event",
            Payload=json.dumps({"Records": [{"s3": {
                "bucket": {"name": "maple-sprint-curated"},
                "object": {"key": "pii/deliberately-missing.csv"}}}]}),
        )
        print(f"    invoked {LAMBDA_ERR} with a missing key (expected to fail)")
    except Exception as exc:
        print(f"    {LAMBDA_ERR}: {type(exc).__name__}")

    # DynamoDB writes -> ConsumedWriteCapacityUnits, SuccessfulRequestLatency
    try:
        for store in ("TOR-001", "OTT-002", "VAN-003"):
            ddb.update_item(
                TableName=DDB_TABLE,
                Key={"store_id": {"S": store}},
                UpdateExpression="ADD dashboard_pings :one",
                ExpressionAttributeValues={":one": {"N": "1"}},
            )
        print("    wrote 3 DynamoDB items")
    except Exception as exc:
        print(f"    dynamodb: {type(exc).__name__}")

    if with_pipeline:
        try:
            arn = f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:{STATE_MACHINE}"
            sfn.start_execution(stateMachineArn=arn, input=json.dumps({
                "validations": [{"name": "fact_not_empty",
                                 "sql": "SELECT count(*) FROM maple_curated.fact_order_item"}]
            }))
            print("    started a pipeline execution")
        except Exception as exc:
            print(f"    stepfunctions: {type(exc).__name__}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["metrics", "load", "both"])
    p.add_argument("--minutes", type=float, default=0,
                   help="repeat every 60s for this many minutes (0 = once)")
    p.add_argument("--with-pipeline", action="store_true",
                   help="also start a Step Functions execution (runs the Glue job)")
    a = p.parse_args()

    deadline = time.time() + a.minutes * 60
    first = True
    while first or time.time() < deadline:
        first = False
        if a.command in ("load", "both"):
            cmd_load(a.with_pipeline and time.time() > deadline - 60)
        if a.command in ("metrics", "both"):
            cmd_metrics()
        if time.time() >= deadline:
            break
        print("    ... sleeping 60s\n")
        time.sleep(60)

    print("\nDone. Give CloudWatch ~2 minutes, then refresh the dashboard.")
