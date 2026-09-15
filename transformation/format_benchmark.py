"""Glue PYTHON SHELL job — file format & compression benchmark.

Deliberately a Python shell job, not PySpark:
> 📝 EXAM: Python shell jobs bill from 0.0625 DPU (1/16th DPU-hour minimum);
  Spark jobs start at 2 DPU (two G.1X workers). For a single-node, sub-GB
  pandas task like this, Python shell is ~30x cheaper. "Small dataset +
  simple transform + cost-sensitive" -> Python shell; distributed shuffle
  or large joins -> PySpark.

Writes the joined orders+items dataset to s3://<raw>/bench/ in five variants
(csv, csv.gz, jsonl.gz, parquet+snappy, avro+deflate), then prints a size
table. Athena scan-cost comparison queries live in sql/athena_benchmark.sql.

Job args: --raw_bucket <bucket>
"""

import gzip
import io
import json
import sys

import boto3
import pandas as pd
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["raw_bucket"])
RAW = args["raw_bucket"]
BENCH = "bench"

s3 = boto3.client("s3")


def read_seed_csv(name: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=RAW, Key=f"seed/olist/{name}")
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


print("Loading and joining orders + items ...")
orders = read_seed_csv("olist_orders_dataset.csv")
items = read_seed_csv("olist_order_items_dataset.csv")
df = orders.merge(items, on="order_id", how="inner")
for col in df.columns:
    if "timestamp" in col or "date" in col:
        df[col] = pd.to_datetime(df[col], errors="coerce")
print(f"Benchmark dataset: {len(df):,} rows x {len(df.columns)} columns")

results = {}


def put(key: str, body: bytes) -> None:
    s3.put_object(Bucket=RAW, Key=key, Body=body)
    results[key] = len(body)


# 1. CSV (uncompressed) — the naive baseline
put(f"{BENCH}/csv/orders_items.csv", df.to_csv(index=False).encode())

# 2. CSV gzip — compression without changing the row layout
put(f"{BENCH}/csv_gz/orders_items.csv.gz", gzip.compress(df.to_csv(index=False).encode()))

# 3. JSON lines gzip — what naive API/Firehose pipelines often land
put(f"{BENCH}/json_gz/orders_items.jsonl.gz",
    gzip.compress(df.to_json(orient="records", lines=True, date_format="iso").encode()))

# 4. Parquet + snappy — columnar; the analytics answer
buf = io.BytesIO()
df.to_parquet(buf, index=False, compression="snappy")
put(f"{BENCH}/parquet/orders_items.snappy.parquet", buf.getvalue())

# 5. Avro + deflate — row-oriented binary with embedded schema
#    (row format: great for streaming/schema evolution, NOT for column scans)
import fastavro  # provided via --additional-python-modules

avro_df = df.copy()
for col in avro_df.columns:
    if str(avro_df[col].dtype).startswith("datetime"):
        avro_df[col] = avro_df[col].astype(str).where(avro_df[col].notna(), None)
avro_records = avro_df.where(pd.notna(avro_df), None).to_dict("records")


def avro_type(dtype) -> list:
    if "int" in str(dtype):
        return ["null", "long"]
    if "float" in str(dtype):
        return ["null", "double"]
    return ["null", "string"]


schema = {
    "type": "record",
    "name": "OrdersItems",
    "fields": [{"name": c, "type": avro_type(t)} for c, t in avro_df.dtypes.items()],
}
buf = io.BytesIO()
fastavro.writer(buf, fastavro.parse_schema(schema), avro_records, codec="deflate")
put(f"{BENCH}/avro/orders_items.avro", buf.getvalue())

# ORC intentionally not generated (pandas/pyarrow write support is poor on
# Python shell). Documented result: columnar like Parquet, similar economics;
# Parquet has the broader AWS-native support. See docs/adr + day-2 notes.

# --- Report -----------------------------------------------------------------
baseline = results[f"{BENCH}/csv/orders_items.csv"]
print(f"\n{'format':<12}{'size':>12}{'vs csv':>9}")
print("-" * 33)
report = {}
for key, size in results.items():
    fmt = key.split("/")[1]
    report[fmt] = size
    print(f"{fmt:<12}{size:>12,}{size / baseline:>8.1%}")

s3.put_object(
    Bucket=RAW,
    Key=f"{BENCH}/results.json",
    Body=json.dumps({"rows": len(df), "bytes_by_format": report}).encode(),
)
print(f"\nWritten to s3://{RAW}/{BENCH}/ — now run sql/athena_benchmark.sql "
      "and compare data_scanned per query. Numbers go in docs/cost-analysis.md.")
