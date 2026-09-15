"""raw -> cleansed: apply DMS full-load + CDC Parquet files to Iceberg tables.

One idempotent job handles both the initial load and every incremental run:
  1. Glue JOB BOOKMARKS hand us only the raw files we haven't processed yet.
  2. Per primary key we keep the LATEST change (by dms_ts, the transaction
     timestamp DMS stamped on every row).
  3. Iceberg MERGE applies it: Op='D' deletes, matches update, rest insert.

> 📝 EXAM: this is the canonical answer to "daily/continuous CDC against a
  data lake with updates and deletes" — an open table format (Iceberg/Hudi/
  Delta) with MERGE. Plain Parquet on S3 has no row-level update/delete;
  rewriting whole partitions is the trap answer.

> 📝 EXAM: job bookmarks track processed files/offsets per transformation_ctx.
  Reset with the job's bookmark controls if you need a full replay — simply
  rerunning the job reprocesses NOTHING (bookmark says "already seen").
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "raw_bucket", "cleansed_db"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

RAW = args["raw_bucket"]
DB = args["cleansed_db"]  # e.g. maple_cleansed
CATALOG = "glue_catalog"

# table -> primary key columns (must match the Postgres PKs / DMS metadata)
PK = {
    "customers": ["customer_id"],
    "sellers": ["seller_id"],
    "products": ["product_id"],
    "category_translation": ["product_category_name"],
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_id", "order_id"],
}

# Business re-skin: Maple & Co. operates in Canada — map source regions to
# provinces in the cleansed layer (documented in docs/data-dictionary.md).
STATE_TO_PROVINCE = {
    "SP": "ON", "RJ": "QC", "MG": "BC", "RS": "AB", "PR": "MB", "SC": "SK",
    "BA": "NS", "DF": "NB", "ES": "NL", "GO": "PE", "PE": "YT", "CE": "NT",
}


def map_province(df, col):
    if col not in df.columns:
        return df
    mapping = F.create_map([F.lit(x) for kv in STATE_TO_PROVINCE.items() for x in kv])
    return df.withColumn(col, F.coalesce(mapping[F.col(col)], F.lit("ON")))


def table_exists(table: str) -> bool:
    return spark.catalog.tableExists(f"{CATALOG}.{DB}.{table}")


processed = {}
for table, pk_cols in PK.items():
    path = f"s3://{RAW}/dms/maple/{table}/"

    # Bookmark-aware read: only new files since the last successful run.
    dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [path], "recurse": True},
        format="parquet",
        transformation_ctx=f"src_{table}",  # bookmark key
    )
    df = dyf.toDF()
    if df.rdd.isEmpty():
        processed[table] = 0
        continue

    # Normalise: empty strings -> NULL, trim province mapping where relevant.
    for c, t in df.dtypes:
        if t == "string" and c not in ("Op",):
            df = df.withColumn(c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.col(c)))
    df = map_province(df, "customer_state")
    df = map_province(df, "seller_state")

    # Latest change per PK wins (a row can be inserted+updated within a batch).
    w = Window.partitionBy(*pk_cols).orderBy(F.col("dms_ts").desc())
    latest = (
        df.withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn")
        .withColumnRenamed("dms_ts", "_cdc_ts")
    )

    if not table_exists(table):
        # First run: full-load rows (Op='I') create the table.
        latest.where(F.col("Op") != "D").drop("Op").writeTo(
            f"{CATALOG}.{DB}.{table}"
        ).using("iceberg").tableProperty("format-version", "2").createOrReplace()
        processed[table] = latest.count()
    else:
        latest.createOrReplaceTempView(f"stage_{table}")
        on_clause = " AND ".join(f"t.{c} = s.{c}" for c in pk_cols)
        # Explicit column lists: the staging view carries Op (needed for the
        # delete branch) which the target table deliberately does not have.
        data_cols = [c for c in latest.columns if c != "Op"]
        set_clause = ", ".join(f"t.{c} = s.{c}" for c in data_cols)
        insert_cols = ", ".join(data_cols)
        insert_vals = ", ".join(f"s.{c}" for c in data_cols)
        spark.sql(f"""
            MERGE INTO {CATALOG}.{DB}.{table} t
            USING stage_{table} s
            ON {on_clause}
            WHEN MATCHED AND s.Op = 'D' THEN DELETE
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED AND s.Op != 'D' THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """)
        processed[table] = latest.count()

print(f"Applied changes per table: {processed}")
job.commit()  # <- commits the bookmarks; without this every run reprocesses everything
