"""cleansed -> curated: the dimensional model (Glue PySpark, Iceberg).

Builds the star schema Maple & Co.'s analysts actually query:

    dim_customer   SCD TYPE 2  (valid_from / valid_to / is_current)
    dim_product    SCD Type 1  (overwrite - category names aren't history-worthy)
    dim_date       generated calendar
    fact_order_item  grain = one order line, partitioned by month (hidden)

Job parameters (mind the UNDERSCORES - Glue matches keys literally):
    --cleansed_db     maple_cleansed
    --curated_db      maple_curated
    --curated_bucket  maple-sprint-curated

> 📝 EXAM: SCD Type 1 overwrites (no history); Type 2 closes the old row and
  inserts a new version (full history, needs surrogate keys); Type 3 keeps a
  "previous value" column only. Facts join to the version that was current at
  the time - that is why fact rows carry the surrogate key, never the natural
  business key.

> 📝 EXAM: Iceberg HIDDEN PARTITIONING - `PARTITIONED BY (months(order_ts))`
  means queries filtering on order_ts get pruned automatically; there is no
  separate partition column to maintain, and no MSCK/ADD PARTITION dance like
  Hive-style tables. That is the whole point of the open table format.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "cleansed_db", "curated_db", "curated_bucket"]
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

CAT = "glue_catalog"
SRC = args["cleansed_db"]
DST = args["curated_db"]
WAREHOUSE = f"s3://{args['curated_bucket']}/iceberg"

# ---------------------------------------------------------------------------
# 1. dim_customer - SLOWLY CHANGING DIMENSION, TYPE 2
# ---------------------------------------------------------------------------
# Surrogate key = md5(customer_id + valid_from) so every version is unique and
# reproducible. attr_hash lets us detect "did anything I track actually
# change?" in one comparison instead of column-by-column.

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CAT}.{DST}.dim_customer (
        customer_key   STRING,
        customer_id    STRING,
        customer_city  STRING,
        province       STRING,
        postal_prefix  STRING,
        attr_hash      STRING,
        valid_from     TIMESTAMP,
        valid_to       TIMESTAMP,
        is_current     BOOLEAN
    ) USING iceberg
    LOCATION '{WAREHOUSE}/dim_customer'
    TBLPROPERTIES ('format-version' = '2')
""")

source_customers = (
    spark.table(f"{CAT}.{SRC}.customers")
    .select(
        F.col("customer_id"),
        F.col("customer_city").alias("customer_city"),
        F.col("customer_state").alias("province"),
        F.col("customer_zip_code_prefix").alias("postal_prefix"),
    )
    .dropDuplicates(["customer_id"])
    .withColumn(
        "attr_hash",
        F.md5(F.concat_ws("||", F.coalesce("customer_city", F.lit("")),
                          F.coalesce("province", F.lit("")),
                          F.coalesce("postal_prefix", F.lit("")))),
    )
)
source_customers.createOrReplaceTempView("src_customer")

# Step 1 of 2: close versions whose tracked attributes changed.
# (Iceberg MERGE gives us the row-level UPDATE that plain Parquet cannot.)
spark.sql(f"""
    MERGE INTO {CAT}.{DST}.dim_customer t
    USING src_customer s
      ON  t.customer_id = s.customer_id
      AND t.is_current  = true
      AND t.attr_hash  <> s.attr_hash
    WHEN MATCHED THEN UPDATE SET
        t.valid_to   = current_timestamp(),
        t.is_current = false
""")

# Step 2 of 2: insert brand-new customers AND new versions of changed ones.
# Both cases share one predicate: "no current row exists for this id".
spark.sql(f"""
    INSERT INTO {CAT}.{DST}.dim_customer
    SELECT
        md5(concat(s.customer_id, cast(current_timestamp() AS string))) AS customer_key,
        s.customer_id,
        s.customer_city,
        s.province,
        s.postal_prefix,
        s.attr_hash,
        current_timestamp() AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to,
        true AS is_current
    FROM src_customer s
    LEFT ANTI JOIN {CAT}.{DST}.dim_customer t
      ON t.customer_id = s.customer_id AND t.is_current = true
""")

# ---------------------------------------------------------------------------
# 2. dim_product - SCD Type 1 (overwrite; category renames carry no history)
# ---------------------------------------------------------------------------
products = (
    spark.table(f"{CAT}.{SRC}.products").alias("p")
    .join(spark.table(f"{CAT}.{SRC}.category_translation").alias("t"),
          F.col("p.product_category_name") == F.col("t.product_category_name"),
          "left")
    .select(
        F.col("p.product_id").alias("product_id"),
        F.coalesce(F.col("t.product_category_name_english"),
                   F.col("p.product_category_name"),
                   F.lit("unknown")).alias("category"),
        F.col("p.product_weight_g").alias("weight_g"),
        (F.col("p.product_length_cm") * F.col("p.product_height_cm")
         * F.col("p.product_width_cm")).alias("volume_cm3"),
    )
    .dropDuplicates(["product_id"])
)
products.writeTo(f"{CAT}.{DST}.dim_product").using("iceberg").createOrReplace()

# ---------------------------------------------------------------------------
# 3. dim_date - generated calendar covering the order history
# ---------------------------------------------------------------------------
bounds = spark.sql(f"""
    SELECT date(min(order_purchase_timestamp)) AS d0,
           date(max(order_purchase_timestamp)) AS d1
    FROM {CAT}.{SRC}.orders
""").first()

dim_date = (
    spark.sql(f"SELECT sequence(date('{bounds.d0}'), date('{bounds.d1}'), interval 1 day) AS ds")
    .select(F.explode("ds").alias("date_key"))
    .withColumn("year", F.year("date_key"))
    .withColumn("quarter", F.quarter("date_key"))
    .withColumn("month", F.month("date_key"))
    .withColumn("day_of_month", F.dayofmonth("date_key"))
    .withColumn("day_name", F.date_format("date_key", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("date_key").isin([1, 7]))
    .withColumn("year_month", F.date_format("date_key", "yyyy-MM"))
)
dim_date.writeTo(f"{CAT}.{DST}.dim_date").using("iceberg").createOrReplace()

# ---------------------------------------------------------------------------
# 4. fact_order_item - grain: one row per order line
# ---------------------------------------------------------------------------
# Partitioned by months(order_ts): hidden partitioning, so an analyst writing
# WHERE order_ts >= date '2018-01-01' gets pruning for free.

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CAT}.{DST}.fact_order_item (
        order_id            STRING,
        order_item_id       INT,
        customer_key        STRING,
        customer_id         STRING,
        product_id          STRING,
        seller_id           STRING,
        order_status        STRING,
        order_ts            TIMESTAMP,
        delivered_ts        TIMESTAMP,
        estimated_delivery  TIMESTAMP,
        price               DECIMAL(10,2),
        freight_value       DECIMAL(10,2),
        line_total          DECIMAL(10,2),
        delivery_days       INT,
        is_late             BOOLEAN,
        at_risk             BOOLEAN
    ) USING iceberg
    PARTITIONED BY (months(order_ts))
    LOCATION '{WAREHOUSE}/fact_order_item'
    TBLPROPERTIES ('format-version' = '2')
""")

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW src_fact AS
    SELECT
        oi.order_id,
        oi.order_item_id,
        dc.customer_key,
        o.customer_id,
        oi.product_id,
        oi.seller_id,
        o.order_status,
        o.order_purchase_timestamp                AS order_ts,
        o.order_delivered_customer_date           AS delivered_ts,
        o.order_estimated_delivery_date           AS estimated_delivery,
        CAST(oi.price AS DECIMAL(10,2))           AS price,
        CAST(oi.freight_value AS DECIMAL(10,2))   AS freight_value,
        CAST(oi.price + oi.freight_value AS DECIMAL(10,2)) AS line_total,
        datediff(o.order_delivered_customer_date, o.order_purchase_timestamp) AS delivery_days,
        -- delivered after the promise date
        (o.order_delivered_customer_date IS NOT NULL
         AND o.order_delivered_customer_date > o.order_estimated_delivery_date) AS is_late,
        -- operational question: undelivered and the promise date has passed
        (o.order_delivered_customer_date IS NULL
         AND o.order_status NOT IN ('canceled', 'unavailable')
         AND o.order_estimated_delivery_date < current_timestamp()) AS at_risk
    FROM {CAT}.{SRC}.order_items oi
    JOIN {CAT}.{SRC}.orders o
      ON oi.order_id = o.order_id
    LEFT JOIN {CAT}.{DST}.dim_customer dc
      ON dc.customer_id = o.customer_id AND dc.is_current = true
""")

# MERGE (not INSERT) so re-running the job is idempotent and picks up status
# changes flowing in from CDC.
spark.sql(f"""
    MERGE INTO {CAT}.{DST}.fact_order_item t
    USING src_fact s
      ON t.order_id = s.order_id AND t.order_item_id = s.order_item_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# ---------------------------------------------------------------------------
# 5. Report
# ---------------------------------------------------------------------------
for table in ["dim_customer", "dim_product", "dim_date", "fact_order_item"]:
    n = spark.table(f"{CAT}.{DST}.{table}").count()
    print(f"{table:<20} {n:>10,} rows")

versions = spark.sql(f"""
    SELECT count(*) AS c FROM (
        SELECT customer_id FROM {CAT}.{DST}.dim_customer
        GROUP BY customer_id HAVING count(*) > 1)
""").first().c
print(f"customers with >1 SCD2 version: {versions:,}")

job.commit()
