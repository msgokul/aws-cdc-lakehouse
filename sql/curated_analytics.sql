-- ============================================================================
-- Day 4 - the curated layer answering Maple & Co.'s actual business questions.
-- Run in Athena, workgroup maple-wg, database maple_curated.
--
-- These four queries ARE the product. Everything upstream exists to make them
-- fast, correct and cheap. Record each query's runtime + data scanned; the
-- numbers go in docs/cost-analysis.md and the README's "key results".
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Q1. ANALYTICAL: customer lifetime value by acquisition cohort and province
-- ---------------------------------------------------------------------------
WITH first_order AS (
  SELECT customer_id,
         min(order_ts) AS acquired_ts
  FROM maple_curated.fact_order_item
  GROUP BY customer_id
),
cohort AS (
  SELECT f.customer_id,
         date_format(fo.acquired_ts, '%Y-%m') AS cohort_month,
         d.province,
         sum(f.line_total) AS lifetime_value,
         count(DISTINCT f.order_id) AS orders
  FROM maple_curated.fact_order_item f
  JOIN first_order fo ON fo.customer_id = f.customer_id
  JOIN maple_curated.dim_customer d
    ON d.customer_id = f.customer_id AND d.is_current = true
  GROUP BY f.customer_id, date_format(fo.acquired_ts, '%Y-%m'), d.province
)
SELECT cohort_month,
       province,
       count(*)                          AS customers,
       round(avg(lifetime_value), 2)     AS avg_clv,
       round(avg(orders), 2)             AS avg_orders,
       round(sum(lifetime_value), 2)     AS cohort_revenue
FROM cohort
GROUP BY cohort_month, province
HAVING count(*) >= 20
ORDER BY cohort_month DESC, avg_clv DESC
LIMIT 50;

-- ---------------------------------------------------------------------------
-- Q2. OPERATIONAL: which orders are at risk of late delivery right now?
-- ---------------------------------------------------------------------------
SELECT f.order_id,
       d.province,
       f.order_status,
       f.order_ts,
       f.estimated_delivery,
       date_diff('day', f.estimated_delivery, current_timestamp) AS days_overdue,
       round(sum(f.line_total), 2) AS order_value
FROM maple_curated.fact_order_item f
JOIN maple_curated.dim_customer d
  ON d.customer_key = f.customer_key
WHERE f.at_risk
GROUP BY f.order_id, d.province, f.order_status, f.order_ts, f.estimated_delivery
ORDER BY days_overdue DESC, order_value DESC
LIMIT 100;

-- Late-delivery rate by province (the version that goes on a dashboard):
SELECT d.province,
       count(*)                                              AS delivered_lines,
       sum(CASE WHEN f.is_late THEN 1 ELSE 0 END)            AS late_lines,
       round(100.0 * sum(CASE WHEN f.is_late THEN 1 ELSE 0 END) / count(*), 2) AS late_pct,
       round(avg(f.delivery_days), 1)                        AS avg_delivery_days
FROM maple_curated.fact_order_item f
JOIN maple_curated.dim_customer d ON d.customer_key = f.customer_key
WHERE f.delivered_ts IS NOT NULL
GROUP BY d.province
ORDER BY late_pct DESC;

-- ---------------------------------------------------------------------------
-- Q3. NEAR-REAL-TIME-ish: what is selling, by category and month
-- (the streaming answer lives in DynamoDB; this is the historical view)
-- ---------------------------------------------------------------------------
SELECT dd.year_month,
       p.category,
       count(*)                       AS units,
       round(sum(f.line_total), 2)    AS revenue
FROM maple_curated.fact_order_item f
JOIN maple_curated.dim_product p ON p.product_id = f.product_id
JOIN maple_curated.dim_date  dd  ON dd.date_key = date(f.order_ts)
WHERE f.order_ts >= timestamp '2018-01-01 00:00:00'   -- hidden partition pruning
GROUP BY dd.year_month, p.category
ORDER BY dd.year_month DESC, revenue DESC
LIMIT 100;

-- Partition-pruning proof: run these two and compare "Data scanned".
SELECT count(*), round(sum(line_total),2) FROM maple_curated.fact_order_item;
SELECT count(*), round(sum(line_total),2) FROM maple_curated.fact_order_item
WHERE order_ts >= timestamp '2018-06-01 00:00:00'
  AND order_ts <  timestamp '2018-07-01 00:00:00';
-- 📝 EXAM: the second scans ~1/24th of the data with no partition column in
-- the WHERE clause - Iceberg hidden partitioning (months(order_ts)) resolves
-- the predicate to partitions. Hive-style tables need the partition column
-- named explicitly, plus MSCK/ADD PARTITION maintenance.

-- ---------------------------------------------------------------------------
-- Q4. SCD TYPE 2 DEMONSTRATION - prove history is preserved
-- ---------------------------------------------------------------------------
-- Step 1: pick a customer and note their current province.
SELECT customer_key, customer_id, customer_city, province, valid_from, valid_to, is_current
FROM maple_curated.dim_customer
WHERE customer_id = (SELECT min(customer_id) FROM maple_curated.dim_customer);

-- Step 2: simulate a source change. Athena can UPDATE Iceberg tables directly
-- (engine v3) - no Spark, no DMS needed for this demo:
UPDATE maple_cleansed.customers
SET customer_city = 'Mississauga', customer_state = 'ON'
WHERE customer_id IN (
  SELECT customer_id FROM maple_cleansed.customers ORDER BY customer_id LIMIT 5
);
-- 📝 EXAM: row-level UPDATE/DELETE on S3 data is exactly what an open table
-- format buys you. On plain Parquet + Hive tables this statement is impossible.

-- Step 3: re-run the Glue job maple-cleansed-to-curated, then:
SELECT customer_id, customer_city, province, valid_from, valid_to, is_current
FROM maple_curated.dim_customer
WHERE customer_id IN (
  SELECT customer_id FROM maple_curated.dim_customer
  GROUP BY customer_id HAVING count(*) > 1
)
ORDER BY customer_id, valid_from;
-- Expect TWO rows per changed customer: the old one closed (valid_to set,
-- is_current = false) and the new one open. That is Type 2.

-- Step 4: the reason it matters - historical facts still join to the version
-- that was current when the order happened:
SELECT dc.province, count(*) AS lines, round(sum(f.line_total), 2) AS revenue
FROM maple_curated.fact_order_item f
JOIN maple_curated.dim_customer dc ON dc.customer_key = f.customer_key
GROUP BY dc.province ORDER BY revenue DESC;

-- ---------------------------------------------------------------------------
-- Q5. Iceberg housekeeping (📝 EXAM: table maintenance is a real job)
-- ---------------------------------------------------------------------------
-- Snapshot history - every write creates one:
SELECT * FROM "maple_curated"."fact_order_item$snapshots" ORDER BY committed_at DESC LIMIT 10;

-- File count/size - many small files = slow scans (the "small file problem"):
SELECT count(*) AS files,
       round(sum(file_size_in_bytes) / 1024.0 / 1024.0, 1) AS total_mb,
       round(avg(file_size_in_bytes) / 1024.0 / 1024.0, 2) AS avg_file_mb
FROM "maple_curated"."fact_order_item$files";

-- Compaction + snapshot expiry (run after several MERGEs):
OPTIMIZE maple_curated.fact_order_item REWRITE DATA USING BIN_PACK;
VACUUM maple_curated.fact_order_item;
-- 📝 EXAM: OPTIMIZE bin-packs small files into large ones (fixes execution
-- time); VACUUM expires old snapshots and removes orphaned files (fixes
-- storage cost, and destroys time-travel history older than the retention
-- window - that trade-off is the exam point).
