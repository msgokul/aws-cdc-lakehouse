-- ============================================================================
-- Day 5 - THE PARTITION LAB. Four ways to tell Athena where your data lives,
-- and the difference between fixing PLANNING time and fixing EXECUTION time.
--
-- Source data: the streaming objects Day 3 produced at
--     s3://maple-sprint-raw/streaming/<event_type>/<yyyy>/<MM>/<dd>/<HH>/*.json.gz
-- Note the path shape: it is NOT Hive-style (no key=value segments). That
-- detail decides which of the four techniques below even work.
--
-- Run in Athena, workgroup maple-wg, database maple_raw. For every query,
-- record BOTH numbers the console shows: "Time in queue + planning" and
-- "Run time", plus "Data scanned". Put them in docs/cost-analysis.md.
--
-- 📝 EXAM, the sentence to memorize:
--   PLANNING time is dominated by how many partitions Athena must enumerate
--   from the Glue Data Catalog  -> fixed by partition PROJECTION or partition
--   INDEXES.
--   EXECUTION time is dominated by how many bytes it must read -> fixed by
--   columnar format, compression, file sizing, and predicate pushdown.
--   A slow query with tiny data scanned is a PLANNING problem. A fast-planning
--   query scanning terabytes is an EXECUTION problem. Different fixes.
-- ============================================================================


-- ############################################################################
-- BASELINE - no partitions at all
-- ############################################################################
CREATE EXTERNAL TABLE maple_raw.events_flat (
  event_id    string,
  event_type  string,
  store_id    string,
  customer_id string,
  product_id  string,
  quantity    int,
  amount      double,
  event_time  string,
  ingest_time string,
  late        boolean
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://maple-sprint-raw/streaming/';

-- Every query reads EVERY object under the prefix, regardless of filters:
SELECT count(*), round(sum(amount), 2)
FROM maple_raw.events_flat
WHERE event_type = 'pos_sale';
-- ^ record data scanned. This is the number the next sections beat.


-- ############################################################################
-- TECHNIQUE 1 - ALTER TABLE ADD PARTITION (targeted, no scan of S3)
-- ############################################################################
CREATE EXTERNAL TABLE maple_raw.events_manual (
  event_id    string,
  store_id    string,
  customer_id string,
  product_id  string,
  quantity    int,
  amount      double,
  event_time  string,
  late        boolean
)
PARTITIONED BY (event_type string, y string, m string, d string, h string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://maple-sprint-raw/streaming/';

-- Nothing is registered yet, so this returns ZERO ROWS - and no error:
SELECT count(*) FROM maple_raw.events_manual;
-- 📝 EXAM: "query returns 0 rows but the files are definitely there" is
-- almost always (a) partitions never registered, or (b) a LOCATION pointing
-- at the wrong prefix. An *error* means schema/SerDe. A zero-row *success*
-- means metadata. Learn to tell those two apart from the symptom.

-- Register exactly the partitions you need. Substitute the real dates you
-- produced on Day 3 (look at the S3 console for the actual paths).
ALTER TABLE maple_raw.events_manual ADD IF NOT EXISTS
  PARTITION (event_type='pos_sale', y='2026', m='08', d='31', h='18')
    LOCATION 's3://maple-sprint-raw/streaming/pos_sale/2026/08/31/18/'
  PARTITION (event_type='pos_sale', y='2026', m='08', d='31', h='19')
    LOCATION 's3://maple-sprint-raw/streaming/pos_sale/2026/08/31/19/'
  PARTITION (event_type='page_view', y='2026', m='08', d='31', h='18')
    LOCATION 's3://maple-sprint-raw/streaming/page_view/2026/08/31/18/';

SHOW PARTITIONS maple_raw.events_manual;

-- Now the filter prunes to one prefix:
SELECT count(*), round(sum(amount), 2)
FROM maple_raw.events_manual
WHERE event_type = 'pos_sale' AND y='2026' AND m='08' AND d='31' AND h='18';
-- ^ compare data scanned to the baseline.

-- 📝 EXAM: ADD PARTITION is TARGETED - it registers named partitions with
-- explicit LOCATIONs and does not list the bucket. It is the right answer for
-- "a new partition arrives hourly, register it as part of the pipeline"
-- (call it from the ETL job or Step Functions, exactly like our Day 5
-- workflow could).


-- ############################################################################
-- TECHNIQUE 2 - MSCK REPAIR TABLE (and why it does nothing here)
-- ############################################################################
MSCK REPAIR TABLE maple_raw.events_manual;
SHOW PARTITIONS maple_raw.events_manual;   -- unchanged: still only what we added

-- 📝 EXAM - two separate facts, both tested:
--   (1) MSCK REPAIR only discovers HIVE-STYLE paths, i.e. .../key=value/...
--       Our producer wrote .../pos_sale/2026/08/31/18/ with no key= prefix,
--       so MSCK finds nothing. Had we written
--       .../event_type=pos_sale/y=2026/m=08/d=31/h=18/ it would work.
--   (2) Even when it does work, MSCK LISTS THE ENTIRE PREFIX every time -
--       O(objects). On a bucket with hundreds of thousands of objects it is
--       slow and expensive, and it is the classic wrong answer when the
--       question says "a single new partition per hour" (ADD PARTITION) or
--       "thousands of partitions and we want no maintenance at all"
--       (projection).


-- ############################################################################
-- TECHNIQUE 3 - PARTITION PROJECTION (zero metadata, zero maintenance)
-- ############################################################################
-- Athena CALCULATES partition values from the table properties instead of
-- reading them from the catalog. No ADD PARTITION, no MSCK, no crawler,
-- and planning time stops growing with partition count.

CREATE EXTERNAL TABLE maple_raw.events_projected (
  event_id    string,
  store_id    string,
  customer_id string,
  product_id  string,
  quantity    int,
  amount      double,
  event_time  string,
  late        boolean
)
PARTITIONED BY (event_type string, y string, m string, d string, h string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://maple-sprint-raw/streaming/'
TBLPROPERTIES (
  'projection.enabled' = 'true',

  'projection.event_type.type'   = 'enum',
  'projection.event_type.values' = 'pos_sale,page_view,inventory_move',

  'projection.y.type'   = 'integer',
  'projection.y.range'  = '2026,2027',
  'projection.y.digits' = '4',

  'projection.m.type'   = 'integer',
  'projection.m.range'  = '1,12',
  'projection.m.digits' = '2',

  'projection.d.type'   = 'integer',
  'projection.d.range'  = '1,31',
  'projection.d.digits' = '2',

  'projection.h.type'   = 'integer',
  'projection.h.range'  = '0,23',
  'projection.h.digits' = '2',

  -- Required because the paths are not key=value: tell Athena how to build
  -- the S3 path from the projected values.
  'storage.location.template' =
    's3://maple-sprint-raw/streaming/${event_type}/${y}/${m}/${d}/${h}'
);

-- No partition registration was needed. This just works:
SELECT count(*), round(sum(amount), 2)
FROM maple_raw.events_projected
WHERE event_type = 'pos_sale' AND y='2026' AND m='08' AND d='31' AND h='18';

SELECT event_type, count(*) AS events
FROM maple_raw.events_projected
WHERE y='2026' AND m='08' AND d='31'
GROUP BY event_type;

-- 📝 EXAM: projection shines when partitions are DENSE and PREDICTABLE
-- (date/hour ranges, known enums). Its failure mode is the opposite case:
-- if the projected range covers partitions that do not exist, Athena still
-- probes those prefixes - harmless but wasteful - and if data lands outside
-- the declared range it is INVISIBLE. Also note projection is a property of
-- the Athena table, not of the Glue crawler: crawlers become unnecessary.


-- ############################################################################
-- TECHNIQUE 4 - PARTITION INDEXES (for catalog tables with MANY partitions)
-- ############################################################################
-- Not SQL - created on the Glue table (console: Glue -> Tables -> table ->
-- Partitions and indexes -> Add index, or the CreatePartitionIndex API,
-- see scripts/create_partitions_api.py which also shows index creation).
--
-- 📝 EXAM: with tens of thousands of partitions, the GetPartitions call the
-- planner makes gets slow because it filters partitions server-side without
-- an index. A partition INDEX (on, say, event_type + y + m) makes that lookup
-- indexed instead of scanned -> planning time collapses. Rules to remember:
--   - indexes are created on the GLUE TABLE, up to 3 per table
--   - they help ONLY partition-pruning lookups, never the data scan itself
--   - the index columns must be a PREFIX of the partition key list to be used
--   - projection vs index: projection = no catalog partitions at all (best
--     for predictable time-series); index = keep catalog partitions but make
--     lookups fast (best when partitions are irregular or externally managed)


-- ############################################################################
-- EXECUTION-side fix, for contrast: format and file size
-- ############################################################################
-- Planning is now fast. Execution is still reading gzipped JSON, one small
-- object per minute - the small-file problem. CTAS converts to Parquet with
-- sane file sizes:

CREATE TABLE maple_raw.events_parquet
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location = 's3://maple-sprint-raw/optimized/events/',
  partitioned_by = ARRAY['event_type']
) AS
SELECT event_id, store_id, customer_id, product_id, quantity, amount,
       from_iso8601_timestamp(event_time) AS event_ts, late, event_type
FROM maple_raw.events_projected
WHERE y='2026';

SELECT count(*), round(sum(amount), 2)
FROM maple_raw.events_parquet
WHERE event_type = 'pos_sale';
-- Compare data scanned AND run time against the projected JSON version.
-- 📝 EXAM: CTAS (and INSERT INTO) are how you materialize an optimized copy
-- inside Athena itself - no Glue job required. Bucketing is also available
-- via bucketed_by/bucket_count for high-cardinality join keys.


-- ############################################################################
-- SCORECARD - fill this in, it goes straight into docs/cost-analysis.md
-- ############################################################################
--  technique              partitions in catalog   planning   run time   scanned
--  ---------------------  ---------------------   --------   --------   -------
--  events_flat (none)                         0
--  events_manual (ADD)                        3
--  events_projected                           0
--  events_parquet (CTAS)                      3
