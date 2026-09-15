# Maple & Co. - CDC Lakehouse with Governance and Quality Gates on AWS

A working AWS data platform for a fictional Canadian retailer: <b>change data capture from an operational Postgres, landed as Apache Iceberg tables, modelled into a star schema, guarded by an automated data-quality gate, orchestrated by Step Functions, and governed by Lake Formation column- and row-level security.</b>

## The pipeline
```mermaid
flowchart LR
    subgraph src["Operational source"]
        RDS[("RDS PostgreSQL 16<br/>maple-oltp-db<br/>8 tables · ~330k rows")]
    end

    subgraph ingest["Ingestion — CDC"]
        DMS["AWS DMS<br/>full load + ongoing replication<br/>Parquet · Op + tx timestamp"]
    end

    subgraph lake["S3 lakehouse — Glue Data Catalog"]
        RAW[("raw<br/>maple-sprint-raw/dms/")]
        CLEAN[("cleansed — Iceberg<br/>maple_cleansed<br/>8 tables")]
        CUR[("curated — Iceberg star schema<br/>maple_curated<br/>fact_order_item 112,650 rows")]
    end

    subgraph govern["Governance"]
        LF["Lake Formation<br/>column + row filters"]
        PII[("customer_pii<br/>synthetic")]
    end

    subgraph orch["Orchestration & quality"]
        SFN["Step Functions<br/>maple-daily-pipeline"]
        DQ["Glue Data Quality<br/>13 DQDL rules"]
        SNS["SNS alerts"]
    end

    ANALYST(["analyst role<br/>3 of 7 columns · ON only"])
    FRAUD(["fraud role<br/>all columns · all provinces"])

    RDS -->|logical replication| DMS --> RAW
    RAW -->|"Glue PySpark · Iceberg MERGE<br/>insert / update / delete"| CLEAN
    CLEAN -->|"Glue PySpark · SCD2 + facts"| CUR
    CUR --> LF
    PII --> LF
    LF --> ANALYST
    LF --> FRAUD
    CUR --> ATHENA["Athena<br/>workgroup maple-wg"]

    SFN -->|1 . run| CUR
    SFN -->|2 . gate| DQ
    DQ -->|score < 1.0 → stop| SNS
    SFN -->|3 . validate| ATHENA
    SFN --> SNS
```

## What it does, and what proves it

| Capability | Evidence |
|---|---|
| **CDC, not batch snapshots** | DMS runs full load then ongoing replication; every row carries `Op` (I/U/D) and a transaction timestamp, applied with an Iceberg `MERGE` that handles inserts, updates **and deletes** |
| **Open table format** | Iceberg on S3 with hidden monthly partitioning (`months(order_ts)`), row-level DML from Athena, snapshot history |
| **Dimensional model** | `fact_order_item` (112,650 rows), `dim_customer` with **SCD Type 2** validity ranges and surrogate keys, `dim_product`, `dim_date` |
| **The pipeline refuses to publish bad data** | Glue Data Quality ruleset (13 rules) evaluated as a **blocking gate** in Step Functions; a deliberately broken rule produced a red execution, an SNS alert, and **skipped downstream validation** |
| **Real orchestration** | Step Functions: `glue:startJobRun.sync` with retries, DQ polling loop, a Choice gate on quality score, and a Map running the **async Athena `StartQueryExecution` + Wait-state polling** pattern — the answer to jobs that outlive Lambda's 15-minute ceiling |
| **Governance that provably restricts** | Same query, two roles: analyst returns **3 of 7 columns and Ontario rows only**; fraud returns all 7 and every province; an explicit `SELECT email` as analyst fails with *Insufficient Lake Formation permission(s)* |
| **Audit** | CloudTrail **data** events on the PII prefix (object-level reads, not just control-plane changes) and Macie classification |

## Repository

| Path | Contents |
|---|---|
| `src/transformation/` | Glue PySpark: `raw_to_cleansed_iceberg.py` (CDC MERGE), `cleansed_to_curated.py` (SCD2 + facts), `format_benchmark.py` (Python shell, 1/16 DPU) |
| `src/ingestion/` | `seed_local_oltp.py` — seeds and exercises the OLTP source with psycopg2 |
| `src/orchestration/` | `maple_daily_pipeline.asl.json` — the state machine |
| `src/quality/` | `maple_fact_quality.dqdl` — the 13-rule ruleset |
| `src/security/` | Redaction handlers (Object Lambda + Function URL variants) |
| `generators/` | `generate_pii.py` (synthetic PII), `profile_data.py` (source profiling) |
| `scripts/` | `lakeformation_setup.py`, `assume_and_query.py`, `dq_ruleset.py`, `dashboard_data.py`, `aws_inventory.py`, `cost_report.py`, `check_replication_slots.py` |
| `sql/` | Athena analytics, partition lab, Lake Formation lab, Redshift DDL |
| `docs/sprint/` | Day-by-day build runbooks — every console setting and IAM policy, so the build is reproducible by hand |
| `docs/adr/`, `docs/spikes/` | Decisions, and services designed but not deployed |

## Security posture

- No credentials in code, job parameters, or config files — **Secrets Manager**
  with IAM access roles throughout (including the DMS regional-principal trust).
- The private DMS instance reaches S3 through a **free gateway endpoint** and
  Secrets Manager through an interface endpoint; **no NAT gateway exists** in
  this account, by design.
- Lake Formation-governed roles hold **no `s3:GetObject` on the data** — S3
  credentials are vended by LF at query time, so column filters cannot be
  bypassed by reading the raw files.
- All PII is **synthetic** (seeded word lists; SIN-shaped values use the `000`
  test prefix). A portfolio project should never handle real personal data to
  prove it can protect personal data.










