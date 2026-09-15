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







