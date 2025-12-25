# DataVelocity

A production-grade data platform implementing medallion architecture (Bronze → Silver → Gold) with real-time streaming capabilities, SCD Type 2 dimensional modeling, and automated data quality validation.

## Architecture Overview

DataVelocity processes food delivery transaction data through a three-tier medallion architecture on Snowflake, supporting both batch and streaming ingestion patterns. The platform handles ~10 entities with full historical tracking, data quality enforcement, and real-time event processing via Kafka.

[//]: # (```)

[//]: # (┌────────────────────────────────────────────────────────────────────────────┐)

[//]: # (│                                                                            │)

[//]: # (│        Batch Layer                          Speed Layer                    │)

[//]: # (│        &#40;CSV Files&#41;                        &#40;Kafka Events&#41;                   │)

[//]: # (│   AWS S3 &#40;External Stage&#41;        Stream Tables → Stream CDC                │)

[//]: # (│            │                                 │                             │)

[//]: # (│            ▼                                 ▼                             │)

[//]: # (│   ┌────────────────────────────────────────────────────────────────────┐   │)

[//]: # (│   │                           BRONZE LAYER                             │   │)

[//]: # (│   │   - Raw data ingestion                                             │   │)

[//]: # (│   │   - Schema-on-read with type conversion                            │   │)

[//]: # (│   │   - Audit columns &#40;ingest_run_id, etc.&#41;                            │   │)

[//]: # (│   └───────────────┬────────────────────────────────────────────────────┘   │)

[//]: # (│                   │                                                        │)

[//]: # (│                   │  Data Quality Validation                               │)

[//]: # (│                   │  &#40;SP_EXECUTE_DATA_QUALITY_VALIDATION&#41;                  │)

[//]: # (│                   │                                                        │)

[//]: # (│   ┌───────────────▼────────────────────────────────────────────────────┐   │)

[//]: # (│   │                           SILVER LAYER                             │   │)

[//]: # (│   │   - Cleansed & validated data                                      │   │)

[//]: # (│   │   - Business key enforcement                                       │   │)

[//]: # (│   │   - MERGE operations &#40;upsert logic&#41;                                │   │)

[//]: # (│   └───────────────┬────────────────────────────────────────────────────┘   │)

[//]: # (│                   │                                                        │)

[//]: # (│                   │  SCD Type 2 Processing                                 │)

[//]: # (│                   │  &#40;Hash-based change detection&#41;                         │)

[//]: # (│                   │                                                        │)

[//]: # (│   ┌───────────────▼────────────────────────────────────────────────────┐   │)

[//]: # (│   │                            GOLD LAYER                              │   │)

[//]: # (│   │   - Dimensional models &#40;SCD Type 2&#41;                                │   │)

[//]: # (│   │   - Fact tables &#40;append-only / status-based&#41;                       │   │)

[//]: # (│   │   - Analytical aggregations &#40;Data Marts&#41;                           │   │)

[//]: # (│   └────────────────────────────────────────────────────────────────────┘   │)

[//]: # (│                                                                            │)

[//]: # (│   Key Metrics:                                                             │)

[//]: # (│   • Sub-15 second streaming latency                                        │)

[//]: # (│   • ~2 second batch processing &#40;5k rows&#41;                                   │)

[//]: # (│   • Zero data loss                                                         │)

[//]: # (│   • Complete audit trail                                                   │)

[//]: # (│                                                                            │)

[//]: # (└────────────────────────────────────────────────────────────────────────────┘)

[//]: # ()
[//]: # (```)

## Data Architecture

```mermaid
flowchart TB

    subgraph Batch_Layer["Batch Layer"]
        S3["AWS S3<br/>(CSV Files / External Stage)"]
    end

    subgraph Speed_Layer["Speed Layer"]
        Kafka["Kafka Events"]
        Streams["Stream Tables<br/>→ Stream CDC"]
        Kafka --> Streams
    end

    subgraph Bronze["BRONZE LAYER"]
        B1["Raw data ingestion"]
        B2["Schema-on-read with type conversion"]
        B3["Audit columns (ingest_run_id, etc.)"]
    end

    subgraph Silver["SILVER LAYER"]
        S1["Cleansed & validated data"]
        S2["Business key enforcement"]
        S3["MERGE operations (Upsert logic)"]
    end

    subgraph Gold["GOLD LAYER"]
        G1["Dimensional models (SCD Type 2)"]
        G2["Fact tables (append-only / status-based)"]
        G3["Analytical aggregations (Data Marts)"]
    end

    DQ["Data Quality Validation<br/>(SP_EXECUTE_DATA_QUALITY_VALIDATION)"]
    SCD["SCD Type 2 Processing<br/>(Hash-based change detection)"]

    S3 --> Bronze
    Streams --> Bronze
    Bronze --> DQ --> Silver
    Silver --> SCD --> Gold
```

## Technical Stack

- **Data Warehouse**: Snowflake (Multi-cluster compute, micro-partitioning)
- **Streaming**: Apache Kafka 3.x (Confluent Platform)
  - Kafka Connect with Snowpipe Streaming Connector
  - Topics: `orders-events`, `order-items-events`, `delivery-events`
- **Orchestration**: Snowflake Tasks (scheduled stored procedures)
- **Data Quality**: Custom validation framework with configurable rules
- **Language**: SQL (Snowflake SQL, Snowflake Scripting)
- **CI/CD**: Docker Compose for local Kafka stack
- **Monitoring**: Snowflake Query History, Task History, custom audit tables

## Project Structure

```
datavelocity/
├── setup/
│   └── setup_swiggy_pipeline.sql          # Database, schemas, file formats, stages
├── entities/
│   ├── customer.sql                        # Customer dimension (SCD2)
│   ├── customer_address.sql                # Address dimension (SCD2)
│   ├── location.sql                        # Location dimension with enrichment
│   ├── restaurant.sql                      # Restaurant dimension (SCD2)
│   ├── menu.sql                            # Menu dimension (SCD2)
│   ├── delivery_agent.sql                  # Delivery agent dimension (SCD2)
│   ├── order.sql                           # Order fact with status tracking
│   ├── order_item.sql                      # Order item fact (append-only)
│   └── delivery.sql                        # Delivery fact with status history
├── orchestration/
│   ├── import_master_procedure.sql         # Master ETL orchestrator
│   └── dq_config_and_validation.sql        # Data quality framework
├── streaming/
│   ├── Kafka_Streaming_Setup.sql           # Snowflake user & permissions
│   ├── Kafka_Tables_Streams_Procedure_Task_Setup.sql  # Stream processing
│   ├── Kafka_Stream_Verification.sql       # Validation queries
│   ├── Kafka_Stream_Monitoring.sql         # Monitoring dashboards
│   ├── docker-compose.yml                  # Kafka stack definition
│   └── producer.py                         # Event generator for testing
├── analytics/
│   ├── dim_date.sql                        # Date dimension (recursive CTE)
│   └── mart.sql                            # Denormalized analytical layer
└── metadata/
    ├── metadata_tables_definition.sql      # Metadata-driven ingestion framework
    └── meta_data_configuration.sql         # Sample metadata configuration
```

## Core Design Decisions

### 1. Medallion Architecture Implementation



**Bronze Layer**:
- Stores raw data with minimal transformation (type casting only)
- Preserves original values in `*_RAW` columns for debugging/replay
- Audit columns: `INGEST_RUN_ID`, `CREATED_AT`, `UPDATED_AT`
- Sequence-based run tracking ensures idempotency
- Explicit transaction management prevents partial loads

**Silver Layer**:
- MERGE-based upserts using business keys
- Data quality validation via `SP_EXECUTE_DATA_QUALITY_VALIDATION`
- Invalid records flagged with `IS_VALID = FALSE` and logged to error tables
- Only valid records promoted to Gold

**Gold Layer**:
- Dimensional models with SCD Type 2 for slowly changing attributes
- Fact tables with status tracking (orders, deliveries)
- Hash-based change detection using `SHA2_HEX` for performance
- Separate status history tables for audit trails

### 2. SCD Type 2 Implementation

```sql
-- Change detection using hash comparison
SHA2_HEX(CONCAT_WS('|', 
    COALESCE(field1, ''),
    COALESCE(field2, ''),
    ...
)) AS CURRENT_HASH

-- Three-step SCD2 process:
-- 1. Create staging table with SCD_ACTION flag
-- 2. UPDATE existing records (set STATUS='INACTIVE', EFF_END_DT=CURRENT_TIMESTAMP)
-- 3. INSERT new versions (STATUS='ACTIVE', EFF_START_DT=CURRENT_TIMESTAMP)
```

**Why this approach?**
- Single MERGE statement causes issues with SCD2 (can't update and insert same key)
- Hash comparison is more performant than column-by-column comparison
- Explicit staging table provides visibility into change types

### 3. Data Quality Framework

Configurable validation rules stored in `COMMON.DQ_CONFIG`:

| Validation Type | Use Case | Example |
|----------------|----------|---------|
| `MANDATORY_CHECK` | Enforce NOT NULL | `EMAIL IS NOT NULL AND TRIM(EMAIL) != ''` |
| `VALUE_CHECK` | Pattern/format validation | `REGEXP_LIKE(EMAIL, '^[A-Za-z0-9._%+-]+@...')` |
| `LOOKUP_CHECK` | Referential integrity | `ORDER_ID IN (SELECT ORDER_ID FROM ORDERS)` |
| `DUPLICATE_ALLOW_ONE_CHECK` | Deduplicate on key | Keep first, mark rest invalid |

Validation results logged to `*_LOAD_ERROR` tables with error messages for reconciliation.

### 4. Streaming Architecture

**Snowpipe Streaming vs. Snowpipe**:
- Uses Snowpipe Streaming for lower latency (<5 seconds vs. ~1 minute)
- Kafka Connect writes directly to Snowflake tables via HTTP API
- No staging files required

**Stream Processing**:
```sql
-- Snowflake Streams for CDC
CREATE STREAM STREAM_ORDERS_CHANGES 
ON TABLE ORDERS_STREAM_SLV 
APPEND_ONLY = TRUE;

-- Scheduled tasks process changes
CREATE TASK TASK_ORDERS_STREAM_TO_GOLD
WAREHOUSE = ADHOC_WH
SCHEDULE = '5 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('STREAM_ORDERS_CHANGES')
AS CALL SP_ORDERS_STREAM_TO_GOLD();
```

**Why Append-Only Streams?**
- Order events are immutable (event sourcing pattern)
- Status changes logged as new events, not updates
- Enables accurate status history tracking

### 5. Orchestration Pattern

Master procedure (`SP_IMPORT_MASTER`) coordinates three-phase ETL:

```sql
-- Phase 1: Stage → Bronze (transactional)
BEGIN TRANSACTION;
  COPY INTO temp_table FROM stage;
  INSERT INTO bronze_table SELECT ... FROM temp_table;
COMMIT;

-- Phase 2: Bronze → Silver (DQ validation + MERGE)
CREATE TEMP TABLE staging AS SELECT * FROM bronze WHERE ingest_run_id = ?;
CALL SP_EXECUTE_DATA_QUALITY_VALIDATION(...);
MERGE INTO silver USING (SELECT * FROM staging WHERE IS_VALID = TRUE) ...;

-- Phase 3: Silver → Gold (SCD2 processing)
CREATE TEMP TABLE staging WITH change_detection;
UPDATE gold SET status='INACTIVE' WHERE key IN (changed_keys);
INSERT INTO gold SELECT * FROM staging WHERE action IN ('INSERT', 'UPDATE');
```

**Error Handling**:
- Explicit transactions with ROLLBACK on failure
- Sequence consumption only on successful commit
- Detailed error logging in `COMMON.INGEST_RUN` table
- `BATCH_ID` tracking for end-to-end lineage

## Entity-Specific Logic

### Orders & Order Items

- **Order**: Fact table with status tracking (`CURRENT_STATUS`, `INITIAL_STATUS`)
- **Order Items**: Append-only (immutable line items)
- Status changes logged to `FACT_ORDER_STATUS_HISTORY` before MERGE
- Handles late-arriving order items via EXISTS check

### Delivery

- Real-time status updates from Kafka
- Status history captured in `FACT_DELIVERY_STATUS_HISTORY`
- MERGE logic prioritizes status changes over other attribute updates

### Location Dimension

Enrichment applied during Bronze → Silver:
```sql
-- State code mapping
CASE WHEN UPPER(STATE) = 'MAHARASHTRA' THEN 'MH' ... END

-- Derived flags
IS_UNION_TERRITORY, CAPITAL_CITY_FLAG, CITY_TIER (TIER1/2/3)
```

### Customer Dimension

- PII columns tagged: `MOBILE`, `EMAIL`, `GENDER`, `DOB`
- Masking policies applied via `COMMON.PII_POLICY_TAG`
- SCD2 tracks changes in preferences, address

## Data Quality & Validation

### Validation Rules Configuration

```sql
INSERT INTO COMMON.DQ_CONFIG VALUES (
    'DQ_CUS_MAN_EMAIL',           -- rule_name
    'BRONZE.CUSTOMER_BRZ',         -- validate_table
    'MANDATORY_CHECK',             -- validation_type
    'EMAIL',                       -- validate_column
    'EMAIL IS NOT NULL AND TRIM(EMAIL) != ''''',  -- validation_query
    'Email address is required'    -- error_message
);
```

### Execution Flow

1. `SP_EXECUTE_DATA_QUALITY_VALIDATION` fetches active rules for table
2. Rules executed in priority order: `MANDATORY_CHECK` → `VALUE_CHECK` → `LOOKUP_CHECK` → `DUPLICATE_ALLOW_ONE_CHECK`
3. Failed records marked with `IS_VALID = FALSE`
4. Errors logged to `*_LOAD_ERROR` with error details
5. Only valid records proceed to Silver layer

### Monitoring Data Quality

```sql
-- Summary of validation results
SELECT 
    INGEST_RUN_ID,
    SOURCE_ROW_COUNT,
    VALID_ROW_COUNT,
    INVALID_ROW_COUNT,
    ROUND(VALID_ROW_COUNT / SOURCE_ROW_COUNT * 100, 2) AS pass_rate
FROM COMMON.INGEST_RUN
ORDER BY EXECUTED_AT DESC;

-- Detailed error analysis
SELECT 
    VALIDATE_COLUMN,
    VALIDATION_TYPE,
    VALIDATION_ERROR_MSG,
    COUNT(*) AS error_count
FROM BRONZE.CUSTOMER_LOAD_ERROR
GROUP BY 1, 2, 3
ORDER BY error_count DESC;
```

## Streaming Pipeline

### Kafka Setup

```bash
# Start Kafka stack (Zookeeper, Kafka, Connect, UI)
docker-compose up -d

# Verify services
curl http://localhost:8083/connectors  # Kafka Connect
curl http://localhost:8080             # Kafka UI
```

### Snowflake Configuration

```sql
-- Create dedicated user for Kafka Connect
CREATE USER KAFKA_CONNECT_USER
RSA_PUBLIC_KEY = '<public_key>'
DEFAULT_ROLE = KAFKA_CONNECTOR_ROLE;

-- Grant permissions
GRANT USAGE ON DATABASE SWIGGY TO ROLE KAFKA_CONNECTOR_ROLE;
GRANT USAGE, CREATE TABLE ON SCHEMA SILVER TO ROLE KAFKA_CONNECTOR_ROLE;
```

### Stream Processing Architecture

**Silver Tables** (landing zone):
```sql
CREATE TABLE SILVER.ORDERS_STREAM_SLV (
    RECORD_METADATA VARIANT,  -- Kafka metadata (offset, partition, timestamp)
    RECORD_CONTENT VARIANT,   -- Event payload
    INGESTED_AT TIMESTAMP_TZ,
    PROCESSED_TO_GOLD BOOLEAN,
    BATCH_ID VARCHAR(36)
);
```

**Views** (for querying):
```sql
CREATE VIEW SILVER.V_ORDERS_STREAM AS
SELECT
    RECORD_METADATA:offset::NUMBER AS KAFKA_OFFSET,
    RECORD_CONTENT:order_id::INTEGER AS ORDER_ID,
    RECORD_CONTENT:status::VARCHAR AS STATUS,
    ...
FROM SILVER.ORDERS_STREAM_SLV;
```

**Snowflake Streams** (CDC):
```sql
CREATE STREAM STREAM_ORDERS_CHANGES 
ON TABLE ORDERS_STREAM_SLV 
APPEND_ONLY = TRUE;
```

**Tasks** (scheduled processing):
```sql
CREATE TASK TASK_ORDERS_STREAM_TO_GOLD
WAREHOUSE = ADHOC_WH
SCHEDULE = '5 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('STREAM_ORDERS_CHANGES')
AS CALL SP_ORDERS_STREAM_TO_GOLD();

ALTER TASK TASK_ORDERS_STREAM_TO_GOLD RESUME;
```

### Event Producer (Testing)

```bash
# Generate test events
python producer.py

# Options:
# 1. Single order lifecycle (order → items → delivery)
# 2. Continuous stream (10 orders, 5s delay)
# 3. Load test (100 orders, 2s delay)
```

### Monitoring Streaming Pipeline

```sql
-- Pipeline health
SELECT * FROM COMMON.VW_STREAMING_PIPELINE_HEALTH;

-- Throughput metrics
SELECT * FROM COMMON.VW_STREAMING_THROUGHPUT;

-- Latency analysis
SELECT * FROM COMMON.VW_STREAMING_LATENCY;

-- Task execution status
SELECT * FROM COMMON.VW_TASK_EXECUTION_STATUS;
```

## Deployment

### Initial Setup

```sql
-- 1. Create infrastructure
@setup/setup_swiggy_pipeline.sql

-- 2. Deploy entities (in dependency order)
@entities/location.sql
@entities/customer.sql
@entities/customer_address.sql
@entities/restaurant.sql
@entities/menu.sql
@entities/delivery_agent.sql
@entities/order.sql
@entities/order_item.sql
@entities/delivery.sql

-- 3. Configure data quality
@orchestration/dq_config_and_validation.sql

-- 4. Deploy orchestration
@orchestration/import_master_procedure.sql

-- 5. (Optional) Setup streaming
@streaming/Kafka_Streaming_Setup.sql
@streaming/Kafka_Tables_Streams_Procedure_Task_Setup.sql
```

### Batch Ingestion

```sql
-- Execute master procedure for entity
CALL COMMON.SP_IMPORT_MASTER('CUSTOMER_PIPELINE', 'customer_01-01-2025.csv');

-- Result:
{
  "batch_start_time": "2025-01-01 10:00:00",
  "stage_to_bronze": { "status": "SUCCESS", "rows_inserted": 1000, ... },
  "bronze_to_silver": { "status": "SUCCESS", "valid_row_count": 950, ... },
  "silver_to_gold": { "status": "SUCCESS", "rows_inserted": 50, ... }
}
```

### Streaming Ingestion

```bash
# 1. Start Kafka stack
docker-compose up -d

# 2. Configure Snowflake connector (manual step via Kafka Connect API)
# See streaming/connector_config.json (not included, create based on docs)

# 3. Generate test events
python streaming/producer.py

# 4. Verify data flow
# Silver: SELECT * FROM SILVER.ORDERS_STREAM_SLV;
# Gold: SELECT * FROM GOLD.FACT_ORDER WHERE BATCH_ID LIKE 'STREAM_%';
```

## Observability

### Audit Tables

```sql
-- Batch execution history
SELECT * FROM COMMON.BATCH 
ORDER BY LAST_BATCH_EXECUTED_AT DESC;

-- Detailed run logs
SELECT * FROM COMMON.INGEST_RUN 
WHERE RUN_STATUS = 'FAILED';

-- Data quality errors
SELECT * FROM BRONZE.CUSTOMER_LOAD_ERROR 
WHERE INGEST_RUN_ID = <run_id>;
```

### Performance Monitoring

```sql
-- Procedure execution times
SELECT 
    PIPELINE_NAME,
    AVG(EXECUTION_DURATION_SEC) AS avg_duration,
    MAX(EXECUTION_DURATION_SEC) AS max_duration
FROM COMMON.INGEST_RUN
GROUP BY PIPELINE_NAME;

-- Task run history
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
))
WHERE NAME LIKE 'TASK_%_STREAM_TO_GOLD'
ORDER BY SCHEDULED_TIME DESC;
```

### Data Lineage

```sql
-- Trace batch through layers
SELECT 
    'BRONZE' AS layer, COUNT(*) AS row_count 
FROM BRONZE.CUSTOMER_BRZ WHERE INGEST_RUN_ID = <run_id>
UNION ALL
SELECT 
    'SILVER', COUNT(*) 
FROM SILVER.CUSTOMER_SLV WHERE BATCH_ID = <batch_id>
UNION ALL
SELECT 
    'GOLD', COUNT(*) 
FROM GOLD.DIM_CUSTOMER WHERE BATCH_ID = <batch_id>;
```

## Performance Considerations

### Clustering Keys

- **Bronze**: Clustered on `INGEST_RUN_ID` (time-series access pattern)
- **Silver**: Clustered on business keys (`MOBILE`, `EMAIL`) or date (`ORDER_DATE`)
- **Gold**: Clustered on surrogate key + `STATUS` + `EFF_START_DT` (SCD2 queries)

### Compute Sizing

- **ADHOC_WH**: X-Small for batch loads (<10k rows/min)
- Auto-suspend after 60 seconds to minimize credits
- Multi-cluster disabled (single-tenant workload)

### Optimization Techniques

1. **Incremental loads**: `INGEST_RUN_ID` partitioning avoids full table scans
2. **MERGE optimization**: Join on indexed business keys
3. **SCD2 hash comparison**: Single hash column vs. 10+ column comparison
4. **Batch processing**: Process 5k-10k rows per transaction (balance between atomicity and performance)

## Limitations & Future Enhancements

### Current Limitations

- No automated schema evolution (DDL changes require manual updates)
- Single-threaded batch processing (one file at a time)
- No cross-entity validation (e.g., order_item.order_id referential integrity)
- Limited monitoring (no alerting on DQ threshold breaches)

### Planned Enhancements

1. **dbt Integration**: Replace stored procedures with dbt models for version control and testing
2. **Airflow DAG**: Replace master procedure with Airflow for better dependency management and retries
3. **Dynamic DQ Rules**: Support SQL-based rule definitions in metadata tables
4. **CDC from Source**: Replace batch loads with Kafka Connect source connectors
5. **Alerting**: Integrate with PagerDuty/Slack for pipeline failures

## Sample Queries

### Analytical Queries

```sql
-- Daily order metrics
SELECT 
    DATE(o.ORDER_DATE) AS order_date,
    COUNT(DISTINCT o.ORDER_ID) AS total_orders,
    SUM(o.TOTAL_AMOUNT) AS revenue,
    COUNT(DISTINCT o.CUSTOMER_ID) AS unique_customers
FROM GOLD.FACT_ORDER o
WHERE o.CURRENT_STATUS = 'DELIVERED'
GROUP BY 1
ORDER BY 1 DESC;

-- Restaurant performance
SELECT 
    r.RESTAURANT_NAME,
    COUNT(DISTINCT o.ORDER_ID) AS order_count,
    AVG(o.TOTAL_AMOUNT) AS avg_order_value,
    SUM(oi.SUBTOTAL) AS total_revenue
FROM GOLD.FACT_ORDER o
JOIN GOLD.DIM_RESTAURANT r ON o.RESTAURANT_ID = r.RESTAURANT_ID
JOIN GOLD.FACT_ORDER_ITEM oi ON o.ORDER_ID = oi.ORDER_ID
WHERE r.STATUS = 'ACTIVE'
GROUP BY 1
ORDER BY total_revenue DESC
LIMIT 10;

-- SCD2 query: Customer history
SELECT 
    CUSTOMER_ID,
    NAME,
    EMAIL,
    STATUS,
    EFF_START_DT,
    EFF_END_DT,
    DATEDIFF(DAY, EFF_START_DT, EFF_END_DT) AS version_duration_days
FROM GOLD.DIM_CUSTOMER
WHERE CUSTOMER_ID = 123
ORDER BY EFF_START_DT DESC;
```

### Operational Queries

```sql
-- Identify stuck deliveries
SELECT 
    d.DELIVERY_ID,
    d.ORDER_ID,
    d.CURRENT_STATUS,
    d.STATUS_UPDATED_AT,
    DATEDIFF(MINUTE, d.STATUS_UPDATED_AT, CURRENT_TIMESTAMP()) AS stuck_minutes
FROM GOLD.FACT_DELIVERY d
WHERE d.CURRENT_STATUS IN ('ASSIGNED', 'PICKED_UP', 'IN_TRANSIT')
  AND DATEDIFF(MINUTE, d.STATUS_UPDATED_AT, CURRENT_TIMESTAMP()) > 60;

-- Data quality report card
SELECT 
    PIPELINE_NAME,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN RUN_STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS success_runs,
    ROUND(AVG(VALID_ROW_COUNT / NULLIF(SOURCE_ROW_COUNT, 0) * 100), 2) AS avg_quality_score
FROM COMMON.INGEST_RUN
WHERE EXECUTED_AT >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY PIPELINE_NAME;
```

## License

MIT License - See LICENSE file for details

---

**Note**: This project demonstrates production-ready patterns. File paths and configurations assume a standard Snowflake environment. Adjust warehouse sizes, clustering keys, and scheduling based on actual data volumes.
