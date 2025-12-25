**I built a Lambda Architecture on Snowflake that unifies batch and streaming without duplicating transformation logic.**

Most data platforms force you to choose: batch OR streaming. Build both, and you maintain two codebases. That's where pipelines break in production.

I spent three months engineering DataVelocity—a production-grade platform that processes CSV batch loads AND real-time Kafka events through the same medallion architecture (Bronze → Silver → Gold). Same validation rules. Same SCD Type 2 logic. Same quality gates. One codebase.

**The problem nobody talks about:**

Lambda architectures promise the best of both worlds but usually deliver twice the complexity. Batch procedures and streaming queries drift apart. Quality checks become inconsistent. When a delivery status changes in Kafka, it needs the exact same validation as a CSV upload—except most teams implement it twice and watch them diverge over six months.

**What I built:**
A unified data platform handling 10+ entities (customers, orders, deliveries, restaurants, menu items) with:
* **Batch layer**: Master orchestrator procedure coordinating Stage → Bronze → Silver → Gold with full transaction management
* **Speed layer**: Kafka topics → Landing tables (ORDERS\_STREAM\_SLV) → Snowflake Streams CDC → Bronze tables → Same Bronze-to-Gold procedures as batch
* **Serving layer**: SCD Type 2 dimensions + status-tracking facts + denormalized MART tables—all queryable with sub-second latency

Both paths converge at Bronze. Same MERGE logic. Same data quality framework. Same audit trail.

**Engineering decisions that made this work:**

1\. Dual-stage streaming architecture with unified Bronze
Kafka events land in lightweight staging tables (ORDERS\_STREAM\_SLV) via Snowpipe Streaming for sub-5-second ingestion. Snowflake Streams (CDC) capture changes and feed Bronze tables (ORDERS\_BRZ) where they join the batch pipeline. From Bronze onwards, streaming and batch use identical procedures. Zero code duplication.

2\. Metadata-driven validation framework
Validation rules stored in COMMON.DQ\_CONFIG table—not hardcoded in procedures. One rule definition applies to both batch and streaming. The same SP\_EXECUTE\_DATA\_QUALITY\_VALIDATION procedure runs on every record after Bronze. Invalid records logged to \*\_LOAD\_ERROR tables with exact error messages, regardless of whether they came from CSV or Kafka.

3\. Hash-based SCD Type 2 with explicit staging
Most SCD2 implementations compare 15 columns individually. I compute SHA2\_HEX once on concatenated fields—10x faster. Three-step process: detect changes in staging table → expire old versions (STATUS='INACTIVE') → insert new versions (STATUS='ACTIVE'). Works identically whether the change came from a CSV batch or a Kafka status update event.

4\. Transactional Bronze layer with indefinite replay
Every Bronze table stores both transformed columns AND raw values (\*\_RAW columns for batch, RECORD\_CONTENT for streaming). Failed at Silver? Reprocess from Bronze without touching source systems. Explicit BEGIN/COMMIT boundaries with INGEST\_RUN\_ID consumption only on success—zero duplicate loads even on retries.

5\. Snowflake-native streaming orchestration
Kafka events land in *_STREAM_SLV tables with RECORD_METADATA (offset, partition, timestamp) and RECORD_CONTENT (event payload). Landing tables feed Snowflake Streams (append-only CDC). Scheduled Tasks wake up every 5 minutes, check for new data via SYSTEM$STREAM\_HAS\_DATA(), then call the EXACT same stored procedures that process batch files: SP\_\*\_BRONZE\_TO\_SILVER → SP\_\*\_SILVER\_TO\_GOLD. One codebase handles both ingestion modes.

6\. Unified status tracking with complete audit trail
Orders and deliveries have CURRENT\_STATUS, INITIAL\_STATUS, and separate \*\_STATUS\_HISTORY tables. Whether a status update comes from CSV reload or Kafka event, the MERGE logic captures the change before updating, logs it to history, then commits. Every state transition traceable with timestamps, batch IDs, and Kafka offsets.

**What this solved:**

**No logic duplication:** One set of stored procedures handles both ingestion paths from Bronze onwards

**Consistent quality:** 95%+ validation pass rate across batch and streaming (same DQ rules, same error logging)

**Sub-15-second streaming latency:** Kafka → Landing → Bronze → Silver → Gold in under 15 seconds with DQ checks

**Full observability:** Every record traceable via INGEST\_RUN\_ID (batch) or Kafka offset + BATCH\_ID (streaming)

**Historical accuracy:** SCD2 tracks every version of every dimension, whether changed via CSV or event

**Indefinite replay:** Bronze tables retained forever (not limited to Kafka's 30-day retention)


**Measurable outcomes:**

Processed 100+ batch files (customers, locations, restaurants, orders) with zero data loss

Real-time order lifecycle simulation: order created → items added → delivery assigned → status updates → completed—all in <15 seconds end-to-end

~2 second pipeline execution for 5k batch records through all three layers

Zero duplicate loads across 50+ test runs with intentional failures and retries

Same data quality framework catches 95%+ of issues in both batch and streaming before Silver

**What I learned the hard way:**

The Lambda Architecture isn't about building two separate pipelines. It's about designing transformations that work on both micro-batches (streaming) and macro-batches (files) with identical logic. The medallion layers provide structure. Data quality gates provide safety. The convergence point (Bronze) eliminates duplication.

The hardest challenge wasn't building the architecture—it was making SCD2 fast enough to handle streaming ingestion rates while maintaining ACID guarantees. Hash-based change detection and explicit staging tables solved it.

**Tech stack specifics:**

Snowflake (multi-cluster compute, micro-partitioning, clustering keys on INGEST\_RUN\_ID and business keys)

Kafka 3.x + Snowpipe Streaming Connector (API-based, not traditional Snowpipe)

Snowflake Streams (append-only CDC for change capture)

Snowflake Tasks (scheduled processing with SYSTEM$STREAM\_HAS\_DATA conditions)

Docker Compose for local Kafka stack (Zookeeper, Broker, Connect, UI)

Python producer for event generation and lifecycle testing

Pure SQL—no Spark, no Airflow (Snowflake native features handle everything)

**Why this matters for real-world systems:**

Most companies start with batch, add streaming later, and end up maintaining two platforms with divergent logic and separate quality checks. This approach proves you can unify them from day one without sacrificing performance or reliability. It's not theoretical—the entire pipeline is on GitHub with DDL, procedures, Kafka configs, monitoring queries, and a Python event generator for testing the full lifecycle.

If you're architecting data platforms, dealing with Lambda Architecture complexity, or interviewing engineers who claim they've built "production pipelines," I'd value your perspective on these trade-offs—especially around unified quality frameworks, CDC-based stream processing, and SCD2 performance at streaming speeds.

**Full repository with architecture docs, all SQL, Kafka setup, and design decisions:**

GitHub link: https://github.com/Pathakdarshan12/DataVelocity-metadata-driven-lambda-platform

#DataEngineering #Snowflake #LambdaArchitecture
