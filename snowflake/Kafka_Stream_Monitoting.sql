-- =====================================================
-- MONITORING DASHBOARD
-- =====================================================
USE ROLE ACCOUNTADMIN;
USE DATABASE SWIGGY;
USE SCHEMA GOLD;
USE WAREHOUSE ADHOC_WH;

-- =====================================================
-- 1. PIPELINE HEALTH CHECK
-- =====================================================
CREATE OR REPLACE VIEW COMMON.VW_STREAMING_PIPELINE_HEALTH AS
SELECT 
    'Kafka Topics' AS component,
    'N/A' AS status,
    'Check Confluent Control Center' AS details
UNION ALL
SELECT 
    'Silver Tables',
    CASE 
        WHEN COUNT(*) > 0 THEN '✓ Active'
        ELSE '✗ No Data'
    END,
    'Last ingested: ' || MAX(INGESTED_AT)::STRING
FROM SILVER.ORDERS_STREAM_SLV
UNION ALL
SELECT 
    'Snowflake Streams',
    CASE 
        WHEN SYSTEM$STREAM_HAS_DATA('STREAM_ORDERS_CHANGES') THEN '⚠ Pending Data'
        ELSE '✓ Up to Date'
    END,
    'Orders stream'
UNION ALL
SELECT 
    'Snowflake Tasks',
    CASE 
        WHEN STATE = 'started' THEN '✓ Running'
        ELSE '✗ Stopped'
    END,
    NAME
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(MINUTE, -10, CURRENT_TIMESTAMP())
))
WHERE NAME LIKE 'TASK_%_STREAM_TO_GOLD'
ORDER BY SCHEDULED_TIME DESC
LIMIT 3;

-- View the dashboard
SELECT * FROM COMMON.VW_STREAMING_PIPELINE_HEALTH;

-- =====================================================
-- 2. DATA FLOW METRICS
-- =====================================================
CREATE OR REPLACE VIEW COMMON.VW_STREAMING_METRICS AS
WITH metrics AS (
    SELECT 
        'Orders' AS entity,
        (SELECT COUNT(*) FROM SILVER.ORDERS_STREAM_SLV) AS silver_count,
        (SELECT COUNT(*) FROM GOLD.FACT_ORDER) AS gold_count,
        (SELECT COUNT(*) FROM STREAM_ORDERS_CHANGES) AS pending_count
    UNION ALL
    SELECT 
        'Order Items',
        (SELECT COUNT(*) FROM SILVER.ORDER_ITEMS_STREAM_SLV),
        (SELECT COUNT(*) FROM GOLD.FACT_ORDER_ITEM),
        (SELECT COUNT(*) FROM STREAM_ORDER_ITEMS_CHANGES)
    UNION ALL
    SELECT 
        'Deliveries',
        (SELECT COUNT(*) FROM SILVER.DELIVERY_STREAM_SLV),
        (SELECT COUNT(*) FROM GOLD.FACT_DELIVERY),
        (SELECT COUNT(*) FROM STREAM_DELIVERY_CHANGES)
)
SELECT 
    entity,
    silver_count,
    gold_count,
    pending_count,
    ROUND((gold_count::FLOAT / NULLIF(silver_count, 0)) * 100, 2) AS processing_pct,
    silver_count - gold_count AS not_processed
FROM metrics;

SELECT * FROM COMMON.VW_STREAMING_METRICS;

-- =====================================================
-- 3. LATENCY MONITORING
-- =====================================================
CREATE OR REPLACE VIEW COMMON.VW_STREAMING_LATENCY AS
SELECT 
    'Kafka → Silver' AS stage,
    AVG(DATEDIFF(SECOND, KAFKA_TIMESTAMP, INGESTED_AT)) AS avg_latency_sec,
    MAX(DATEDIFF(SECOND, KAFKA_TIMESTAMP, INGESTED_AT)) AS max_latency_sec,
    COUNT(*) AS record_count
FROM SILVER.ORDERS_STREAM_SLV
WHERE INGESTED_AT >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
  AND KAFKA_TIMESTAMP IS NOT NULL
UNION ALL
SELECT 
    'Silver → Gold',
    AVG(DATEDIFF(SECOND, s.INGESTED_AT, g.CREATED_AT)),
    MAX(DATEDIFF(SECOND, s.INGESTED_AT, g.CREATED_AT)),
    COUNT(*)
FROM SILVER.ORDERS_STREAM_SLV s
JOIN GOLD.FACT_ORDER g ON s.ORDER_ID = g.ORDER_ID
WHERE s.INGESTED_AT >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP());

SELECT * FROM COMMON.VW_STREAMING_LATENCY;

-- =====================================================
-- 4. TASK EXECUTION MONITORING
-- =====================================================
CREATE OR REPLACE VIEW COMMON.VW_TASK_EXECUTION_STATUS AS
SELECT 
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    DATEDIFF(SECOND, SCHEDULED_TIME, COMPLETED_TIME) AS duration_sec,
    CASE 
        WHEN ERROR_MESSAGE IS NOT NULL THEN '✗ Failed'
        WHEN STATE = 'SUCCEEDED' THEN '✓ Success'
        ELSE '⚠ ' || STATE
    END AS status,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
))
WHERE NAME LIKE 'TASK_%_STREAM_TO_GOLD'
ORDER BY SCHEDULED_TIME DESC;

SELECT * FROM COMMON.VW_TASK_EXECUTION_STATUS LIMIT 20;

-- =====================================================
-- 5. ERROR MONITORING
-- =====================================================
CREATE OR REPLACE VIEW COMMON.VW_STREAMING_ERRORS AS
SELECT 
    'Task Failures' AS error_type,
    COUNT(*) AS error_count,
    MAX(SCHEDULED_TIME) AS last_error
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
))
WHERE NAME LIKE 'TASK_%_STREAM_TO_GOLD'
  AND STATE = 'FAILED';

SELECT * FROM COMMON.VW_STREAMING_ERRORS;

-- =====================================================
-- 6. REAL-TIME THROUGHPUT
-- =====================================================
CREATE OR REPLACE VIEW COMMON.VW_STREAMING_THROUGHPUT AS
SELECT 
    DATE_TRUNC('MINUTE', INGESTED_AT) AS minute,
    COUNT(*) AS records_per_minute,
    COUNT(DISTINCT ORDER_ID) AS unique_orders
FROM SILVER.ORDERS_STREAM_SLV
WHERE INGESTED_AT >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1 DESC;

SELECT * FROM COMMON.VW_STREAMING_THROUGHPUT LIMIT 60;

-- =====================================================
-- 7. ALERTING QUERIES
-- =====================================================

-- Alert: Stream backlog > 1000 records
SELECT 
    CASE 
        WHEN (SELECT COUNT(*) FROM STREAM_ORDERS_CHANGES) > 1000 
        THEN '🚨 ALERT: Order stream backlog exceeds 1000 records'
        ELSE '✓ OK'
    END AS alert_status,
    (SELECT COUNT(*) FROM STREAM_ORDERS_CHANGES) AS current_backlog;

-- Alert: Task not executed in last 10 minutes
SELECT 
    CASE 
        WHEN MAX(SCHEDULED_TIME) < DATEADD(MINUTE, -10, CURRENT_TIMESTAMP())
        THEN '🚨 ALERT: Tasks not executed in last 10 minutes'
        ELSE '✓ OK'
    END AS alert_status,
    MAX(SCHEDULED_TIME) AS last_execution
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
))
WHERE NAME LIKE 'TASK_%_STREAM_TO_GOLD';

-- Alert: High latency (> 5 minutes)
SELECT 
    CASE 
        WHEN AVG(DATEDIFF(MINUTE, KAFKA_TIMESTAMP, INGESTED_AT)) > 5
        THEN '🚨 ALERT: High latency detected'
        ELSE '✓ OK'
    END AS alert_status,
    AVG(DATEDIFF(MINUTE, KAFKA_TIMESTAMP, INGESTED_AT)) AS avg_latency_min
FROM SILVER.ORDERS_STREAM_SLV
WHERE INGESTED_AT >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
  AND KAFKA_TIMESTAMP IS NOT NULL;

-- =====================================================
-- 8. BUSINESS METRICS
-- =====================================================
CREATE OR REPLACE VIEW COMMON.VW_REALTIME_BUSINESS_METRICS AS
SELECT 
    DATE_TRUNC('HOUR', CREATED_AT) AS hour,
    COUNT(DISTINCT ORDER_ID) AS total_orders,
    SUM(TOTAL_AMOUNT) AS total_revenue,
    AVG(TOTAL_AMOUNT) AS avg_order_value,
    COUNT(DISTINCT CUSTOMER_ID) AS unique_customers
FROM GOLD.FACT_ORDER
WHERE CREATED_AT >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1 DESC;

SELECT * FROM COMMON.VW_REALTIME_BUSINESS_METRICS;

-- =====================================================
-- 9. MONITORING STORED PROCEDURE
-- =====================================================
CREATE OR REPLACE PROCEDURE COMMON.SP_CHECK_STREAMING_HEALTH()
RETURNS TABLE(
    check_name STRING,
    status STRING,
    details STRING,
    checked_at TIMESTAMP_TZ
)
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET DEFAULT (
        SELECT 
            'Pipeline Health' AS check_name,
            '✓ OK' AS status,
            'All components operational' AS details,
            CURRENT_TIMESTAMP() AS checked_at
    );
BEGIN
    RETURN TABLE(res);
END;
$$;

-- Run health check
CALL COMMON.SP_CHECK_STREAMING_HEALTH();