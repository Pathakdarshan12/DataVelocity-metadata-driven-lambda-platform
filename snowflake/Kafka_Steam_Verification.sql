-- =====================================================
-- VERIFICATION QUERIES
-- =====================================================
USE ROLE ACCOUNTADMIN;
USE DATABASE SWIGGY;
USE WAREHOUSE ADHOC_WH;

-- =====================================================
-- 1. CHECK SILVER STREAM TABLES
-- =====================================================

-- Orders in Silver
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT ORDER_ID) AS unique_orders,
    MIN(INGESTED_AT) AS first_record,
    MAX(INGESTED_AT) AS last_record,
    SUM(CASE WHEN PROCESSED_TO_GOLD THEN 1 ELSE 0 END) AS processed_count
FROM SILVER.ORDERS_STREAM_SLV;

-- Sample order data
SELECT 
    ORDER_ID,
    EVENT_TYPE,
    STATUS,
    TOTAL_AMOUNT,
    INGESTED_AT,
    PROCESSED_TO_GOLD
FROM SILVER.ORDERS_STREAM_SLV
ORDER BY INGESTED_AT DESC
LIMIT 10;

-- Order Items in Silver
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT ORDER_ITEM_ID) AS unique_items,
    COUNT(DISTINCT ORDER_ID) AS unique_orders,
    MIN(INGESTED_AT) AS first_record,
    MAX(INGESTED_AT) AS last_record
FROM SILVER.ORDER_ITEMS_STREAM_SLV;

-- Delivery in Silver
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT DELIVERY_ID) AS unique_deliveries,
    MIN(INGESTED_AT) AS first_record,
    MAX(INGESTED_AT) AS last_record
FROM SILVER.DELIVERY_STREAM_SLV;

-- =====================================================
-- 2. CHECK STREAMS (CDC)
-- =====================================================

-- Check if streams have data
SELECT 
    'STREAM_ORDERS_CHANGES' AS stream_name,
    SYSTEM$STREAM_HAS_DATA('STREAM_ORDERS_CHANGES') AS has_data;

SELECT 
    'STREAM_ORDER_ITEMS_CHANGES' AS stream_name,
    SYSTEM$STREAM_HAS_DATA('STREAM_ORDER_ITEMS_CHANGES') AS has_data;

SELECT 
    'STREAM_DELIVERY_CHANGES' AS stream_name,
    SYSTEM$STREAM_HAS_DATA('STREAM_DELIVERY_CHANGES') AS has_data;

-- View pending changes in streams
SELECT COUNT(*) AS pending_orders 
FROM STREAM_ORDERS_CHANGES;

SELECT COUNT(*) AS pending_items 
FROM STREAM_ORDER_ITEMS_CHANGES;

SELECT COUNT(*) AS pending_deliveries 
FROM STREAM_DELIVERY_CHANGES;

-- =====================================================
-- 3. CHECK GOLD TABLES
-- =====================================================

-- Orders in Gold
SELECT 
    COUNT(*) AS total_orders,
    COUNT(DISTINCT ORDER_ID) AS unique_orders,
    COUNT(DISTINCT CUSTOMER_ID) AS unique_customers,
    MIN(CREATED_AT) AS first_order,
    MAX(CREATED_AT) AS last_order
FROM GOLD.FACT_ORDER;

-- Order status distribution
SELECT 
    CURRENT_STATUS,
    COUNT(*) AS order_count,
    SUM(TOTAL_AMOUNT) AS total_revenue
FROM GOLD.FACT_ORDER
GROUP BY CURRENT_STATUS
ORDER BY order_count DESC;

-- Order Items in Gold
SELECT 
    COUNT(*) AS total_items,
    COUNT(DISTINCT ORDER_ID) AS unique_orders,
    SUM(QUANTITY) AS total_quantity,
    SUM(SUBTOTAL) AS total_amount
FROM GOLD.FACT_ORDER_ITEM;

-- Delivery in Gold
SELECT 
    COUNT(*) AS total_deliveries,
    COUNT(DISTINCT ORDER_ID) AS unique_orders
FROM GOLD.FACT_DELIVERY;

-- Delivery status distribution
SELECT 
    CURRENT_STATUS,
    COUNT(*) AS delivery_count
FROM GOLD.FACT_DELIVERY
GROUP BY CURRENT_STATUS
ORDER BY delivery_count DESC;

-- =====================================================
-- 4. CHECK TASK EXECUTION
-- =====================================================

-- Task history
SELECT 
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    DATEDIFF(SECOND, SCHEDULED_TIME, COMPLETED_TIME) AS duration_sec,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(HOUR, -1, CURRENT_TIMESTAMP()),
    TASK_NAME => 'TASK_ORDERS_STREAM_TO_GOLD'
))
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;

-- All streaming tasks
SELECT 
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
))
WHERE NAME LIKE 'TASK_%_STREAM_TO_GOLD'
ORDER BY SCHEDULED_TIME DESC
LIMIT 20;

-- =====================================================
-- 5. END-TO-END DATA FLOW
-- =====================================================

-- Complete order journey
SELECT 
    'SILVER' AS layer,
    COUNT(DISTINCT ORDER_ID) AS order_count
FROM SILVER.ORDERS_STREAM_SLV
UNION ALL
SELECT 
    'GOLD',
    COUNT(DISTINCT ORDER_ID)
FROM GOLD.FACT_ORDER;

-- Order with items and delivery
SELECT 
    o.ORDER_ID,
    o.CUSTOMER_ID,
    o.TOTAL_AMOUNT,
    o.CURRENT_STATUS AS order_status,
    COUNT(DISTINCT oi.ORDER_ITEM_ID) AS item_count,
    d.DELIVERY_ID,
    d.CURRENT_STATUS AS delivery_status,
    d.DELIVERY_AGENT_ID
FROM GOLD.FACT_ORDER o
LEFT JOIN GOLD.FACT_ORDER_ITEM oi ON o.ORDER_ID = oi.ORDER_ID
LEFT JOIN GOLD.FACT_DELIVERY d ON o.ORDER_ID = d.ORDER_ID
GROUP BY 1,2,3,4,6,7,8
ORDER BY o.ORDER_ID DESC
LIMIT 10;

-- =====================================================
-- 6. STATUS CHANGE HISTORY
-- =====================================================

-- Order status changes
SELECT 
    ORDER_ID,
    OLD_STATUS,
    NEW_STATUS,
    STATUS_CHANGED_AT,
    BATCH_ID
FROM GOLD.FACT_ORDER_STATUS_HISTORY
ORDER BY STATUS_CHANGED_AT DESC
LIMIT 20;

-- Delivery status changes
SELECT 
    DELIVERY_ID,
    OLD_STATUS,
    NEW_STATUS,
    STATUS_CHANGED_AT,
    BATCH_ID
FROM GOLD.FACT_DELIVERY_STATUS_HISTORY
ORDER BY STATUS_CHANGED_AT DESC
LIMIT 20;

-- =====================================================
-- 7. LATENCY ANALYSIS
-- =====================================================

-- Average time from Kafka to Silver
SELECT 
    AVG(DATEDIFF(SECOND, KAFKA_TIMESTAMP, INGESTED_AT)) AS avg_latency_sec,
    MIN(DATEDIFF(SECOND, KAFKA_TIMESTAMP, INGESTED_AT)) AS min_latency_sec,
    MAX(DATEDIFF(SECOND, KAFKA_TIMESTAMP, INGESTED_AT)) AS max_latency_sec
FROM SILVER.ORDERS_STREAM_SLV
WHERE KAFKA_TIMESTAMP IS NOT NULL;

-- =====================================================
-- 8. DATA QUALITY CHECKS
-- =====================================================

-- Check for missing order items
SELECT 
    o.ORDER_ID,
    o.TOTAL_AMOUNT,
    COUNT(oi.ORDER_ITEM_ID) AS item_count
FROM GOLD.FACT_ORDER o
LEFT JOIN GOLD.FACT_ORDER_ITEM oi ON o.ORDER_ID = oi.ORDER_ID
GROUP BY 1,2
HAVING COUNT(oi.ORDER_ITEM_ID) = 0;

-- Check for orders without delivery
SELECT 
    o.ORDER_ID,
    o.CURRENT_STATUS,
    o.CREATED_AT
FROM GOLD.FACT_ORDER o
LEFT JOIN GOLD.FACT_DELIVERY d ON o.ORDER_ID = d.ORDER_ID
WHERE d.DELIVERY_ID IS NULL
  AND o.CURRENT_STATUS NOT IN ('CANCELLED');

-- =====================================================
-- 9. REAL-TIME DASHBOARD QUERIES
-- =====================================================

-- Orders in last hour
SELECT 
    DATE_TRUNC('MINUTE', CREATED_AT) AS minute,
    COUNT(*) AS order_count,
    SUM(TOTAL_AMOUNT) AS revenue
FROM GOLD.FACT_ORDER
WHERE CREATED_AT >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1 DESC;

-- Current active deliveries
SELECT 
    CURRENT_STATUS,
    COUNT(*) AS delivery_count
FROM GOLD.FACT_DELIVERY
WHERE CURRENT_STATUS NOT IN ('DELIVERED', 'CANCELLED')
GROUP BY 1;