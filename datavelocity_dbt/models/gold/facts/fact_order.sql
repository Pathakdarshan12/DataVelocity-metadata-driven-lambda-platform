{{
    config(
        materialized='table',
        schema='gold',
        tags=['fact', 'order']
    )
}}

-- Aggregate order items to create order-level fact table
-- This is derived from FACT_ORDER_ITEM since you don't have a separate FACT_ORDER table

WITH order_aggregates AS (
    SELECT
        ORDER_ID,
        MIN(ORDER_TIMESTAMP) AS ORDER_TIMESTAMP,
        COUNT(DISTINCT MENU_ID) AS TOTAL_ITEMS,
        SUM(QUANTITY) AS TOTAL_QUANTITY,
        SUM(SUBTOTAL) AS ORDER_TOTAL,
        MAX(BATCH_ID) AS BATCH_ID,
        MIN(CREATED_AT) AS CREATED_AT
    FROM {{ source('gold', 'FACT_ORDER_ITEM') }}
    GROUP BY ORDER_ID
)

SELECT
    ORDER_ID,
    ORDER_TIMESTAMP,
    TOTAL_ITEMS,
    TOTAL_QUANTITY,
    ORDER_TOTAL,
    BATCH_ID,
    CREATED_AT
FROM order_aggregates