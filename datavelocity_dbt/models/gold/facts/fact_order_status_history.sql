{{
    config(
        materialized='table',
        schema='gold',
        tags=['fact', 'order', 'audit', 'history']
    )
}}

-- FACT_ORDER_STATUS_HISTORY
-- Audit trail for order status changes
-- Tracks all status transitions for orders

SELECT
    STATUS_HISTORY_KEY,
    ORDER_ID,
    OLD_STATUS,
    NEW_STATUS,
    STATUS_CHANGED_AT,
    BATCH_ID
FROM {{ source('gold', 'FACT_ORDER_STATUS_HISTORY') }}