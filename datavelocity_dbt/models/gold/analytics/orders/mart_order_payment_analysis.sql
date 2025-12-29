{{
    config(
        materialized='table',
        tags=['analytics', 'orders', 'daily'],
        description='Payment method analysis and trends'
    )
}}

SELECT
    o.ORDER_ID,
    o.ORDER_DATE,
    DATE(o.ORDER_DATE) AS ORDER_DATE_KEY,
    o.CUSTOMER_ID,
    c.NAME AS CUSTOMER_NAME,
    o.RESTAURANT_ID,
    r.RESTAURANT_NAME,
    o.TOTAL_AMOUNT,
    o.PAYMENT_METHOD,
    o.CURRENT_STATUS,

    -- Location
    l.CITY,
    l.STATE,
    l.CITY_TIER,

    -- Time Dimensions
    d.YEAR,
    d.MONTH,
    d.DAY_OF_WEEK,
    CASE
        WHEN d.DAY_OF_WEEK IN (6, 7) THEN 'WEEKEND'
        ELSE 'WEEKDAY'
    END AS DAY_TYPE,
    HOUR(o.ORDER_DATE) AS ORDER_HOUR,

    -- Payment Method Categories
    CASE
        WHEN o.PAYMENT_METHOD IN ('CARD', 'CREDIT_CARD', 'DEBIT_CARD') THEN 'CARD'
        WHEN o.PAYMENT_METHOD IN ('UPI', 'GPAY', 'PHONEPE', 'PAYTM') THEN 'UPI'
        WHEN o.PAYMENT_METHOD = 'CASH' THEN 'CASH'
        WHEN o.PAYMENT_METHOD = 'WALLET' THEN 'WALLET'
        WHEN o.PAYMENT_METHOD = 'NET_BANKING' THEN 'NET_BANKING'
        ELSE 'OTHER'
    END AS PAYMENT_CATEGORY,

    -- Order Value Segments
    CASE
        WHEN o.TOTAL_AMOUNT < 200 THEN 'LOW_VALUE'
        WHEN o.TOTAL_AMOUNT < 500 THEN 'MEDIUM_VALUE'
        WHEN o.TOTAL_AMOUNT < 1000 THEN 'HIGH_VALUE'
        ELSE 'PREMIUM'
    END AS ORDER_VALUE_SEGMENT,

    -- Customer Payment History
    COUNT(DISTINCT o.PAYMENT_METHOD) OVER (
        PARTITION BY o.CUSTOMER_ID
        ORDER BY o.ORDER_DATE
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS PAYMENT_METHODS_USED_TO_DATE,

    -- Payment Method Rank for Customer
    ROW_NUMBER() OVER (
        PARTITION BY o.CUSTOMER_ID, o.PAYMENT_METHOD
        ORDER BY o.ORDER_DATE
    ) AS PAYMENT_METHOD_USE_COUNT,

    -- First Time Using This Payment Method
    CASE
        WHEN ROW_NUMBER() OVER (
            PARTITION BY o.CUSTOMER_ID, o.PAYMENT_METHOD
            ORDER BY o.ORDER_DATE
        ) = 1 THEN 1
        ELSE 0
    END AS IS_FIRST_USE_OF_PAYMENT_METHOD,

    -- Restaurant Payment Method Mix
    COUNT(DISTINCT o.PAYMENT_METHOD) OVER (
        PARTITION BY o.RESTAURANT_ID
    ) AS RESTAURANT_PAYMENT_METHODS_ACCEPTED,

    -- Success Indicators
    CASE
        WHEN o.CURRENT_STATUS = 'COMPLETED' THEN 1
        ELSE 0
    END AS PAYMENT_SUCCESS,

    CASE
        WHEN o.CURRENT_STATUS = 'CANCELLED'
            AND o.PAYMENT_METHOD IN ('CARD', 'UPI', 'NET_BANKING')
        THEN 1
        ELSE 0
    END AS POTENTIAL_PAYMENT_FAILURE,

    -- Average Order Value by Payment Method (Customer Level)
    AVG(o.TOTAL_AMOUNT) OVER (
        PARTITION BY o.CUSTOMER_ID, o.PAYMENT_METHOD
    ) AS CUSTOMER_AVG_ORDER_VALUE_BY_PAYMENT,

    -- Average Order Value by Payment Method (Restaurant Level)
    AVG(o.TOTAL_AMOUNT) OVER (
        PARTITION BY o.RESTAURANT_ID, o.PAYMENT_METHOD
    ) AS RESTAURANT_AVG_ORDER_VALUE_BY_PAYMENT,

    CURRENT_TIMESTAMP() AS CREATED_AT

FROM {{ ref('fact_order') }} o
LEFT JOIN {{ ref('dim_customer') }} c
    ON o.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN {{ ref('dim_restaurant') }} r
    ON o.RESTAURANT_ID = r.RESTAURANT_ID
LEFT JOIN {{ ref('dim_location') }} l
    ON r.LOCATION_ID = l.LOCATION_ID
LEFT JOIN {{ ref('dim_date') }} d
    ON DATE(o.ORDER_DATE) = d.CALENDAR_DATE