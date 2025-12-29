{{
    config(
        materialized='table',
        tags=['analytics', 'orders', 'daily'],
        cluster_by=['ORDER_DATE', 'CURRENT_STATUS'],
        description='Comprehensive order summary with customer and restaurant context'
    )
}}

SELECT
    -- Order Information
    o.ORDER_ID,
    o.ORDER_DATE,
    DATE(o.ORDER_DATE) AS ORDER_DATE_KEY,
    o.TOTAL_AMOUNT,
    o.CURRENT_STATUS,
    o.INITIAL_STATUS,
    o.PAYMENT_METHOD,
    o.STATUS_UPDATED_AT,

    -- Customer Information
    o.CUSTOMER_ID,
    c.NAME AS CUSTOMER_NAME,
    c.GENDER AS CUSTOMER_GENDER,
    c.LOGIN_BY_USING,
    DATEDIFF(YEAR, c.DOB, o.ORDER_DATE) AS CUSTOMER_AGE_AT_ORDER,

    -- Restaurant Information
    o.RESTAURANT_ID,
    r.RESTAURANT_NAME,
    r.CUISINE_TYPE,
    r.PRICING_FOR_TWO AS RESTAURANT_PRICING,
    r.LOCALITY AS RESTAURANT_LOCALITY,

    -- Location Information
    l.CITY,
    l.STATE,
    l.CITY_TIER,
    l.ZIP_CODE,

    -- Delivery Information
    fd.DELIVERY_ID,
    fd.DELIVERY_AGENT_ID,
    fd.DELIVERY_DATE,
    fd.CURRENT_STATUS AS DELIVERY_STATUS,
    TRY_CAST(REGEXP_REPLACE(fd.ESTIMATED_TIME, '[^0-9]', '') AS INTEGER) AS ESTIMATED_DELIVERY_TIME_MINS,
    DATEDIFF(MINUTE, o.ORDER_DATE, fd.DELIVERY_DATE) AS ACTUAL_DELIVERY_TIME_MINS,

    -- Time Dimensions
    d.YEAR,
    d.QUARTER,
    d.MONTH,
    d.WEEK,
    d.DAY_OF_WEEK,
    d.DAY_NAME,
    HOUR(o.ORDER_DATE) AS ORDER_HOUR,
    CASE
        WHEN HOUR(o.ORDER_DATE) BETWEEN 6 AND 11 THEN 'MORNING'
        WHEN HOUR(o.ORDER_DATE) BETWEEN 12 AND 16 THEN 'AFTERNOON'
        WHEN HOUR(o.ORDER_DATE) BETWEEN 17 AND 21 THEN 'EVENING'
        ELSE 'NIGHT'
    END AS TIME_OF_DAY,
    CASE
        WHEN d.DAY_OF_WEEK IN (6, 7) THEN 'WEEKEND'
        ELSE 'WEEKDAY'
    END AS DAY_TYPE,

    -- Order Characteristics
    CASE
        WHEN o.TOTAL_AMOUNT < 200 THEN 'LOW_VALUE'
        WHEN o.TOTAL_AMOUNT < 500 THEN 'MEDIUM_VALUE'
        WHEN o.TOTAL_AMOUNT < 1000 THEN 'HIGH_VALUE'
        ELSE 'PREMIUM'
    END AS ORDER_VALUE_SEGMENT,

    -- Customer Order History at Time of Order
    ROW_NUMBER() OVER (
        PARTITION BY o.CUSTOMER_ID
        ORDER BY o.ORDER_DATE
    ) AS CUSTOMER_ORDER_NUMBER,

    -- Restaurant Order History
    ROW_NUMBER() OVER (
        PARTITION BY o.RESTAURANT_ID
        ORDER BY o.ORDER_DATE
    ) AS RESTAURANT_ORDER_NUMBER,

    -- Status Flags
    CASE WHEN o.CURRENT_STATUS = 'COMPLETED' THEN 1 ELSE 0 END AS IS_COMPLETED,
    CASE WHEN o.CURRENT_STATUS = 'CANCELLED' THEN 1 ELSE 0 END AS IS_CANCELLED,
    CASE WHEN o.CURRENT_STATUS = 'PENDING' THEN 1 ELSE 0 END AS IS_PENDING,

    -- Delivery Performance Flags
    CASE
        WHEN fd.CURRENT_STATUS = 'DELIVERED'
            AND DATEDIFF(MINUTE, o.ORDER_DATE, fd.DELIVERY_DATE) <=
                TRY_CAST(REGEXP_REPLACE(fd.ESTIMATED_TIME, '[^0-9]', '') AS INTEGER)
        THEN 1
        ELSE 0
    END AS IS_ON_TIME_DELIVERY,

    CASE
        WHEN fd.CURRENT_STATUS = 'DELIVERED' THEN 1
        ELSE 0
    END AS IS_DELIVERED,

    -- Processing Time
    DATEDIFF(MINUTE, o.CREATED_AT, o.STATUS_UPDATED_AT) AS ORDER_PROCESSING_TIME_MINS,

    CURRENT_TIMESTAMP() AS CREATED_AT

FROM {{ ref('fact_order') }} o
LEFT JOIN {{ ref('dim_customer') }} c
    ON o.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN {{ ref('dim_restaurant') }} r
    ON o.RESTAURANT_ID = r.RESTAURANT_ID
LEFT JOIN {{ ref('dim_location') }} l
    ON r.LOCATION_ID = l.LOCATION_ID
LEFT JOIN {{ ref('fact_delivery') }} fd
    ON o.ORDER_ID = fd.ORDER_ID
LEFT JOIN {{ ref('dim_date') }} d
    ON DATE(o.ORDER_DATE) = d.CALENDAR_DATE