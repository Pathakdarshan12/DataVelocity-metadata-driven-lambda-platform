{{
    config(
        materialized='table',
        tags=['analytics', 'orders', 'daily'],
        description='Temporal patterns and trends in order behavior'
    )
}}

SELECT
    o.ORDER_ID,
    o.ORDER_DATE,
    DATE(o.ORDER_DATE) AS ORDER_DATE_KEY,
    o.CUSTOMER_ID,
    o.RESTAURANT_ID,
    o.TOTAL_AMOUNT,
    o.CURRENT_STATUS,
    o.PAYMENT_METHOD,

    -- Date Dimensions
    d.YEAR,
    d.QUARTER,
    d.MONTH,
    d.WEEK,
    d.DAY_OF_YEAR,
    d.DAY_OF_WEEK,
    d.DAY_OF_THE_MONTH,
    d.DAY_NAME,

    -- Time of Day
    HOUR(o.ORDER_DATE) AS ORDER_HOUR,
    MINUTE(o.ORDER_DATE) AS ORDER_MINUTE,
    CASE
        WHEN HOUR(o.ORDER_DATE) BETWEEN 6 AND 11 THEN 'MORNING'
        WHEN HOUR(o.ORDER_DATE) BETWEEN 12 AND 16 THEN 'AFTERNOON'
        WHEN HOUR(o.ORDER_DATE) BETWEEN 17 AND 21 THEN 'EVENING'
        ELSE 'NIGHT'
    END AS TIME_OF_DAY,

    -- Day Type Classifications
    CASE
        WHEN d.DAY_OF_WEEK IN (6, 7) THEN 'WEEKEND'
        ELSE 'WEEKDAY'
    END AS DAY_TYPE,

    CASE
        WHEN d.DAY_OF_WEEK = 1 THEN 'MONDAY'
        WHEN d.DAY_OF_WEEK = 5 THEN 'FRIDAY'
        WHEN d.DAY_OF_WEEK IN (6, 7) THEN 'WEEKEND'
        ELSE 'MID_WEEK'
    END AS DAY_CATEGORY,

    -- Meal Time Classification
    CASE
        WHEN HOUR(o.ORDER_DATE) BETWEEN 7 AND 10 THEN 'BREAKFAST'
        WHEN HOUR(o.ORDER_DATE) BETWEEN 11 AND 14 THEN 'LUNCH'
        WHEN HOUR(o.ORDER_DATE) BETWEEN 16 AND 18 THEN 'SNACK'
        WHEN HOUR(o.ORDER_DATE) BETWEEN 19 AND 22 THEN 'DINNER'
        ELSE 'LATE_NIGHT'
    END AS MEAL_TIME,

    -- Peak Hour Indicators
    CASE
        WHEN HOUR(o.ORDER_DATE) BETWEEN 12 AND 14 THEN 'LUNCH_PEAK'
        WHEN HOUR(o.ORDER_DATE) BETWEEN 19 AND 21 THEN 'DINNER_PEAK'
        ELSE 'OFF_PEAK'
    END AS PEAK_INDICATOR,

    -- Restaurant and Location
    r.RESTAURANT_NAME,
    r.CUISINE_TYPE,
    l.CITY,
    l.STATE,

    -- Temporal Aggregations
    COUNT(o.ORDER_ID) OVER (
        PARTITION BY DATE(o.ORDER_DATE), HOUR(o.ORDER_DATE)
    ) AS ORDERS_IN_SAME_HOUR,

    COUNT(o.ORDER_ID) OVER (
        PARTITION BY o.RESTAURANT_ID, DATE(o.ORDER_DATE), HOUR(o.ORDER_DATE)
    ) AS RESTAURANT_ORDERS_IN_SAME_HOUR,

    COUNT(o.ORDER_ID) OVER (
        PARTITION BY o.CUSTOMER_ID, DATE(o.ORDER_DATE)
    ) AS CUSTOMER_ORDERS_SAME_DAY,

    -- Comparative Metrics
    AVG(o.TOTAL_AMOUNT) OVER (
        PARTITION BY HOUR(o.ORDER_DATE)
    ) AS AVG_ORDER_VALUE_FOR_HOUR,

    AVG(o.TOTAL_AMOUNT) OVER (
        PARTITION BY d.DAY_OF_WEEK
    ) AS AVG_ORDER_VALUE_FOR_DAY,

    -- Seasonality Indicators
    CASE
        WHEN d.MONTH IN (12, 1, 2) THEN 'WINTER'
        WHEN d.MONTH IN (3, 4, 5) THEN 'SPRING'
        WHEN d.MONTH IN (6, 7, 8) THEN 'SUMMER'
        ELSE 'FALL'
    END AS SEASON,

    -- Month Category
    CASE
        WHEN d.DAY_OF_THE_MONTH <= 10 THEN 'EARLY_MONTH'
        WHEN d.DAY_OF_THE_MONTH <= 20 THEN 'MID_MONTH'
        ELSE 'END_MONTH'
    END AS MONTH_PERIOD,

    CURRENT_TIMESTAMP() AS CREATED_AT

FROM {{ ref('fact_order') }} o
LEFT JOIN {{ ref('dim_date') }} d
    ON DATE(o.ORDER_DATE) = d.CALENDAR_DATE
LEFT JOIN {{ ref('dim_restaurant') }} r
    ON o.RESTAURANT_ID = r.RESTAURANT_ID
LEFT JOIN {{ ref('dim_location') }} l
    ON r.LOCATION_ID = l.LOCATION_ID