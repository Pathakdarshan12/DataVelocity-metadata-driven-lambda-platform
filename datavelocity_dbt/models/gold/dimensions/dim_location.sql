{{
    config(
        materialized='table',
        schema='gold',
        tags=['dimension', 'location', 'scd2']
    )
}}

-- Since table already exists, we'll select from it
-- This allows dbt to manage documentation and tests
SELECT
    LOCATION_ID,
    CITY,
    STATE,
    STATE_CODE,
    IS_UNION_TERRITORY,
    CAPITAL_CITY_FLAG,
    CITY_TIER,
    ZIP_CODE,
    STATUS,
    EFF_START_DT,
    EFF_END_DT,
    BATCH_ID,
    CREATED_AT,
    UPDATED_AT
FROM {{ source('gold', 'DIM_LOCATION') }}