{{
    config(
        materialized='table',
        schema='gold',
        tags=['dimension', 'restaurant', 'scd2']
    )
}}

SELECT
    RESTAURANT_ID,
    FSSAI_REGISTRATION_NO,
    RESTAURANT_NAME,
    CUISINE_TYPE,
    PRICING_FOR_TWO,
    RESTAURANT_PHONE,
    OPERATING_HOURS,
    LOCATION_ID,
    ACTIVE_FLAG,
    OPEN_STATUS,
    LOCALITY,
    RESTAURANT_ADDRESS,
    LATITUDE,
    LONGITUDE,
    BATCH_ID,
    STATUS,
    EFF_START_DT,
    EFF_END_DT,
    CREATED_AT,
    UPDATED_AT
FROM {{ source('gold', 'DIM_RESTAURANT') }}