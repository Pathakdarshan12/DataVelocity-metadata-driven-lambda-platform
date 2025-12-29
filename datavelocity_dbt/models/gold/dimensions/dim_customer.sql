{{
    config(
        materialized='table',
        schema='gold',
        tags=['dimension', 'customer', 'scd2', 'pii']
    )
}}

SELECT
    CUSTOMER_ID,
    NAME,
    MOBILE,
    EMAIL,
    LOGIN_BY_USING,
    GENDER,
    DOB,
    ANNIVERSARY,
    PREFERENCES,
    STATUS,
    EFF_START_DT,
    EFF_END_DT,
    BATCH_ID,
    CREATED_AT,
    UPDATED_AT
FROM {{ source('gold', 'DIM_CUSTOMER') }}