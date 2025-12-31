-- ====================================================================================================
-- DELIVERY
-- ====================================================================================================
-- CHANGE CONVARCHAR
USE DATABASE DATAVELOCITY;
USE SCHEMA BRONZE;
USE WAREHOUSE ADHOC_WH;

-- ----------------------------------------------------------------------------------------------------
-- CREATE DELIVERY_BRZ
-- ----------------------------------------------------------------------------------------------------
-- BRONZE LAYER
CREATE OR REPLACE TABLE BRONZE.DELIVERY_BRZ (
    DELIVERY_ID VARCHAR,
    ORDER_ID VARCHAR,
    DELIVERY_AGENT_ID INTEGER,
    DELIVERY_STATUS VARCHAR(50),
    ESTIMATED_TIME VARCHAR,
    CUSTOMER_ADDRESS_ID INTEGER,
    DELIVERY_DATE TIMESTAMP_TZ,

    -- RAW COLUMNS
    DELIVERY_ID_RAW VARCHAR,
    ORDER_ID_RAW VARCHAR,
    DELIVERY_AGENT_ID_RAW VARCHAR,
    DELIVERY_STATUS_RAW VARCHAR,
    ESTIMATED_TIME_RAW VARCHAR,
    CUSTOMER_ADDRESS_ID_RAW VARCHAR,
    DELIVERY_DATE_RAW VARCHAR,

    -- AUDIT COLUMNS
    INGEST_RUN_ID VARCHAR,
    CREATED_AT VARCHAR,
    UPDATED_AT VARCHAR
);
ALTER TABLE BRONZE.DELIVERY_BRZ CLUSTER BY (INGEST_RUN_ID);

-- CREATING SEQUNCE TO GENERATE INGEST_RUN_ID
CREATE OR REPLACE SEQUENCE SEQ_DELIVERY_INGEST_RUN_ID START = 1 INCREMENT = 1;

-- ----------------------------------------------------------------------------------------------------
-- CREATE ORDER_ITEM_LOAD_ERROR
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.DELIVERY_LOAD_ERROR(
    ERROR_ID INTEGER PRIMARY KEY,
    VALIDATE_COLUMN VARCHAR(50),
    VALIDATION_TYPE VARCHAR(30),
    VALIDATION_ERROR_MSG VARCHAR(200),
    INGEST_RUN_ID INTEGER
);

-- ----------------------------------------------------------------------------------------------------
-- CREATE DELIVERY_SLV
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.DELIVERY_SLV (
    DELIVERY_ID VARCHAR,
    ORDER_ID VARCHAR,
    DELIVERY_AGENT_ID INTEGER,
    DELIVERY_STATUS VARCHAR(50),
    ESTIMATED_TIME VARCHAR,
    CUSTOMER_ADDRESS_ID INTEGER,
    DELIVERY_DATE TIMESTAMP_TZ,
    CREATED_AT TIMESTAMP_TZ,
    UPDATED_AT TIMESTAMP_TZ,
    BATCH_ID STRING(50)
);

ALTER TABLE SILVER.DELIVERY_SLV CLUSTER BY (DATE(DELIVERY_DATE));

-- ----------------------------------------------------------------------------------------------------
-- CREATE FACT_DELIVERY
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.FACT_DELIVERY (
    DELIVERY_ID VARCHAR,
    ORDER_ID VARCHAR,
    DELIVERY_AGENT_ID INTEGER,
    DELIVERY_STATUS VARCHAR(50),
    ESTIMATED_TIME VARCHAR,
    CUSTOMER_ADDRESS_ID INTEGER,
    DELIVERY_DATE TIMESTAMP_TZ,
    STATUS VARCHAR DEFAULT 'ACTIVE',
    EFF_START_DT TIMESTAMP_TZ,
    EFF_END_DT TIMESTAMP_TZ,
    BATCH_ID VARCHAR,
    CREATED_AT TIMESTAMP_TZ,
    UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'FACT TABLE FOR DELIVERIES WITH STATUS TRACKING';

ALTER TABLE GOLD.FACT_DELIVERY CLUSTER BY (DATE(DELIVERY_DATE));

-- ----------------------------------------------------------------------------------------------------
-- STATUS HISTORY TABLES (FOR DETAILED AUDIT TRAIL)
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.FACT_DELIVERY_STATUS_HISTORY (
    STATUS_HISTORY_KEY INTEGER PRIMARY KEY AUTOINCREMENT,
    DELIVERY_ID INTEGER,
    OLD_STATUS STRING(50),
    NEW_STATUS STRING(50),
    STATUS_CHANGED_AT TIMESTAMP_TZ,
    BATCH_ID STRING(36)
)
COMMENT = 'AUDIT TRAIL FOR DELIVERY STATUS CHANGES';

-- ----------------------------------------------------------------------------------------------------