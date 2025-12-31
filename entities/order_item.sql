-- ====================================================================================================
-- ORDER_ITEM
-- ====================================================================================================
-- CHANGE_CONTEXT
USE ROLE ACCOUNTADMIN;
USE DATABASE datavelocity;
USE SCHEMA SILVER;
USE WAREHOUSE ADHOC_WH;

-- ----------------------------------------------------------------------------------------------------
-- CREATE ORDER_ITEM_BRZ
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.ORDER_ITEM_BRZ (
    ORDER_ITEM_ID VARCHAR PRIMARY KEY,
    ORDER_ID VARCHAR,
    MENU_ID INTEGER,
    QUANTITY NUMBER(10,2),
    PRICE NUMBER(10,2),
    SUBTOTAL NUMBER(10,2),
    ORDER_TIMESTAMP DATE,

    --RAW COLUMNS
    ORDER_ITEM_ID_RAW VARCHAR,
    ORDER_ID_RAW VARCHAR,
    MENU_ID_RAW VARCHAR,
    QUANTITY_RAW VARCHAR,
    PRICE_RAW VARCHAR,
    SUBTOTAL_RAW VARCHAR,
    ORDER_TIMESTAMP_RAW VARCHAR,

    -- AUDIT COLUMNS
    INGEST_RUN_ID INTEGER,
    CREATED_AT TIMESTAMP_TZ,
    UPDATED_AT TIMESTAMP_TZ
);
ALTER TABLE BRONZE.ORDER_ITEM_BRZ CLUSTER BY (INGEST_RUN_ID);

-- CREATING SEQUNCE TO GENERATE INGEST_RUN_ID
CREATE OR REPLACE SEQUENCE BRONZE.SEQ_ORDER_ITEM_INGEST_RUN_ID START = 1 INCREMENT = 1;

-- ----------------------------------------------------------------------------------------------------
-- CREATE ORDER_ITEM_LOAD_ERROR
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.ORDER_ITEM_LOAD_ERROR(
    ERROR_ID INTEGER PRIMARY KEY,
    VALIDATE_COLUMN VARCHAR(50),
    VALIDATION_TYPE VARCHAR(30),
    VALIDATION_ERROR_MSG VARCHAR(200),
    INGEST_RUN_ID INTEGER
);

SELECT CURRENT_TIMESTAMP();

-- ----------------------------------------------------------------------------------------------------
-- CREATE ORDER_ITEM_SLV
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.ORDER_ITEM_SLV (
    ORDER_ITEM_ID VARCHAR PRIMARY KEY,
    ORDER_ID VARCHAR,
    MENU_ID INTEGER,
    QUANTITY INTEGER,
    PRICE NUMBER(10, 2),
    SUBTOTAL NUMBER(10, 2),
    ORDER_TIMESTAMP TIMESTAMP_TZ,

    -- AUDIT COLUMNS
    BATCH_ID STRING(50),
    CREATED_AT TIMESTAMP_TZ,
    UPDATED_AT TIMESTAMP_TZ
);
ALTER TABLE SILVER.ORDER_ITEM_SLV CLUSTER BY (DAY(ORDER_TIMESTAMP),MONTH(ORDER_TIMESTAMP));

-- ----------------------------------------------------------------------------------------------------
-- CREATE FACT_ORDER_ITEM
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.FACT_ORDER_ITEM (
    ORDER_ITEM_ID VARCHAR PRIMARY KEY,
    ORDER_ID VARCHAR,
    MENU_ID INTEGER,
    QUANTITY INTEGER,
    PRICE NUMBER(10, 2),
    SUBTOTAL NUMBER(10, 2),
    ORDER_TIMESTAMP TIMESTAMP_TZ,

    -- SCD 2 METADATA
    STATUS VARCHAR DEFAULT 'ACTIVE',
    EFF_START_DT TIMESTAMP_TZ,
    EFF_END_DT TIMESTAMP_TZ,
    BATCH_ID VARCHAR,
    CREATED_AT TIMESTAMP_TZ,
    UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'FACT TABLE FOR ORDER ITEMS – APPEND ONLY';
ALTER TABLE GOLD.FACT_ORDER_ITEM CLUSTER BY (DAY(ORDER_TIMESTAMP),MONTH(ORDER_TIMESTAMP));
-- ----------------------------------------------------------------------------------------------------
