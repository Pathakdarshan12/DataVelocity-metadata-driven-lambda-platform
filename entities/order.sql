-- ==============================================================================================================================================================
-- ORDER
-- ==============================================================================================================================================================
-- CHANGE CONTEXT
USE DATABASE DATAVELOCITY;
USE SCHEMA BRONZE;
USE WAREHOUSE ADHOC_WH;

-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
-- CREATE ORDER_BRZ
-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.ORDER_BRZ (
    ORDER_ID VARCHAR PRIMARY KEY COMMENT 'PRIMARY KEY (SOURCE SYSTEM)',
    CUSTOMER_ID VARCHAR COMMENT 'CUSTOMER FK(SOURCE SYSTEM)',
    RESTAURANT_ID INTEGER COMMENT 'RESTAURANT FK(SOURCE SYSTEM)',
    ORDER_DATE DATE,
    TOTAL_AMOUNT NUMBER(10, 2),
    ORDER_STATUS VARCHAR(20),
    PAYMENT_METHOD VARCHAR,

    -- RAW_COLUMNS
    ORDER_ID_RAW VARCHAR,
    CUSTOMER_ID_RAW VARCHAR,
    RESTAURANT_ID_RAW VARCHAR,
    ORDER_DATE_RAW VARCHAR,
    TOTAL_AMOUNT_RAW VARCHAR,
    ORDER_STATUS_RAW VARCHAR,
    PAYMENT_METHOD_RAW VARCHAR,

    -- AUDIT COLUMNS
    INGEST_RUN_ID INTEGER,
    CREATED_AT VARCHAR,
    UPDATED_AT VARCHAR
);
ALTER TABLE BRONZE.ORDER_BRZ CLUSTER BY (INGEST_RUN_ID);

-- CREATING SEQUNCE TO GENERATE INGEST_RUN_ID
CREATE OR REPLACE SEQUENCE BRONZE.SEQ_ORDER_INGEST_RUN_ID START = 1 INCREMENT = 1;

-- ----------------------------------------------------------------------------------------------------
-- CREATE ORDER_LOAD_ERROR
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.ORDER_LOAD_ERROR(
    ERROR_ID INTEGER PRIMARY KEY,
    VALIDATE_COLUMN VARCHAR(50),
    VALIDATION_TYPE VARCHAR(30),
    VALIDATION_ERROR_MSG VARCHAR(200),
    INGEST_RUN_ID INTEGER
);

-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
-- CREATE ORDER_SLV
-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.ORDER_SLV (
    ORDER_ID VARCHAR PRIMARY KEY COMMENT 'PRIMARY KEY (SOURCE SYSTEM)',
    CUSTOMER_ID VARCHAR COMMENT 'CUSTOMER FK(SOURCE SYSTEM)',
    RESTAURANT_ID INTEGER COMMENT 'RESTAURANT FK(SOURCE SYSTEM)',
    ORDER_DATE TIMESTAMP_TZ,
    TOTAL_AMOUNT NUMBER(10, 2),
    ORDER_STATUS VARCHAR(20),
    PAYMENT_METHOD STRING(50),

    -- AUDIT COLUMNS
    BATCH_ID VARCHAR,
    CREATED_AT TIMESTAMP_TZ,
    UPDATED_AT TIMESTAMP_TZ
);

-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
-- CREATE FACT_ORDER
-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.FACT_ORDER (
    ORDER_ID VARCHAR PRIMARY KEY COMMENT 'BUSINESS KEY (SOURCE SYSTEM)',
    CUSTOMER_ID VARCHAR COMMENT 'CUSTOMER FK(SOURCE SYSTEM)',
    RESTAURANT_ID INTEGER COMMENT 'RESTAURANT FK(SOURCE SYSTEM)',
    ORDER_DATE TIMESTAMP_TZ,
    TOTAL_AMOUNT NUMBER(10, 2),
    ORDER_STATUS VARCHAR(20),
    PAYMENT_METHOD STRING(50),

    -- SCD 2 METADATA
    STATUS VARCHAR DEFAULT 'ACTIVE',
    EFF_START_DT TIMESTAMP_TZ,
    EFF_END_DT TIMESTAMP_TZ,
    BATCH_ID VARCHAR,
    CREATED_AT TIMESTAMP_TZ,
    UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'FACT TABLE FOR ORDER WITH ORDER_STATUS TRACKING';

-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
-- ORDER_STATUS HISTORY TABLES (DETAILED AUDIT TRAIL)
-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.FACT_ORDER_ORDER_STATUS_HISTORY (
    ORDER_STATUS_HISTORY_KEY INTEGER PRIMARY KEY AUTOINCREMENT,
    ORDER_ID VARCHAR,  -- Changed from INTEGER to VARCHAR to match FACT_ORDER
    OLD_ORDER_STATUS VARCHAR(20),
    NEW_ORDER_STATUS VARCHAR(20),
    ORDER_STATUS_CHANGED_AT TIMESTAMP_TZ,
    BATCH_ID VARCHAR
)
COMMENT = 'AUDIT TRAIL FOR ORDER_STATUS CHANGES';

-- ==============================================================================================================================================================