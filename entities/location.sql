-- ====================================================================================================
-- Location
-- ====================================================================================================
-- CHANGE_CONTEXT
USE DATABASE DATAVELOCITY;
USE SCHEMA BRONZE;
USE WAREHOUSE ADHOC_WH;
-- ====================================================================================================
-- CREATE LOCATION_BRZ
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.LOCATION_BRZ (
    LOCATION_BRZ_ID INTEGER AUTOINCREMENT,
    CITY VARCHAR,
    STATE VARCHAR,
    ZIP_CODE VARCHAR,

    -- RAW COLUMNS
    CITY_RAW VARCHAR,
    STATE_RAW VARCHAR,
    ZIP_CODE_RAW VARCHAR,

    INGEST_RUN_ID INTEGER,
    CREATED_AT TIMESTAMP_TZ(9)
);
ALTER TABLE BRONZE.LOCATION_BRZ CLUSTER BY (INGEST_RUN_ID);

-- CREATING SEQUNCE TO GENERATE INGEST_RUN_ID
CREATE OR REPLACE SEQUENCE SEQ_LOCATION_INGEST_RUN_ID START = 1 INCREMENT = 1;

-- ----------------------------------------------------------------------------------------------------
-- CREATE LOCATION_LOAD_ERROR
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.LOCATION_LOAD_ERROR(
    ERROR_ID INTEGER PRIMARY KEY,
    VALIDATE_COLUMN VARCHAR(50),
    VALIDATION_TYPE VARCHAR(30),
    VALIDATION_ERROR_MSG VARCHAR(200),
    INGEST_RUN_ID INTEGER
);

-- ----------------------------------------------------------------------------------------------------
-- CREATING LOCATION_SLV
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.LOCATION_SLV (
    LOCATION_SLV_ID INTEGER AUTOINCREMENT PRIMARY KEY,
    CITY VARCHAR(100) ,
    STATE VARCHAR(100) ,
    STATE_CODE VARCHAR(2),
    IS_UNION_TERRITORY BOOLEAN DEFAULT FALSE,
    CAPITAL_CITY_FLAG BOOLEAN DEFAULT FALSE,
    CITY_TIER VARCHAR(6),
    ZIP_CODE VARCHAR(10),
    STATUS VARCHAR(10),
    BATCH_ID VARCHAR(36),
    CREATED_AT TIMESTAMP_TZ(9),
    UPDATED_AT TIMESTAMP_TZ(9)
)
COMMENT = 'LOCATION ENTITY UNDER CLEAN SCHEMA WITH APPROPRIATE DATA TYPE UNDER CLEAN SCHEMA LAYER, DATA IS POPULATED USING MERGE STATEMENT FROM THE STAGE LAYER LOCATION TABLE. THIS TABLE DOES NOT SUPPORT SCD2';

ALTER TABLE SILVER.LOCATION_SLV CLUSTER BY (STATE);
-- ----------------------------------------------------------------------------------------------------
-- CREAING DIM_LOCATION
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.DIM_LOCATION (
    LOCATION_ID INTEGER PRIMARY KEY,
    CITY VARCHAR(100) NOT NULL,
    STATE VARCHAR(100) NOT NULL,
    STATE_CODE VARCHAR(2),
    IS_UNION_TERRITORY BOOLEAN DEFAULT FALSE,
    CAPITAL_CITY_FLAG BOOLEAN DEFAULT FALSE,
    CITY_TIER VARCHAR(6),
    ZIP_CODE VARCHAR(10) NOT NULL,
    STATUS VARCHAR(10) DEFAULT 'ACTIVE',
    EFF_START_DT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP(),
    EFF_END_DT TIMESTAMP_TZ(9) DEFAULT '9999-12-31 23:59:59'::TIMESTAMP_TZ,
    BATCH_ID VARCHAR(36) NOT NULL,
    CREATED_AT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP()
);

ALTER TABLE GOLD.DIM_LOCATION CLUSTER BY (STATE);
-- ====================================================================================================