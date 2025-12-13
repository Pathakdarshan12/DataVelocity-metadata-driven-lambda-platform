-- ====================================================================================================
-- CUSTOMER
-- ====================================================================================================
-- CHANGE CONTEXT
USE DATABASE SWIGGY;
USE SCHEMA BRONZE;
USE WAREHOUSE ADHOC_WH;

-- ----------------------------------------------------------------------------------------------------
-- CREATE RESTAURANT_BRZ
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.CUSTOMER_BRZ (
    CUSTOMERID TEXT,                    -- PRIMARY KEY AS TEXT
    NAME TEXT,                          -- NAME AS TEXT
    MOBILE TEXT WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),                        -- MOBILE NUMBER AS TEXT
    EMAIL TEXT WITH TAG (COMMON.PII_POLICY_TAG = 'EMAIL'),                         -- EMAIL AS TEXT
    LOGINBYUSING TEXT,                  -- LOGIN METHOD AS TEXT
    GENDER TEXT WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),                        -- GENDER AS TEXT
    DOB TEXT WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),                           -- DATE OF BIRTH AS TEXT
    ANNIVERSARY TEXT,                   -- ANNIVERSARY AS TEXT
    PREFERENCES TEXT,                   -- PREFERENCES AS TEXT
    CREATEDDATE TEXT,                   -- CREATED DATE AS TEXT
    MODIFIEDDATE TEXT,                  -- MODIFIED DATE AS TEXT
    -- AUDIT COLUMNS WITH APPROPRIATE DATA TYPES
    STG_FILE_NAME TEXT,
    STG_FILE_LOAD_TS TIMESTAMP,
    STG_FILE_MD5 TEXT,
    COPY_DATA_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
COMMENT = 'THIS IS THE CUSTOMER STAGE/RAW TABLE WHERE DATA WILL BE COPIED FROM INTERNAL STAGE USING COPY COMMAND. THIS IS AS-IS DATA REPRESETATION FROM THE SOURCE LOCATION. ALL THE COLUMNS ARE TEXT DATA TYPE EXCEPT THE AUDIT COLUMNS THAT ARE ADDED FOR TRACEABILITY.';

-- ----------------------------------------------------------------------------------------------------
-- CREATE CUSTOMER_SLV
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.CUSTOMER_SLV (
    CUSTOMER_SK NUMBER AUTOINCREMENT PRIMARY KEY,                -- AUTO-INCREMENTED PRIMARY KEY
    CUSTOMER_ID STRING NOT NULL,                                 -- CUSTOMER ID
    NAME STRING(100) NOT NULL,                                   -- CUSTOMER NAME
    MOBILE STRING(15)  WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),                                           -- MOBILE NUMBER, ACCOMMODATING INTERNATIONAL FORMAT
    EMAIL STRING(100) WITH TAG (COMMON.PII_POLICY_TAG = 'EMAIL'),                                           -- EMAIL
    LOGIN_BY_USING STRING(50),                                   -- METHOD OF LOGIN (E.G., SOCIAL, GOOGLE, ETC.)
    GENDER STRING(10)  WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),                                           -- GENDER
    DOB DATE WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),                                                    -- DATE OF BIRTH IN DATE FORMAT
    ANNIVERSARY DATE,                                            -- ANNIVERSARY IN DATE FORMAT
    PREFERENCES STRING,                                          -- CUSTOMER PREFERENCES
    CREATED_DT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP,           -- RECORD CREATION TIMESTAMP
    MODIFIED_DT TIMESTAMP_TZ,                                    -- RECORD MODIFICATION TIMESTAMP, ALLOWS NULL IF NOT MODIFIED
    -- ADDITIONAL AUDIT COLUMNS
    STG_FILE_NAME STRING,                                       -- FILE NAME FOR AUDIT
    STG_FILE_LOAD_TS TIMESTAMP_NTZ,                             -- FILE LOAD TIMESTAMP
    STG_FILE_MD5 STRING,                                        -- MD5 HASH FOR FILE CONTENT
    COPY_DATA_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP        -- COPY DATA TIMESTAMP
)
COMMENT = 'CUSTOMER ENTITY UNDER CLEAN SCHEMA WITH APPROPRIATE DATA TYPE UNDER CLEAN SCHEMA LAYER, DATA IS POPULATED USING MERGE STATEMENT FROM THE STAGE LAYER LOCATION TABLE. THIS TABLE DOES NOT SUPPORT SCD2';

-- ----------------------------------------------------------------------------------------------------
-- CREATE CUSTOMER_DIM
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.DIM_CUSTOMER (
    CUSTOMER_HK NUMBER PRIMARY KEY,               -- SURROGATE KEY FOR THE CUSTOMER
    CUSTOMER_ID STRING NOT NULL,                                 -- NATURAL KEY FOR THE CUSTOMER
    NAME STRING(100) NOT NULL,                                   -- CUSTOMER NAME
    MOBILE STRING(15) WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),                                           -- MOBILE NUMBER
    EMAIL STRING(100) WITH TAG (COMMON.PII_POLICY_TAG = 'EMAIL'),                                           -- EMAIL
    LOGIN_BY_USING STRING(50),                                   -- METHOD OF LOGIN
    GENDER STRING(10) WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),                                           -- GENDER
    DOB DATE WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),                                                    -- DATE OF BIRTH
    ANNIVERSARY DATE,                                            -- ANNIVERSARY
    PREFERENCES STRING,                                          -- PREFERENCES
    EFF_START_DATE TIMESTAMP_TZ,                                 -- EFFECTIVE START DATE
    EFF_END_DATE TIMESTAMP_TZ,                                   -- EFFECTIVE END DATE (NULL IF ACTIVE)
    IS_CURRENT BOOLEAN                                           -- FLAG TO INDICATE THE CURRENT RECORD
)
COMMENT = 'CUSTOMER DIMENSION TABLE WITH SCD TYPE 2 HANDLING FOR HISTORICAL TRACKING.';

-- ----------------------------------------------------------------------------------------------------
--  STAGE TO BRONZE
-- ----------------------------------------------------------------------------------------------------
COPY INTO  BRONZE.CUSTOMER_BRZ (CUSTOMERID, NAME, MOBILE, EMAIL, LOGINBYUSING, GENDER, DOB, ANNIVERSARY,
                    PREFERENCES, CREATEDDATE, MODIFIEDDATE,
                    STG_FILE_NAME, STG_FILE_LOAD_TS, STG_FILE_MD5, COPY_DATA_TS)
FROM (
    SELECT
        T.$1::TEXT AS CUSTOMERID,
        T.$2::TEXT AS NAME,
        T.$3::TEXT AS MOBILE,
        T.$4::TEXT AS EMAIL,
        T.$5::TEXT AS LOGINBYUSING,
        T.$6::TEXT AS GENDER,
        T.$7::TEXT AS DOB,
        T.$8::TEXT AS ANNIVERSARY,
        T.$9::TEXT AS PREFERENCES,
        T.$10::TEXT AS CREATEDDATE,
        T.$11::TEXT AS MODIFIEDDATE,
        METADATA$FILENAME AS STG_FILE_NAME,
        METADATA$FILE_LAST_MODIFIED AS STG_FILE_LOAD_TS,
        METADATA$FILE_CONTENT_KEY AS STG_FILE_MD5,
        CURRENT_TIMESTAMP AS COPY_DATA_TS
    FROM '@"SWIGGY"."BRONZE"."CSV_STG"/customer/customers-initial.csv' T
)
FILE_FORMAT = (FORMAT_NAME = 'BRONZE.CSV_FILE_FORMAT')
ON_ERROR = ABORT_STATEMENT;

-- ----------------------------------------------------------------------------------------------------
-- BRONZE TO SILVER
-- ----------------------------------------------------------------------------------------------------
INSERT INTO SILVER.CUSTOMER_SLV (
    CUSTOMER_ID,
    NAME,
    MOBILE,
    EMAIL,
    LOGIN_BY_USING,
    GENDER,
    DOB,
    ANNIVERSARY,
    PREFERENCES,
    CREATED_DT,
    MODIFIED_DT,
    STG_FILE_NAME,
    STG_FILE_LOAD_TS,
    STG_FILE_MD5,
    COPY_DATA_TS
)
SELECT
    CUSTOMERID::STRING,
    NAME::STRING,
    MOBILE::STRING,
    EMAIL::STRING,
    LOGINBYUSING::STRING,
    GENDER::STRING,
    TRY_TO_DATE(DOB, 'YYYY-MM-DD') AS DOB,                     -- CONVERTING DOB TO DATE
    TRY_TO_DATE(ANNIVERSARY, 'YYYY-MM-DD') AS ANNIVERSARY,     -- CONVERTING ANNIVERSARY TO DATE
    PREFERENCES::STRING,
    TRY_TO_TIMESTAMP_TZ(CREATEDDATE, 'YYYY-MM-DD HH24:MI:SS') AS CREATED_DT,  -- TIMESTAMP CONVERSION
    TRY_TO_TIMESTAMP_TZ(MODIFIEDDATE, 'YYYY-MM-DD HH24:MI:SS') AS MODIFIED_DT, -- TIMESTAMP CONVERSION
    STG_FILE_NAME,
    STG_FILE_LOAD_TS,
    STG_FILE_MD5,
    COPY_DATA_TS
FROM BRONZE.CUSTOMER_BRZ;

-- ----------------------------------------------------------------------------------------------------
-- SILVER TO GOLD
-- ----------------------------------------------------------------------------------------------------
MERGE INTO GOLD.CUSTOMER_SLV AS TARGET
USING (
    SELECT
        CUSTOMERID::STRING AS CUSTOMER_ID,
        NAME::STRING AS NAME,
        MOBILE::STRING AS MOBILE,
        EMAIL::STRING AS EMAIL,
        LOGINBYUSING::STRING AS LOGIN_BY_USING,
        GENDER::STRING AS GENDER,
        TRY_TO_DATE(DOB, 'YYYY-MM-DD') AS DOB,
        TRY_TO_DATE(ANNIVERSARY, 'YYYY-MM-DD') AS ANNIVERSARY,
        PREFERENCES::STRING AS PREFERENCES,
        TRY_TO_TIMESTAMP_TZ(CREATEDDATE, 'YYYY-MM-DD"T"HH24:MI:SS.FF6') AS CREATED_DT,
        TRY_TO_TIMESTAMP_TZ(MODIFIEDDATE, 'YYYY-MM-DD"T"HH24:MI:SS.FF6') AS MODIFIED_DT,
        STG_FILE_NAME,
        STG_FILE_LOAD_TS,
        STG_FILE_MD5,
        COPY_DATA_TS
    FROM BRONZE.CUSTOMER_BRZ
) AS SOURCE
ON TARGET.CUSTOMER_ID = SOURCE.CUSTOMER_ID
WHEN MATCHED THEN
    UPDATE SET
        TARGET.NAME = SOURCE.NAME,
        TARGET.MOBILE = SOURCE.MOBILE,
        TARGET.EMAIL = SOURCE.EMAIL,
        TARGET.LOGIN_BY_USING = SOURCE.LOGIN_BY_USING,
        TARGET.GENDER = SOURCE.GENDER,
        TARGET.DOB = SOURCE.DOB,
        TARGET.ANNIVERSARY = SOURCE.ANNIVERSARY,
        TARGET.PREFERENCES = SOURCE.PREFERENCES,
        TARGET.CREATED_DT = SOURCE.CREATED_DT,
        TARGET.MODIFIED_DT = SOURCE.MODIFIED_DT,
        TARGET.STG_FILE_NAME = SOURCE.STG_FILE_NAME,
        TARGET.STG_FILE_LOAD_TS = SOURCE.STG_FILE_LOAD_TS,
        TARGET.STG_FILE_MD5 = SOURCE.STG_FILE_MD5,
        TARGET.COPY_DATA_TS = SOURCE.COPY_DATA_TS
WHEN NOT MATCHED THEN
    INSERT (
        CUSTOMER_ID,
        NAME,
        MOBILE,
        EMAIL,
        LOGIN_BY_USING,
        GENDER,
        DOB,
        ANNIVERSARY,
        PREFERENCES,
        CREATED_DT,
        MODIFIED_DT,
        STG_FILE_NAME,
        STG_FILE_LOAD_TS,
        STG_FILE_MD5,
        COPY_DATA_TS
    )
    VALUES (
        SOURCE.CUSTOMER_ID,
        SOURCE.NAME,
        SOURCE.MOBILE,
        SOURCE.EMAIL,
        SOURCE.LOGIN_BY_USING,
        SOURCE.GENDER,
        SOURCE.DOB,
        SOURCE.ANNIVERSARY,
        SOURCE.PREFERENCES,
        SOURCE.CREATED_DT,
        SOURCE.MODIFIED_DT,
        SOURCE.STG_FILE_NAME,
        SOURCE.STG_FILE_LOAD_TS,
        SOURCE.STG_FILE_MD5,
        SOURCE.COPY_DATA_TS
    );
    -- ====================================================================================================
