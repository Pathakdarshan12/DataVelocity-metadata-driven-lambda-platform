-- ====================================================================================================
-- ORDER
-- ====================================================================================================
-- CHANGE CONTEXT
USE DATABASE SWIGGY;
USE SCHEMA BRONZE;
USE WAREHOUSE ADHOC_WH;

-- ----------------------------------------------------------------------------------------------------
-- CREATE ORDER_BRZ
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.ORDERS_BRZ (
    ORDER_ID TEXT COMMENT 'PRIMARY KEY (SOURCE SYSTEM)',                  -- PRIMARY KEY AS TEXT
    CUSTOMER_ID TEXT COMMENT 'CUSTOMER FK(SOURCE SYSTEM)',               -- FOREIGN KEY REFERENCE AS TEXT (NO CONSTRAINT IN SNOWFLAKE)
    RESTAURANT_ID TEXT COMMENT 'RESTAURANT FK(SOURCE SYSTEM)',             -- FOREIGN KEY REFERENCE AS TEXT (NO CONSTRAINT IN SNOWFLAKE)
    ORDERDATE TEXT,                -- ORDER DATE AS TEXT
    TOTALAMOUNT TEXT,              -- TOTAL AMOUNT AS TEXT (NO DECIMAL CONSTRAINT)
    STATUS TEXT,                   -- STATUS AS TEXT
    PAYMENTMETHOD TEXT,            -- PAYMENT METHOD AS TEXT
    CREATEDDATE TEXT,              -- CREATED DATE AS TEXT
    MODIFIEDDATE TEXT,             -- MODIFIED DATE AS TEXT
    -- AUDIT COLUMNS WITH APPROPRIATE DATA TYPES
    STG_FILE_NAME TEXT,
    STG_FILE_LOAD_TS TIMESTAMP,
    STG_FILE_MD5 TEXT,
    COPY_DATA_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
COMMENT = 'THIS IS THE ORDER STAGE/RAW TABLE WHERE DATA WILL BE COPIED FROM INTERNAL STAGE USING COPY COMMAND. THIS IS AS-IS DATA REPRESETATION FROM THE SOURCE LOCATION. ALL THE COLUMNS ARE TEXT DATA TYPE EXCEPT THE AUDIT COLUMNS THAT ARE ADDED FOR TRACEABILITY.';

-- ----------------------------------------------------------------------------------------------------
-- CREATE ORDER_SLV
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.ORDERS_SLV (
    ORDER_SK NUMBER AUTOINCREMENT PRIMARY KEY COMMENT 'SURROGATE KEY (EDW)',                -- AUTO-INCREMENTED PRIMARY KEY
    ORDER_ID BIGINT UNIQUE COMMENT 'PRIMARY KEY (SOURCE SYSTEM)',                      -- PRIMARY KEY INFERRED AS BIGINT
    CUSTOMER_ID_FK BIGINT COMMENT 'CUSTOMER FK(SOURCE SYSTEM)',                   -- FOREIGN KEY INFERRED AS BIGINT
    RESTAURANT_ID_FK BIGINT COMMENT 'RESTAURANT FK(SOURCE SYSTEM)',                 -- FOREIGN KEY INFERRED AS BIGINT
    ORDER_DATE TIMESTAMP,                 -- ORDER DATE INFERRED AS TIMESTAMP
    TOTAL_AMOUNT DECIMAL(10, 2),          -- TOTAL AMOUNT INFERRED AS DECIMAL WITH TWO DECIMAL PLACES
    STATUS STRING,                        -- STATUS AS STRING
    PAYMENT_METHOD STRING,                -- PAYMENT METHOD AS STRING
    CREATED_DT TIMESTAMP_TZ,                                     -- RECORD CREATION DATE
    MODIFIED_DT TIMESTAMP_TZ,                                    -- LAST MODIFIED DATE, ALLOWS NULL IF NOT MODIFIED
    -- ADDITIONAL AUDIT COLUMNS
    STG_FILE_NAME STRING,                                       -- FILE NAME FOR AUDIT
    STG_FILE_LOAD_TS TIMESTAMP_NTZ,                             -- FILE LOAD TIMESTAMP FOR AUDIT
    STG_FILE_MD5 STRING,                                        -- MD5 HASH FOR FILE CONTENT FOR AUDIT
    COPY_DATA_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP        -- TIMESTAMP WHEN DATA IS COPIED, DEFAULTS TO CURRENT TIMESTAMP
)
COMMENT = 'ORDER ENTITY UNDER CLEAN SCHEMA WITH APPROPRIATE DATA TYPE UNDER CLEAN SCHEMA LAYER, DATA IS POPULATED USING MERGE STATEMENT FROM THE STAGE LAYER LOCATION TABLE. THIS TABLE DOES NOT SUPPORT SCD2';

-- ----------------------------------------------------------------------------------------------------
-- CREATE DIM_ORDER
-- ----------------------------------------------------------------------------------------------------

-- ----------------------------------------------------------------------------------------------------
-- STAGE TO BRONZE
-- ----------------------------------------------------------------------------------------------------
COPY INTO BRONZE.ORDERS_BRZ (ORDER_ID, CUSTOMER_ID, RESTAURANT_ID, ORDERDATE, TOTALAMOUNT,
                  STATUS, PAYMENTMETHOD, CREATEDDATE, MODIFIEDDATE,
                  STG_FILE_NAME, STG_FILE_LOAD_TS, STG_FILE_MD5, COPY_DATA_TS)
FROM (
    SELECT
        T.$1::TEXT AS ORDER_ID,
        T.$2::TEXT AS CUSTOMER_ID,
        T.$3::TEXT AS RESTAURANT_ID,
        T.$4::TEXT AS ORDERDATE,
        T.$5::TEXT AS TOTALAMOUNT,
        T.$6::TEXT AS STATUS,
        T.$7::TEXT AS PAYMENTMETHOD,
        T.$8::TEXT AS CREATEDDATE,
        T.$9::TEXT AS MODIFIEDDATE,
        METADATA$FILENAME AS STG_FILE_NAME,
        METADATA$FILE_LAST_MODIFIED AS STG_FILE_LOAD_TS,
        METADATA$FILE_CONTENT_KEY AS STG_FILE_MD5,
        CURRENT_TIMESTAMP AS COPY_DATA_TS
    FROM '@"SWIGGY"."BRONZE"."CSV_STG"/order/orders-initial.csv' T
)
FILE_FORMAT = (FORMAT_NAME = 'BRONZE.CSV_FILE_FORMAT')
ON_ERROR = ABORT_STATEMENT;

-- ----------------------------------------------------------------------------------------------------
-- BRONZE TO SILVER
-- ----------------------------------------------------------------------------------------------------
MERGE INTO SILVER.ORDERS_SLV AS TARGET
USING BRONZE.ORDERS_BRZ AS SOURCE
    ON TARGET.ORDER_ID = TRY_TO_NUMBER(SOURCE.ORDER_ID) -- MATCH BASED ON ORDER_ID
WHEN MATCHED THEN
    -- UPDATE EXISTING RECORDS
    UPDATE SET
        TOTAL_AMOUNT = TRY_TO_DECIMAL(SOURCE.TOTALAMOUNT),
        STATUS = SOURCE.STATUS,
        PAYMENT_METHOD = SOURCE.PAYMENTMETHOD,
        MODIFIED_DT = TRY_TO_TIMESTAMP_TZ(SOURCE.MODIFIEDDATE),
        STG_FILE_NAME = SOURCE.STG_FILE_NAME,
        STG_FILE_LOAD_TS = SOURCE.STG_FILE_LOAD_TS,
        STG_FILE_MD5 = SOURCE.STG_FILE_MD5,
        COPY_DATA_TS = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    -- INSERT NEW RECORDS
    INSERT (
        ORDER_ID,
        CUSTOMER_ID_FK,
        RESTAURANT_ID_FK,
        ORDER_DATE,
        TOTAL_AMOUNT,
        STATUS,
        PAYMENT_METHOD,
        CREATED_DT,
        MODIFIED_DT,
        STG_FILE_NAME,
        STG_FILE_LOAD_TS,
        STG_FILE_MD5,
        COPY_DATA_TS
    )
    VALUES (
        TRY_TO_NUMBER(SOURCE.ORDER_ID),
        TRY_TO_NUMBER(SOURCE.CUSTOMER_ID),
        TRY_TO_NUMBER(SOURCE.RESTAURANT_ID),
        TRY_TO_TIMESTAMP(SOURCE.ORDERDATE),
        TRY_TO_DECIMAL(SOURCE.TOTALAMOUNT),
        SOURCE.STATUS,
        SOURCE.PAYMENTMETHOD,
        TRY_TO_TIMESTAMP_TZ(SOURCE.CREATEDDATE),
        TRY_TO_TIMESTAMP_TZ(SOURCE.MODIFIEDDATE),
        SOURCE.STG_FILE_NAME,
        SOURCE.STG_FILE_LOAD_TS,
        SOURCE.STG_FILE_MD5,
        CURRENT_TIMESTAMP
    );

-- ----------------------------------------------------------------------------------------------------
-- SILVER TO GOLD
-- ----------------------------------------------------------------------------------------------------

-- ====================================================================================================