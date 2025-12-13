-- ====================================================================================================
-- ORDER_ITEM
-- ====================================================================================================
-- CHANGE_CONTEXT
USE ROLE ACCOUNTADMIN;
USE DATABASE SWIGGY;
USE SCHEMA SILVER;
USE WAREHOUSE ADHOC_WH;

-- ----------------------------------------------------------------------------------------------------
-- CREATE ORDER_ITEM_BRZ
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.ORDER_ITEM_BRZ (
    ORDER_ITEM_ID TEXT COMMENT 'PRIMARY KEY (SOURCE SYSTEM)',              -- PRIMARY KEY AS TEXT
    ORDER_ID TEXT COMMENT 'ORDER FK(SOURCE SYSTEM)',                  -- FOREIGN KEY REFERENCE AS TEXT (NO CONSTRAINT IN SNOWFLAKE)
    MENU_ID TEXT COMMENT 'MENU FK(SOURCE SYSTEM)',                   -- FOREIGN KEY REFERENCE AS TEXT (NO CONSTRAINT IN SNOWFLAKE)
    QUANTITY TEXT,                 -- QUANTITY AS TEXT
    PRICE TEXT,                    -- PRICE AS TEXT (NO DECIMAL CONSTRAINT)
    SUBTOTAL TEXT,                 -- SUBTOTAL AS TEXT (NO DECIMAL CONSTRAINT)
    CREATEDDATE TEXT,              -- CREATED DATE AS TEXT
    MODIFIEDDATE TEXT,             -- MODIFIED DATE AS TEXT

    -- AUDIT COLUMNS WITH APPROPRIATE DATA TYPES
    STG_FILE_NAME TEXT,
    STG_FILE_LOAD_TS TIMESTAMP,
    STG_FILE_MD5 TEXT,
    COPY_DATA_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
COMMENT = 'THIS IS THE ORDER ITEM STAGE/RAW TABLE WHERE DATA WILL BE COPIED FROM INTERNAL STAGE USING COPY COMMAND. THIS IS AS-IS DATA REPRESETATION FROM THE SOURCE LOCATION. ALL THE COLUMNS ARE TEXT DATA TYPE EXCEPT THE AUDIT COLUMNS THAT ARE ADDED FOR TRACEABILITY.';

-- ----------------------------------------------------------------------------------------------------
-- CREATE ORDER_ITEM_SLV
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.ORDER_ITEM_SLV (
    ORDER_ITEM_SK NUMBER AUTOINCREMENT PRIMARY KEY COMMENT 'SURROGATE KEY (EDW)',    -- AUTO-INCREMENTED UNIQUE IDENTIFIER FOR EACH ORDER ITEM
    ORDER_ITEM_ID NUMBER  NOT NULL UNIQUE COMMENT 'PRIMARY KEY (SOURCE SYSTEM)',
    ORDER_ID_FK NUMBER  NOT NULL COMMENT 'ORDER FK(SOURCE SYSTEM)',                  -- FOREIGN KEY REFERENCE FOR ORDER ID
    MENU_ID_FK NUMBER  NOT NULL COMMENT 'MENU FK(SOURCE SYSTEM)',                   -- FOREIGN KEY REFERENCE FOR MENU ID
    QUANTITY NUMBER(10, 2),                 -- QUANTITY AS A DECIMAL NUMBER
    PRICE NUMBER(10, 2),                    -- PRICE AS A DECIMAL NUMBER
    SUBTOTAL NUMBER(10, 2),                 -- SUBTOTAL AS A DECIMAL NUMBER
    CREATED_DT TIMESTAMP,                 -- CREATED DATE OF THE ORDER ITEM
    MODIFIED_DT TIMESTAMP,                -- MODIFIED DATE OF THE ORDER ITEM
    -- AUDIT COLUMNS
    STG_FILE_NAME VARCHAR(255),            -- FILE NAME OF THE STAGING FILE
    STG_FILE_LOAD_TS TIMESTAMP,            -- TIMESTAMP WHEN THE FILE WAS LOADED
    STG_FILE_MD5 VARCHAR(255),             -- MD5 HASH OF THE FILE FOR INTEGRITY CHECK
    COPY_DATA_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- TIMESTAMP WHEN DATA IS COPIED INTO THE CLEAN LAYER
)
COMMENT = 'ORDER ITEM ENTITY UNDER CLEAN SCHEMA WITH APPROPRIATE DATA TYPE UNDER CLEAN SCHEMA LAYER, DATA IS POPULATED USING MERGE STATEMENT FROM THE STAGE LAYER LOCATION TABLE. THIS TABLE DOES NOT SUPPORT SCD2';

-- ----------------------------------------------------------------------------------------------------
-- CREATE DIM_ORDER_ITEM
-- ----------------------------------------------------------------------------------------------------

-- ----------------------------------------------------------------------------------------------------
-- STAGE TO BRONZE
-- ----------------------------------------------------------------------------------------------------
COPY INTO SILVER.ORDER_ITEM_SLV (
    ORDER_ITEM_ID,
    ORDER_ID_FK,
    MENU_ID_FK,
    QUANTITY,
    PRICE,
    SUBTOTAL,
    CREATED_DT,
    MODIFIED_DT,
    STG_FILE_NAME,
    STG_FILE_LOAD_TS,
    STG_FILE_MD5,
    COPY_DATA_TS
)
FROM (
    SELECT
        T.$1::TEXT AS ORDER_ITEM_ID,
        T.$2::TEXT AS ORDER_ID_FK,
        T.$3::TEXT AS MENU_ID_FK,
        T.$4::NUMBER AS QUANTITY,
        T.$5::NUMBER(10,2) AS PRICE,
        T.$6::NUMBER(10,2) AS SUBTOTAL,
        T.$7::TIMESTAMP AS CREATED_DT,
        T.$8::TIMESTAMP AS MODIFIED_DT,
        METADATA$FILENAME AS STG_FILE_NAME,
        METADATA$FILE_LAST_MODIFIED AS STG_FILE_LOAD_TS,
        METADATA$FILE_CONTENT_KEY AS STG_FILE_MD5,
        CURRENT_TIMESTAMP AS COPY_DATA_TS
    FROM @"SWIGGY"."BRONZE"."CSV_STG"/order_item/order-item-initial-v2.csv T
)
FILE_FORMAT = (FORMAT_NAME = 'BRONZE.CSV_FILE_FORMAT')
ON_ERROR = ABORT_STATEMENT;


-- ----------------------------------------------------------------------------------------------------
-- BRONZE TO SILVER
-- ----------------------------------------------------------------------------------------------------
MERGE INTO SILVER.ORDER_ITEM_SLV AS TARGET
USING BRONZE.ORDER_ITEM_BRZ AS SOURCE
ON
    TARGET.ORDER_ITEM_ID = SOURCE.ORDER_ITEM_ID AND
    TARGET.ORDER_ID_FK = SOURCE.ORDER_ID AND
    TARGET.MENU_ID_FK = SOURCE.MENU_ID
WHEN MATCHED THEN
    -- UPDATE THE EXISTING RECORD WITH NEW DATA
    UPDATE SET
        TARGET.QUANTITY = SOURCE.QUANTITY,
        TARGET.PRICE = SOURCE.PRICE,
        TARGET.SUBTOTAL = SOURCE.SUBTOTAL,
        TARGET.CREATED_DT = SOURCE.CREATEDDATE,
        TARGET.MODIFIED_DT = SOURCE.MODIFIEDDATE,
        TARGET.STG_FILE_NAME = SOURCE.STG_FILE_NAME,
        TARGET.STG_FILE_LOAD_TS = SOURCE.STG_FILE_LOAD_TS,
        TARGET.STG_FILE_MD5 = SOURCE.STG_FILE_MD5,
        TARGET.COPY_DATA_TS = SOURCE.COPY_DATA_TS
WHEN NOT MATCHED THEN
    -- INSERT NEW RECORD IF NO MATCH IS FOUND
    INSERT (
        ORDER_ITEM_ID,
        ORDER_ID_FK,
        MENU_ID_FK,
        QUANTITY,
        PRICE,
        SUBTOTAL,
        CREATED_DT,
        MODIFIED_DT,
        STG_FILE_NAME,
        STG_FILE_LOAD_TS,
        STG_FILE_MD5,
        COPY_DATA_TS
    )
    VALUES (
        SOURCE.ORDER_ITEM_ID,
        SOURCE.ORDER_ID,
        SOURCE.MENU_ID,
        SOURCE.QUANTITY,
        SOURCE.PRICE,
        SOURCE.SUBTOTAL,
        SOURCE.CREATEDDATE,
        SOURCE.MODIFIEDDATE,
        SOURCE.STG_FILE_NAME,
        SOURCE.STG_FILE_LOAD_TS,
        SOURCE.STG_FILE_MD5,
        CURRENT_TIMESTAMP()
    );

-- ----------------------------------------------------------------------------------------------------
-- SILVER TO GOLD
-- ----------------------------------------------------------------------------------------------------
-- ====================================================================================================