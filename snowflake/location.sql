-- ====================================================================================================
-- Location
-- ====================================================================================================
-- CHANGE_CONTEXT
USE ROLE ACCOUNTADMIN;
USE DATABASE SWIGGY;
USE SCHEMA BRONZE;
USE WAREHOUSE ADHOC_WH;
-- ====================================================================================================
-- CREATE LOCATION_BRZ
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.LOCATION_BRZ (
    LOCATIONID TEXT,
    CITY TEXT,
    STATE TEXT,
    ZIPCODE TEXT,
    ACTIVEFLAG TEXT,
    CREATEDDATE TEXT,
    MODIFIEDDATE TEXT,
    -- AUDIT COLUMNS FOR TRACKING & DEBUGGING
    STG_FILE_NAME TEXT,
    STG_FILE_LOAD_TS TIMESTAMP,
    STG_FILE_MD5 TEXT,
    COPY_DATA_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
COMMENT = 'THIS IS THE LOCATION STAGE/RAW TABLE WHERE DATA WILL BE COPIED FROM INTERNAL STAGE USING COPY COMMAND. THIS IS AS-IS DATA REPRESETATION FROM THE SOURCE LOCATION. ALL THE COLUMNS ARE TEXT DATA TYPE EXCEPT THE AUDIT COLUMNS THAT ARE ADDED FOR TRACEABILITY.';

-- ----------------------------------------------------------------------------------------------------
-- CREATING LOCATION_SLV
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.LOCATION_SLV (
    LOCATION_SLV_SK NUMBER AUTOINCREMENT PRIMARY KEY,
    LOCATION_ID NUMBER NOT NULL UNIQUE,
    CITY STRING(100) NOT NULL,
    STATE STRING(100) NOT NULL,
    STATE_CODE STRING(2) NOT NULL,
    IS_UNION_TERRITORY BOOLEAN NOT NULL DEFAULT FALSE,
    CAPITAL_CITY_FLAG BOOLEAN NOT NULL DEFAULT FALSE,
    CITY_TIER TEXT(6),
    ZIP_CODE STRING(10) NOT NULL,
    ACTIVE_FLAG STRING(10) NOT NULL,
    CREATED_TS TIMESTAMP_TZ NOT NULL,
    MODIFIED_TS TIMESTAMP_TZ,
    -- ADDITIONAL AUDIT COLUMNS
    STG_FILE_NAME STRING,
    STG_FILE_LOAD_TS TIMESTAMP_NTZ,
    STG_FILE_MD5 STRING,
    COPY_DATA_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
)
COMMENT = 'LOCATION ENTITY UNDER CLEAN SCHEMA WITH APPROPRIATE DATA TYPE UNDER CLEAN SCHEMA LAYER, DATA IS POPULATED USING MERGE STATEMENT FROM THE STAGE LAYER LOCATION TABLE. THIS TABLE DOES NOT SUPPORT SCD2';

-- ----------------------------------------------------------------------------------------------------
-- CREAING DIM_LOCATION
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.DIM_LOCATION (
    LOCATION_HK NUMBER PRIMARY KEY,                 -- HASH KEY FOR THE DIMENSION
    LOCATION_ID NUMBER(38,0) NOT NULL,                  -- BUSINESS KEY
    CITY VARCHAR(100) NOT NULL,                         -- CITY
    STATE VARCHAR(100) NOT NULL,                        -- STATE
    STATE_CODE VARCHAR(2) NOT NULL,                     -- STATE CODE
    IS_UNION_TERRITORY BOOLEAN NOT NULL DEFAULT FALSE,  -- UNION TERRITORY FLAG
    CAPITAL_CITY_FLAG BOOLEAN NOT NULL DEFAULT FALSE,   -- CAPITAL CITY FLAG
    CITY_TIER VARCHAR(6),                               -- CITY TIER
    ZIP_CODE VARCHAR(10) NOT NULL,                      -- ZIP CODE
    ACTIVE_FLAG VARCHAR(10) NOT NULL,                   -- ACTIVE FLAG (INDICATING CURRENT RECORD)
    EFF_START_DT TIMESTAMP_TZ(9) NOT NULL,              -- EFFECTIVE START DATE FOR SCD2
    EFF_END_DT TIMESTAMP_TZ(9),                         -- EFFECTIVE END DATE FOR SCD2
    CURRENT_FLAG BOOLEAN NOT NULL DEFAULT TRUE          -- INDICATOR OF THE CURRENT RECORD
)
COMMENT = 'DIMENSION TABLE FOR RESTAURANT LOCATION WITH SCD2 (SLOWLY CHANGING DIMENSION) ENABLED AND HASHKEY AS SURROGATE KEY';

-- ----------------------------------------------------------------------------------------------------
-- STAGE TO BRONZE
-- ----------------------------------------------------------------------------------------------------
COPY INTO BRONZE.LOCATION_BRZ (LOCATIONID, CITY, STATE, ZIPCODE, ACTIVEFLAG, CREATEDDATE, MODIFIEDDATE, STG_FILE_NAME, STG_FILE_LOAD_TS, STG_FILE_MD5, COPY_DATA_TS)
FROM (
    SELECT
        T.$1::TEXT AS LOCATIONID,
        T.$2::TEXT AS CITY,
        T.$3::TEXT AS STATE,
        T.$4::TEXT AS ZIPCODE,
        T.$5::TEXT AS ACTIVEFLAG,
        T.$6::TEXT AS CREATEDDATE,
        T.$7::TEXT AS MODIFIEDDATE,
        METADATA$FILENAME AS STG_FILE_NAME,
        METADATA$FILE_LAST_MODIFIED AS STG_FILE_LOAD_TS,
        METADATA$FILE_CONTENT_KEY AS STG_FILE_MD5,
        CURRENT_TIMESTAMP AS COPY_DATA_TS
    FROM '@"SWIGGY"."BRONZE"."CSV_STG"/location/location-5rows.csv' T
)
FILE_FORMAT = (FORMAT_NAME = 'BRONZE.CSV_FILE_FORMAT')
ON_ERROR = ABORT_STATEMENT;

-- ----------------------------------------------------------------------------------------------------
-- BRONZE TO SILVER
-- ----------------------------------------------------------------------------------------------------
MERGE INTO SILVER.LOCATION_SLV AS TARGET
USING (
    SELECT
        CAST(LOCATIONID AS NUMBER) AS LOCATION_ID,
        CAST(CITY AS STRING) AS CITY,
        CASE
            WHEN CAST(STATE AS STRING) = 'DELHI' THEN 'NEW DELHI'
            ELSE CAST(STATE AS STRING)
        END AS STATE,
        -- STATE CODE MAPPING
        CASE
            WHEN UPPER(STATE) = 'DELHI' THEN 'DL'
            WHEN UPPER(STATE) = 'MAHARASHTRA' THEN 'MH'
            WHEN UPPER(STATE) = 'UTTAR PRADESH' THEN 'UP'
            WHEN UPPER(STATE) = 'GUJARAT' THEN 'GJ'
            WHEN UPPER(STATE) = 'RAJASTHAN' THEN 'RJ'
            WHEN UPPER(STATE) = 'KERALA' THEN 'KL'
            WHEN UPPER(STATE) = 'PUNJAB' THEN 'PB'
            WHEN UPPER(STATE) = 'KARNATAKA' THEN 'KA'
            WHEN UPPER(STATE) = 'MADHYA PRADESH' THEN 'MP'
            WHEN UPPER(STATE) = 'ODISHA' THEN 'OR'
            WHEN UPPER(STATE) = 'CHANDIGARH' THEN 'CH'
            WHEN UPPER(STATE) = 'WEST BENGAL' THEN 'WB'
            WHEN UPPER(STATE) = 'SIKKIM' THEN 'SK'
            WHEN UPPER(STATE) = 'ANDHRA PRADESH' THEN 'AP'
            WHEN UPPER(STATE) = 'ASSAM' THEN 'AS'
            WHEN UPPER(STATE) = 'JAMMU AND KASHMIR' THEN 'JK'
            WHEN UPPER(STATE) = 'PUDUCHERRY' THEN 'PY'
            WHEN UPPER(STATE) = 'UTTARAKHAND' THEN 'UK'
            WHEN UPPER(STATE) = 'HIMACHAL PRADESH' THEN 'HP'
            WHEN UPPER(STATE) = 'TAMIL NADU' THEN 'TN'
            WHEN UPPER(STATE) = 'GOA' THEN 'GA'
            WHEN UPPER(STATE) = 'TELANGANA' THEN 'TG'
            WHEN UPPER(STATE) = 'CHHATTISGARH' THEN 'CG'
            WHEN UPPER(STATE) = 'JHARKHAND' THEN 'JH'
            WHEN UPPER(STATE) = 'BIHAR' THEN 'BR'
            ELSE NULL
        END AS STATE_CODE,
        CASE
            WHEN STATE IN ('DELHI', 'CHANDIGARH', 'PUDUCHERRY', 'JAMMU AND KASHMIR') THEN 'Y'
            ELSE 'N'
        END AS IS_UNION_TERRITORY,
        CASE
            WHEN (STATE = 'DELHI' AND CITY = 'NEW DELHI') THEN TRUE
            WHEN (STATE = 'MAHARASHTRA' AND CITY = 'MUMBAI') THEN TRUE
            -- OTHER CONDITIONS FOR CAPITAL CITIES
            ELSE FALSE
        END AS CAPITAL_CITY_FLAG,
        CASE
            WHEN CITY IN ('MUMBAI', 'DELHI', 'BENGALURU', 'HYDERABAD', 'CHENNAI', 'KOLKATA', 'PUNE', 'AHMEDABAD') THEN 'TIER-1'
            WHEN CITY IN ('JAIPUR', 'LUCKNOW', 'KANPUR', 'NAGPUR', 'INDORE', 'BHOPAL', 'PATNA', 'VADODARA', 'COIMBATORE',
                          'LUDHIANA', 'AGRA', 'NASHIK', 'RANCHI', 'MEERUT', 'RAIPUR', 'GUWAHATI', 'CHANDIGARH') THEN 'TIER-2'
            ELSE 'TIER-3'
        END AS CITY_TIER,
        CAST(ZIPCODE AS STRING) AS ZIP_CODE,
        CAST(ACTIVEFLAG AS STRING) AS ACTIVE_FLAG,
        TO_TIMESTAMP_TZ(CREATEDDATE, 'YYYY-MM-DD HH24:MI:SS') AS CREATED_TS,
        TO_TIMESTAMP_TZ(MODIFIEDDATE, 'YYYY-MM-DD HH24:MI:SS') AS MODIFIED_TS,
        STG_FILE_NAME,
        STG_FILE_LOAD_TS,
        STG_FILE_MD5,
        CURRENT_TIMESTAMP AS COPY_DATA_TS
    FROM BRONZE.LOCATION_BRZ
) AS SOURCE
ON TARGET.LOCATION_ID = SOURCE.LOCATION_ID
WHEN MATCHED AND (
    TARGET.CITY != SOURCE.CITY OR
    TARGET.STATE != SOURCE.STATE OR
    TARGET.STATE_CODE != SOURCE.STATE_CODE OR
    TARGET.IS_UNION_TERRITORY != SOURCE.IS_UNION_TERRITORY OR
    TARGET.CAPITAL_CITY_FLAG != SOURCE.CAPITAL_CITY_FLAG OR
    TARGET.CITY_TIER != SOURCE.CITY_TIER OR
    TARGET.ZIP_CODE != SOURCE.ZIP_CODE OR
    TARGET.ACTIVE_FLAG != SOURCE.ACTIVE_FLAG OR
    TARGET.MODIFIED_TS != SOURCE.MODIFIED_TS
) THEN
    UPDATE SET
        TARGET.CITY = SOURCE.CITY,
        TARGET.STATE = SOURCE.STATE,
        TARGET.STATE_CODE = SOURCE.STATE_CODE,
        TARGET.IS_UNION_TERRITORY = SOURCE.IS_UNION_TERRITORY,
        TARGET.CAPITAL_CITY_FLAG = SOURCE.CAPITAL_CITY_FLAG,
        TARGET.CITY_TIER = SOURCE.CITY_TIER,
        TARGET.ZIP_CODE = SOURCE.ZIP_CODE,
        TARGET.ACTIVE_FLAG = SOURCE.ACTIVE_FLAG,
        TARGET.MODIFIED_TS = SOURCE.MODIFIED_TS,
        TARGET.STG_FILE_NAME = SOURCE.STG_FILE_NAME,
        TARGET.STG_FILE_LOAD_TS = SOURCE.STG_FILE_LOAD_TS,
        TARGET.STG_FILE_MD5 = SOURCE.STG_FILE_MD5,
        TARGET.COPY_DATA_TS = SOURCE.COPY_DATA_TS
WHEN NOT MATCHED THEN
    INSERT (
        LOCATION_ID,
        CITY,
        STATE,
        STATE_CODE,
        IS_UNION_TERRITORY,
        CAPITAL_CITY_FLAG,
        CITY_TIER,
        ZIP_CODE,
        ACTIVE_FLAG,
        CREATED_TS,
        MODIFIED_TS,
        STG_FILE_NAME,
        STG_FILE_LOAD_TS,
        STG_FILE_MD5,
        COPY_DATA_TS
    )
    VALUES (
        SOURCE.LOCATION_ID,
        SOURCE.CITY,
        SOURCE.STATE,
        SOURCE.STATE_CODE,
        SOURCE.IS_UNION_TERRITORY,
        SOURCE.CAPITAL_CITY_FLAG,
        SOURCE.CITY_TIER,
        SOURCE.ZIP_CODE,
        SOURCE.ACTIVE_FLAG,
        SOURCE.CREATED_TS,
        SOURCE.MODIFIED_TS,
        SOURCE.STG_FILE_NAME,
        SOURCE.STG_FILE_LOAD_TS,
        SOURCE.STG_FILE_MD5,
        SOURCE.COPY_DATA_TS
    );

-- ----------------------------------------------------------------------------------------------------
-- SILVER TO GOLD
-- ----------------------------------------------------------------------------------------------------
-- MERGE INTO
--         GOLD.DIM_LOCATION AS target
--     USING
--         SILVER.LOCATION_SLV AS source
--     ON
--         target.LOCATION_ID = source.LOCATION_ID and
--         target.ACTIVE_FLAG = source.ACTIVE_FLAG
--     WHEN MATCHED
--         AND source.METADATA$ACTION = 'DELETE' and source.METADATA$ISUPDATE = 'TRUE' THEN
--     -- Update the existing record to close its validity period
--     UPDATE SET
--         target.EFF_END_DT = CURRENT_TIMESTAMP(),
--         target.CURRENT_FLAG = FALSE
--     WHEN NOT MATCHED
--         AND source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'TRUE'
--     THEN
--     -- Insert new record with current data and new effective start date
--     INSERT (
--         LOCATION_HK,
--         LOCATION_ID,
--         CITY,
--         STATE,
--         STATE_CODE,
--         IS_UNION_TERRITORY,
--         CAPITAL_CITY_FLAG,
--         CITY_TIER,
--         ZIP_CODE,
--         ACTIVE_FLAG,
--         EFF_START_DT,
--         EFF_END_DT,
--         CURRENT_FLAG
--     )
--     VALUES (
--         hash(SHA1_hex(CONCAT(source.CITY, source.STATE, source.STATE_CODE, source.ZIP_CODE))),
--         source.LOCATION_ID,
--         source.CITY,
--         source.STATE,
--         source.STATE_CODE,
--         source.IS_UNION_TERRITORY,
--         source.CAPITAL_CITY_FLAG,
--         source.CITY_TIER,
--         source.ZIP_CODE,
--         source.ACTIVE_FLAG,
--         CURRENT_TIMESTAMP(),
--         NULL,
--         TRUE
--     )
--     WHEN NOT MATCHED AND
--     source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'FALSE' THEN
--     -- Insert new record with current data and new effective start date
--     INSERT (
--         LOCATION_HK,
--         LOCATION_ID,
--         CITY,
--         STATE,
--         STATE_CODE,
--         IS_UNION_TERRITORY,
--         CAPITAL_CITY_FLAG,
--         CITY_TIER,
--         ZIP_CODE,
--         ACTIVE_FLAG,
--         EFF_START_DT,
--         EFF_END_DT,
--         CURRENT_FLAG
--     )
--     VALUES (
--         hash(SHA1_hex(CONCAT(source.CITY, source.STATE, source.STATE_CODE, source.ZIP_CODE))),
--         source.LOCATION_ID,
--         source.CITY,
--         source.STATE,
--         source.STATE_CODE,
--         source.IS_UNION_TERRITORY,
--         source.CAPITAL_CITY_FLAG,
--         source.CITY_TIER,
--         source.ZIP_CODE,
--         source.ACTIVE_FLAG,
--         CURRENT_TIMESTAMP(),
--         NULL,
--         TRUE
--     );

-- ====================================================================================================