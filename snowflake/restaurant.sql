-- ====================================================================================================
-- RESTAURANT
-- ====================================================================================================
-- CHANGE CONTEXT
USE ROLE ACCOUNTADMIN;
USE DATABASE SWIGGY;
USE SCHEMA BRONZE;
USE WAREHOUSE ADHOC_WH;

-- ----------------------------------------------------------------------------------------------------
-- CREATE RESTAURANT_BRZ
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.RESTAURANT_BRZ (
    RESTAURANT_BRZ_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    FSSAI_REGISTRATION_NO INTEGER,
    RESTAURANT_NAME VARCHAR ,
    CUISINE_TYPE VARCHAR,
    PRICING_FOR_TWO VARCHAR,
    RESTAURANT_PHONE VARCHAR WITH TAG (COMMON.PII_POLICY_TAG = 'SENSITIVE'),
    OPERATING_HOURS VARCHAR,
    LOCATION_ID INTEGER,
    ACTIVE_FLAG VARCHAR,
    OPEN_STATUS VARCHAR,
    LOCALITY VARCHAR,
    RESTAURANT_ADDRESS VARCHAR,
    LATITUDE NUMBER(9,6),
    LONGITUDE NUMBER(9,6),

    -- RAW COLUMNS
    FSSAI_REGISTRATION_NO_RAW VARCHAR,
    RESTAURANT_NAME_RAW VARCHAR,
    CUISINE_TYPE_RAW VARCHAR,
    PRICING_FOR_TWO_RAW VARCHAR,
    RESTAURANT_PHONE_RAW VARCHAR WITH TAG (COMMON.PII_POLICY_TAG = 'SENSITIVE'),
    OPERATING_HOURS_RAW VARCHAR,
    LOCATION_ID_RAW VARCHAR,
    ACTIVE_FLAG_RAW VARCHAR,
    OPEN_STATUS_RAW VARCHAR,
    LOCALITY_RAW VARCHAR,
    RESTAURANT_ADDRESS_RAW VARCHAR,
    LATITUDE_RAW VARCHAR,
    LONGITUDE_RAW VARCHAR,

    -- AUDIT COLUMNS
    INGEST_RUN_ID INTEGER,
    CREATED_AT STRING,
    UPDATED_AT STRING
)
COMMENT = 'THIS IS THE RESTAURANT STAGE/RAW TABLE WHERE DATA WILL BE COPIED FROM INTERNAL STAGE USING COPY COMMAND. THIS IS AS-IS DATA REPRESETATION FROM THE SOURCE LOCATION. ALL THE COLUMNS ARE TEXT DATA TYPE EXCEPT THE AUDIT COLUMNS THAT ARE ADDED FOR TRACEABILITY.';
ALTER TABLE BRONZE.LOCATION_BRZ CLUSTER BY (INGEST_RUN_ID);

-- CREATING SEQUNCE TO GENERATE INGEST_RUN_ID
CREATE OR REPLACE SEQUENCE SEQ_RESTAURANT_INGEST_RUN_ID START = 1 INCREMENT = 1;
-- ----------------------------------------------------------------------------------------------------
-- CREATE RESTAURANT_LOAD_ERROR
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.RESTAURANT_LOAD_ERROR(
    ERROR_ID INTEGER PRIMARY KEY,
    VALIDATE_COLUMN VARCHAR(50),
    VALIDATION_TYPE VARCHAR(30),
    VALIDATION_ERROR_MSG VARCHAR(200),
    INGEST_RUN_ID INTEGER
);
-- ----------------------------------------------------------------------------------------------------
-- CREATE RESTAURANT_SLV
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.RESTAURANT_SLV (
    RESTAURANT_SLV_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    FSSAI_REGISTRATION_NO INTEGER,
    RESTAURANT_NAME VARCHAR(100),
    CUISINE_TYPE VARCHAR,
    PRICING_FOR_TWO NUMBER(10, 2),
    RESTAURANT_PHONE STRING(15) WITH TAG (COMMON.PII_POLICY_TAG = 'SENSITIVE'),
    OPERATING_HOURS VARCHAR(100),
    LOCATION_ID VARCHAR,
    ACTIVE_FLAG VARCHAR(10),
    OPEN_STATUS VARCHAR(10),
    LOCALITY VARCHAR(100),
    RESTAURANT_ADDRESS VARCHAR,
    LATITUDE NUMBER(9,6),
    LONGITUDE NUMBER(9,6),
    BATCH_ID VARCHAR,
    CREATED_AT TIMESTAMP_TZ,
    UPDATED_AT TIMESTAMP_TZ
)
COMMENT = 'RESTAURANT ENTITY UNDER CLEAN SCHEMA WITH APPROPRIATE DATA TYPE UNDER CLEAN SCHEMA LAYER, DATA IS POPULATED USING MERGE STATEMENT FROM THE STAGE LAYER LOCATION TABLE. THIS TABLE DOES NOT SUPPORT SCD2';

-- ----------------------------------------------------------------------------------------------------
-- CREATE RESTAURANT_GLD
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.DIM_RESTAURANT (
    RESTAURANT_ID INTEGER PRIMARY KEY,
    FSSAI_REGISTRATION_NO INTEGER,
    RESTAURANT_NAME VARCHAR(100),
    CUISINE_TYPE VARCHAR,
    PRICING_FOR_TWO NUMBER(10, 2),
    RESTAURANT_PHONE VARCHAR(15) WITH TAG (COMMON.PII_POLICY_TAG = 'SENSITIVE'),
    OPERATING_HOURS VARCHAR(100),
    LOCATION_ID NUMBER,
    ACTIVE_FLAG VARCHAR(10),
    OPEN_STATUS VARCHAR(10),
    LOCALITY VARCHAR(100),
    RESTAURANT_ADDRESS VARCHAR,
    LATITUDE NUMBER(9, 6),
    LONGITUDE NUMBER(9, 6),

    -- AUDIT COLUMNS
    BATCH_ID VARCHAR,
    STATUS VARCHAR(10) DEFAULT 'ACTIVE',
    EFF_START_DT TIMESTAMP_TZ,
    EFF_END_DT TIMESTAMP_TZ,
    CREATED_AT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'DIMENSIONAL TABLE FOR RESTAURANT ENTITY WITH HASH KEYS AND SCD ENABLED.';

-- =====================================================
-- PROCEDURE 1: STAGE TO BRONZE
-- =====================================================
CREATE OR REPLACE PROCEDURE BRONZE.SP_RESTAURANT_STAGE_TO_BRONZE(P_PIPELINE_NAME VARCHAR, P_FILE_NAME VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    V_ROWS_INSERTED INTEGER DEFAULT 0;
    V_INGEST_RUN_ID NUMBER DEFAULT 0;
    V_ERROR_MESSAGE VARCHAR(5000);
    V_START_TIME TIMESTAMP_TZ(9);
    V_END_TIME TIMESTAMP_TZ(9);
    V_EXECUTION_DURATION INTEGER;
    V_SOURCE_LOCATION VARCHAR;
    V_FILE_FORMAT VARCHAR;
    V_FILE_PATH VARCHAR;
BEGIN
    V_START_TIME := CURRENT_TIMESTAMP();

    -- GET PIPELINE CONFIGURATION
    SELECT SOURCE_LOCATION, FILE_FORMAT
    INTO :V_SOURCE_LOCATION, :V_FILE_FORMAT
    FROM COMMON.IMPORT_CONFIGURATION
    WHERE PIPELINE_NAME = :P_PIPELINE_NAME;

    -- Construct file path
    V_FILE_PATH := V_SOURCE_LOCATION || P_FILE_NAME;

    -- Start explicit transaction
    BEGIN TRANSACTION;

    -- CREATE TEMP TABLE
    CREATE OR REPLACE TEMPORARY TABLE TEMP_RESTAURANT_LOAD(
        RESTAURANT_NAME STRING,
        FSSAI_REGISTRATION_NO INTEGER,
        CUISINE_TYPE STRING,
        PRICING_FOR_TWO STRING,
        RESTAURANT_PHONE STRING,
        OPERATING_HOURS STRING,
        LOCATION_ID STRING,
        ACTIVE_FLAG STRING,
        OPEN_STATUS STRING,
        LOCALITY STRING,
        RESTAURANT_ADDRESS STRING,
        LATITUDE STRING,
        LONGITUDE STRING
    );

    -- Copy data from stage
    EXECUTE IMMEDIATE
    '
    COPY INTO TEMP_RESTAURANT_LOAD(
        FSSAI_REGISTRATION_NO, RESTAURANT_NAME, CUISINE_TYPE, PRICING_FOR_TWO, RESTAURANT_PHONE, OPERATING_HOURS,
        LOCATION_ID, ACTIVE_FLAG, OPEN_STATUS, LOCALITY, RESTAURANT_ADDRESS,
        LATITUDE, LONGITUDE)
    FROM (
        SELECT
            $1::STRING AS FSSAI_REGISTRATION_NO,
            $2::STRING AS RESTAURANT_NAME,
            $3::STRING AS CUISINE_TYPE,
            $4::STRING AS PRICING_FOR_TWO,
            $5::STRING AS RESTAURANT_PHONE,
            $6::STRING AS OPERATING_HOURS,
            $7::STRING AS LOCATION_ID,
            $8::STRING AS ACTIVE_FLAG,
            $9::STRING AS OPEN_STATUS,
            $10::STRING AS LOCALITY,
            $11::STRING AS RESTAURANT_ADDRESS,
            $12::STRING AS LATITUDE,
            $13::STRING AS LONGITUDE
        FROM ''' || V_FILE_PATH || '''
    )
    FILE_FORMAT = (FORMAT_NAME = ''' || V_FILE_FORMAT || ''')
    ON_ERROR = ABORT_STATEMENT
    ';

    -- Get row count
    SELECT COUNT(*) INTO :V_ROWS_INSERTED FROM BRONZE.TEMP_RESTAURANT_LOAD;

    -- Validate data loaded
    IF (V_ROWS_INSERTED = 0) THEN
        ROLLBACK;
        DROP TABLE IF EXISTS TEMP_RESTAURANT_LOAD;
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', 'No records loaded from file',
            'FILE_PATH', P_FILE_NAME,
            'ROWS_INSERTED', 0,
            'INGEST_RUN_ID', 0
        );
    END IF;

    -- Consume sequence
    SELECT SWIGGY.BRONZE.SEQ_RESTAURANT_INGEST_RUN_ID.NEXTVAL INTO :V_INGEST_RUN_ID;

    -- Insert into bronze table
    INSERT INTO BRONZE.RESTAURANT_BRZ (
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
        FSSAI_REGISTRATION_NO_RAW,
        RESTAURANT_NAME_RAW,
        CUISINE_TYPE_RAW,
        PRICING_FOR_TWO_RAW,
        RESTAURANT_PHONE_RAW,
        OPERATING_HOURS_RAW,
        LOCATION_ID_RAW,
        ACTIVE_FLAG_RAW,
        OPEN_STATUS_RAW,
        LOCALITY_RAW,
        RESTAURANT_ADDRESS_RAW,
        LATITUDE_RAW,
        LONGITUDE_RAW,
        INGEST_RUN_ID, CREATED_AT, UPDATED_AT
    )
    SELECT
        TRY_TO_NUMBER(FSSAI_REGISTRATION_NO),
        TO_VARCHAR(RESTAURANT_NAME),
        TO_VARCHAR(CUISINE_TYPE),
        TRY_TO_NUMBER(PRICING_FOR_TWO),
        TRY_TO_NUMBER(RESTAURANT_PHONE),
        TRIM(OPERATING_HOURS),
        TRY_TO_NUMBER(LOCATION_ID),
        TO_VARCHAR(ACTIVE_FLAG),
        TO_VARCHAR(OPEN_STATUS),
        TO_VARCHAR(LOCALITY),
        TO_VARCHAR(RESTAURANT_ADDRESS),
        TRY_TO_NUMBER(LATITUDE,9,6),
        TRY_TO_NUMBER(LONGITUDE,9,6),
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
        :V_INGEST_RUN_ID,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    FROM TEMP_RESTAURANT_LOAD;

    -- Commit transaction
    COMMIT;

    -- Cleanup
    DROP TABLE IF EXISTS BRONZE.TEMP_RESTAURANT_LOAD;

    V_END_TIME := CURRENT_TIMESTAMP();
    V_EXECUTION_DURATION := DATEDIFF(SECOND, V_START_TIME, V_END_TIME);

    RETURN OBJECT_CONSTRUCT(
        'STATUS', 'SUCCESS',
        'MESSAGE', 'Data loaded successfully with transaction',
        'FILE_PATH', V_FILE_PATH,
        'ROWS_INSERTED', V_ROWS_INSERTED,
        'INGEST_RUN_ID', V_INGEST_RUN_ID,
        'EXECUTION_TIME_SEC', V_EXECUTION_DURATION
    );

EXCEPTION
    WHEN OTHER THEN
        -- Rollback everything including sequence consumption
        ROLLBACK;

        V_ERROR_MESSAGE := SQLERRM;
        V_END_TIME := CURRENT_TIMESTAMP();
        V_EXECUTION_DURATION := DATEDIFF(SECOND, V_START_TIME, V_END_TIME);

        -- Cleanup
        DROP TABLE IF EXISTS TEMP_RESTAURANT_LOAD;

        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', V_ERROR_MESSAGE,
            'FILE_PATH', V_FILE_PATH,
            'ROWS_INSERTED', 0,
            'INGEST_RUN_ID', 0,
            'EXECUTION_TIME_SEC', V_EXECUTION_DURATION,
            'NOTE', 'Transaction rolled back - no sequence consumed'
        );
END;
$$;

-- =====================================================
-- PROCEDURE 2: BRONZE TO SILVER
-- =====================================================
CREATE OR REPLACE PROCEDURE SILVER.SP_RESTAURANT_BRONZE_TO_SILVER(
    P_PIPELINE_NAME STRING,
    P_INGEST_RUN_ID INTEGER,
    P_BATCH_ID STRING
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    V_ROWS_INSERTED INTEGER DEFAULT 0;
    V_ROWS_UPDATED INTEGER DEFAULT 0;
    v_bronze_table VARCHAR(200);
    v_load_error_table VARCHAR(200);
    v_stage_table VARCHAR(200);
    v_silver_table VARCHAR(200);

    v_bronze_row_count INTEGER DEFAULT 0;
    v_valid_row_count INTEGER DEFAULT 0;
    v_invalid_row_count INTEGER DEFAULT 0;
    v_silver_row_count INTEGER DEFAULT 0;
    v_start_time TIMESTAMP_TZ(9);
    v_end_time TIMESTAMP_TZ(9);
    v_execution_duration INTEGER;
    v_error_message VARCHAR(5000);
    v_sql VARCHAR(10000);
    v_dq_result VARIANT;
    v_dq_result_status VARCHAR(50);
    v_run_status VARCHAR(50);

BEGIN

    -- STEP 0: INITIALIZE VARIABLES
    v_start_time := CURRENT_TIMESTAMP();

    -- Get configuration from IMPORT_CONFIGURATION table
    SELECT BRONZE_TABLE, LOAD_ERROR_TABLE, STAGE_TABLE, SILVER_TABLE
    INTO :v_bronze_table, :v_load_error_table, :v_stage_table, :v_silver_table
    FROM COMMON.IMPORT_CONFIGURATION
    WHERE PIPELINE_NAME = :P_PIPELINE_NAME;

    SELECT COUNT(*)
    INTO :v_bronze_row_count
    FROM IDENTIFIER(:v_bronze_table)
    WHERE INGEST_RUN_ID = :P_INGEST_RUN_ID;

    -- IF EXISTS (SELECT 1 FROM COMMON.INGEST_RUN WHERE INGEST_RUN_ID = :P_INGEST_RUN_ID) THEN
    -- RETURN OBJECT_CONSTRUCT('STATUS', 'SKIPPED', 'ERROR', 'Already processed');
    -- END IF;

    -- Validate configuration
    IF (v_bronze_table IS NULL OR v_load_error_table IS NULL OR
        v_stage_table IS NULL OR v_silver_table IS NULL) THEN
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', 'Configuration not found for pipeline: ' || P_PIPELINE_NAME
        );
    END IF;

    IF (v_bronze_row_count = 0) THEN
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', 'No records found for INGEST_RUN_ID'
        );
    END IF;

    -- Create staging table with DQ flags
    v_sql := 'CREATE OR REPLACE TEMPORARY TABLE ' || v_stage_table || ' AS
          SELECT *,
                 TRUE AS IS_VALID,
                 ''' || P_BATCH_ID || ''' AS BATCH_ID
          FROM ' || v_bronze_table || '
          WHERE INGEST_RUN_ID = ' || P_INGEST_RUN_ID;

    EXECUTE IMMEDIATE v_sql;

    -- Run DQ checks and mark invalid records
    LET res RESULTSET := (EXECUTE IMMEDIATE 'CALL BRONZE.SP_EXECUTE_DATA_QUALITY_VALIDATION (?, ?, ?, ?)' USING (v_stage_table, v_bronze_table, v_load_error_table, P_INGEST_RUN_ID));
    LET cur CURSOR FOR res;
    OPEN cur;
    FETCH cur INTO v_dq_result;
    CLOSE cur;

    -- Check if DQ validation was successful
    v_dq_result_status := v_dq_result:STATUS::STRING;
    IF (v_dq_result_status NOT LIKE 'SUCCESS%') THEN
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', 'DQ Validation failed'
        );
    END IF;

    -- Count valid from stage table
    SELECT COUNT(*) INTO :v_valid_row_count FROM IDENTIFIER(:v_stage_table) WHERE IS_VALID = TRUE;

    -- Calculate invalid
    v_invalid_row_count := v_bronze_row_count - v_valid_row_count;

    -- Merge only VALID records into Silver table using MERGE statement
    v_sql := '
        MERGE INTO ' || v_silver_table || ' AS TGT
        USING (
            SELECT
                RESTAURANT_BRZ_ID,
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
                CURRENT_TIMESTAMP() AS CREATED_AT,
                CURRENT_TIMESTAMP() AS UPDATED_AT
            FROM ' || v_stage_table || '
            WHERE IS_VALID = TRUE AND BATCH_ID = ''' || P_BATCH_ID || '''
        ) AS SRC
        ON TGT.FSSAI_REGISTRATION_NO = SRC.FSSAI_REGISTRATION_NO
        AND TGT.LATITUDE = SRC.LATITUDE
        AND TGT.LONGITUDE = SRC.LONGITUDE
        WHEN MATCHED THEN
            UPDATE SET
                TGT.RESTAURANT_NAME = SRC.RESTAURANT_NAME,
                TGT.CUISINE_TYPE = SRC.CUISINE_TYPE,
                TGT.PRICING_FOR_TWO = SRC.PRICING_FOR_TWO,
                TGT.RESTAURANT_PHONE = SRC.RESTAURANT_PHONE,
                TGT.OPERATING_HOURS = SRC.OPERATING_HOURS,
                TGT.LOCATION_ID = SRC.LOCATION_ID,
                TGT.ACTIVE_FLAG = SRC.ACTIVE_FLAG,
                TGT.OPEN_STATUS = SRC.OPEN_STATUS,
                TGT.LOCALITY = SRC.LOCALITY,
                TGT.RESTAURANT_ADDRESS = SRC.RESTAURANT_ADDRESS,
                TGT.LATITUDE = SRC.LATITUDE,
                TGT.LONGITUDE = SRC.LONGITUDE,
                TGT.BATCH_ID = SRC.BATCH_ID,
                TGT.UPDATED_AT = SRC.UPDATED_AT
        WHEN NOT MATCHED THEN
            INSERT (
                FSSAI_REGISTRATION_NO, RESTAURANT_NAME, CUISINE_TYPE, PRICING_FOR_TWO, RESTAURANT_PHONE, OPERATING_HOURS,
                LOCATION_ID, ACTIVE_FLAG, OPEN_STATUS, LOCALITY, RESTAURANT_ADDRESS,
                LATITUDE, LONGITUDE, BATCH_ID, CREATED_AT, UPDATED_AT
            )
            VALUES (
                SRC.FSSAI_REGISTRATION_NO, SRC.RESTAURANT_NAME, SRC.CUISINE_TYPE, SRC.PRICING_FOR_TWO, SRC.RESTAURANT_PHONE,
                SRC.OPERATING_HOURS, SRC.LOCATION_ID, SRC.ACTIVE_FLAG, SRC.OPEN_STATUS,
                SRC.LOCALITY, SRC.RESTAURANT_ADDRESS, SRC.LATITUDE, SRC.LONGITUDE,
                SRC.BATCH_ID, SRC.CREATED_AT, SRC.UPDATED_AT
            )';
    EXECUTE IMMEDIATE v_sql;

    -- Get merge statistics (rows inserted + updated)
    V_ROWS_INSERTED := SQLROWCOUNT;

    -- Get target row count for this batch
    SELECT COUNT(*) INTO :v_silver_row_count FROM IDENTIFIER(:v_stage_table) WHERE BATCH_ID = :P_BATCH_ID;

    v_end_time := CURRENT_TIMESTAMP();
    v_execution_duration := DATEDIFF(SECOND, v_start_time, v_end_time);

    -- Insert into INGEST_RUN table
    INSERT INTO COMMON.INGEST_RUN(
        INGEST_RUN_ID, PIPELINE_NAME, SOURCE_TABLE, LOAD_ERROR_TABLE, RUN_STATUS,
        SOURCE_ROW_COUNT, VALID_ROW_COUNT, INVALID_ROW_COUNT, EXECUTION_DURATION_SEC,
        ERROR_MESSAGE, EXECUTED_AT, EXECUTED_BY)
    VALUES(
        :p_ingest_run_id,
        :p_pipeline_name,
        :v_bronze_table,
        :v_load_error_table,
        'SUCCESS',
        :v_bronze_row_count,
        :v_valid_row_count,
        :v_invalid_row_count,
        :v_execution_duration,
        'None',
        :v_end_time,
        CURRENT_USER()
    );

    -- Drop staging table
    v_sql := 'DROP TABLE IF EXISTS ' || v_stage_table;
    EXECUTE IMMEDIATE v_sql;

    RETURN OBJECT_CONSTRUCT(
        'STATUS', 'SUCCESSFUL',
        'ERROR', 'NONE',
        'ROWS_INSERTED', v_rows_inserted::VARCHAR,
        'ROWS_UPDATED', v_rows_updated::VARCHAR,
        'INGEST_LOG', OBJECT_CONSTRUCT(
            'INGEST_RUN_ID', P_INGEST_RUN_ID::VARCHAR,
            'BATCH_ID', P_BATCH_ID,
            'BRONZE_ROW_COUNT', v_bronze_row_count::VARCHAR,
            'VALID_ROW_COUNT', v_valid_row_count::VARCHAR,
            'INVALID_ROW_COUNT', v_invalid_row_count::VARCHAR,
            'TARGET_ROW_COUNT', v_silver_row_count::VARCHAR,
            'ROWS_MERGED', v_rows_inserted::VARCHAR,
            'EXECUTION_TIME_SEC', v_execution_duration::VARCHAR,
            'DATA_VALIDATION_RESULT', v_dq_result
        )
    );

EXCEPTION
    WHEN OTHER THEN
        v_error_message := SQLERRM;
        v_end_time := CURRENT_TIMESTAMP();
        v_execution_duration := DATEDIFF(SECOND, v_start_time, v_end_time);

        -- Update run status to failed
        INSERT INTO COMMON.INGEST_RUN(
            INGEST_RUN_ID, PIPELINE_NAME, SOURCE_TABLE, LOAD_ERROR_TABLE, RUN_STATUS,
            SOURCE_ROW_COUNT, VALID_ROW_COUNT, INVALID_ROW_COUNT, EXECUTION_DURATION_SEC,
            ERROR_MESSAGE, EXECUTED_AT, EXECUTED_BY)
        VALUES(
            :p_ingest_run_id,
            :p_pipeline_name,
            :v_bronze_table,
            :v_load_error_table,
            'FAILED',
            :v_bronze_row_count,
            :v_valid_row_count,
            :v_invalid_row_count,
            :v_execution_duration,
            :v_error_message,
            :v_end_time,
            CURRENT_USER()
        );

        -- Drop temp table if exists
        EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS ' || v_stage_table;

        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', v_error_message
        );
END;
$$;

-- =====================================================
-- PROCEDURE 3: SILVER TO GOLD (WITH SCD2)
-- =====================================================
CREATE OR REPLACE PROCEDURE GOLD.SP_RESTAURANT_SILVER_TO_GOLD(
    P_BATCH_ID STRING
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    V_CURRENT_TIMESTAMP TIMESTAMP_TZ(9);
    V_ROWS_INSERTED INTEGER DEFAULT 0;
    V_ROWS_UPDATED INTEGER DEFAULT 0;
    V_ROWS_UNCHANGED INTEGER DEFAULT 0;
    V_ROWS_DELETED INTEGER DEFAULT 0;
    V_SOURCE_ROW_COUNT INTEGER DEFAULT 0;
    V_BATCH_EXISTS INTEGER DEFAULT 0;
    V_START_TIME TIMESTAMP_TZ(9);
    V_END_TIME TIMESTAMP_TZ(9);
    V_EXECUTION_DURATION INTEGER;
    V_ERROR_MESSAGE VARCHAR(5000);
BEGIN
    V_START_TIME := CURRENT_TIMESTAMP();
    V_CURRENT_TIMESTAMP := CURRENT_TIMESTAMP();

    -- ================================================================
    -- STEP 1: VALIDATION & IDEMPOTENCY CHECK
    -- ================================================================

    -- Check if batch already processed
    SELECT COUNT(*)
    INTO :V_BATCH_EXISTS
    FROM GOLD.DIM_RESTAURANT
    WHERE BATCH_ID = :P_BATCH_ID
    AND STATUS = 'ACTIVE'
    LIMIT 1;

    IF (V_BATCH_EXISTS > 0) THEN
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'SKIPPED',
            'MESSAGE', 'Batch already processed',
            'BATCH_ID', P_BATCH_ID,
            'ROWS_INSERTED', 0,
            'ROWS_UPDATED', 0,
            'ROWS_UNCHANGED', 0
        );
    END IF;

    -- Check if source batch exists
    SELECT COUNT(*)
    INTO :V_SOURCE_ROW_COUNT
    FROM SILVER.RESTAURANT_SLV
    WHERE BATCH_ID = :P_BATCH_ID;

    IF (V_SOURCE_ROW_COUNT = 0) THEN
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', 'No records found for BATCH_ID: ' || P_BATCH_ID,
            'BATCH_ID', P_BATCH_ID
        );
    END IF;

    -- ================================================================
    -- STEP 2: CREATE STAGING TABLE WITH CHANGE DETECTION
    -- ================================================================
    -- This staging table will identify what action to take for each record

    CREATE OR REPLACE TEMPORARY TABLE TEMP_RESTAURANT_STAGING AS
    SELECT
        SRC.RESTAURANT_SLV_ID,
        SRC.FSSAI_REGISTRATION_NO,
        SRC.RESTAURANT_NAME,
        SRC.CUISINE_TYPE,
        SRC.PRICING_FOR_TWO,
        SRC.RESTAURANT_PHONE,
        SRC.OPERATING_HOURS,
        SRC.LOCATION_ID,
        SRC.ACTIVE_FLAG,
        SRC.OPEN_STATUS,
        SRC.LOCALITY,
        SRC.RESTAURANT_ADDRESS,
        SRC.LATITUDE,
        SRC.LONGITUDE,
        SRC.BATCH_ID,

        -- Calculate hash of attributes for change detection
        SHA2_HEX(
            CONCAT_WS('|',
                COALESCE(TO_VARCHAR(SRC.FSSAI_REGISTRATION_NO), ''),
                COALESCE(SRC.RESTAURANT_NAME, ''),
                COALESCE(SRC.CUISINE_TYPE, ''),
                COALESCE(TO_VARCHAR(SRC.PRICING_FOR_TWO), ''),
                COALESCE(SRC.RESTAURANT_PHONE, ''),
                COALESCE(SRC.OPERATING_HOURS, ''),
                COALESCE(SRC.LOCATION_ID, ''),
                COALESCE(SRC.OPEN_STATUS, ''),
                COALESCE(SRC.LOCALITY, ''),
                COALESCE(SRC.RESTAURANT_ADDRESS, ''),
                COALESCE(TO_VARCHAR(SRC.LATITUDE), ''),
                COALESCE(TO_VARCHAR(SRC.LONGITUDE), '')
            )
        ) AS CURRENT_HASH,

        -- Get existing record info
        TGT.RESTAURANT_ID AS EXISTING_RESTAURANT_ID,
        SHA2_HEX(
            CONCAT_WS('|',
                COALESCE(TO_VARCHAR(SRC.FSSAI_REGISTRATION_NO), ''),
                COALESCE(SRC.RESTAURANT_NAME, ''),
                COALESCE(SRC.CUISINE_TYPE, ''),
                COALESCE(TO_VARCHAR(SRC.PRICING_FOR_TWO), ''),
                COALESCE(SRC.RESTAURANT_PHONE, ''),
                COALESCE(SRC.OPERATING_HOURS, ''),
                COALESCE(SRC.LOCATION_ID, ''),
                COALESCE(SRC.OPEN_STATUS, ''),
                COALESCE(SRC.LOCALITY, ''),
                COALESCE(SRC.RESTAURANT_ADDRESS, ''),
                COALESCE(TO_VARCHAR(SRC.LATITUDE), ''),
                COALESCE(TO_VARCHAR(SRC.LONGITUDE), '')
            )
        ) AS EXISTING_HASH,

        -- Determine action
        CASE
            WHEN TGT.RESTAURANT_ID IS NULL THEN 'INSERT'
            WHEN SHA2_HEX(
            CONCAT_WS('|',
                COALESCE(TO_VARCHAR(SRC.FSSAI_REGISTRATION_NO), ''),
                COALESCE(SRC.RESTAURANT_NAME, ''),
                COALESCE(SRC.CUISINE_TYPE, ''),
                COALESCE(TO_VARCHAR(SRC.PRICING_FOR_TWO), ''),
                COALESCE(SRC.RESTAURANT_PHONE, ''),
                COALESCE(SRC.OPERATING_HOURS, ''),
                COALESCE(SRC.LOCATION_ID, ''),
                COALESCE(SRC.OPEN_STATUS, ''),
                COALESCE(SRC.LOCALITY, ''),
                COALESCE(SRC.RESTAURANT_ADDRESS, ''),
                COALESCE(TO_VARCHAR(SRC.LATITUDE), ''),
                COALESCE(TO_VARCHAR(SRC.LONGITUDE), '')
            )
        ) != SHA2_HEX(
            CONCAT_WS('|',
                COALESCE(TO_VARCHAR(SRC.FSSAI_REGISTRATION_NO), ''),
                COALESCE(SRC.RESTAURANT_NAME, ''),
                COALESCE(SRC.CUISINE_TYPE, ''),
                COALESCE(TO_VARCHAR(SRC.PRICING_FOR_TWO), ''),
                COALESCE(SRC.RESTAURANT_PHONE, ''),
                COALESCE(SRC.OPERATING_HOURS, ''),
                COALESCE(SRC.LOCATION_ID, ''),
                COALESCE(SRC.OPEN_STATUS, ''),
                COALESCE(SRC.LOCALITY, ''),
                COALESCE(SRC.RESTAURANT_ADDRESS, ''),
                COALESCE(TO_VARCHAR(SRC.LATITUDE), ''),
                COALESCE(TO_VARCHAR(SRC.LONGITUDE), '')
            )
        ) THEN 'UPDATE'
            ELSE 'UNCHANGED'
        END AS SCD_ACTION

    FROM SILVER.RESTAURANT_SLV SRC
    LEFT JOIN GOLD.DIM_RESTAURANT TGT
        ON TGT.RESTAURANT_ID = SRC.RESTAURANT_SLV_ID
        AND TGT.STATUS = 'ACTIVE'
    WHERE SRC.BATCH_ID = :P_BATCH_ID;

    -- ================================================================
    -- STEP 3: EXPIRE CHANGED RECORDS (SCD2 - CLOSE OLD VERSIONS)
    -- ================================================================

    UPDATE GOLD.DIM_RESTAURANT
    SET
        STATUS = 'INACTIVE',
        EFF_END_DT = :V_CURRENT_TIMESTAMP,
        UPDATED_AT = :V_CURRENT_TIMESTAMP
    WHERE RESTAURANT_ID IN (
        SELECT RESTAURANT_SLV_ID
        FROM TEMP_RESTAURANT_STAGING
        WHERE SCD_ACTION = 'UPDATE'
    )
    AND STATUS = 'ACTIVE';

    V_ROWS_UPDATED := SQLROWCOUNT;

    -- ================================================================
    -- STEP 4: INSERT NEW AND CHANGED RECORDS
    -- ================================================================

    INSERT INTO GOLD.DIM_RESTAURANT (
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
        STATUS,
        EFF_START_DT,
        EFF_END_DT,
        BATCH_ID,
        CREATED_AT,
        UPDATED_AT
    )
    SELECT
        RESTAURANT_SLV_ID AS RESTAURANT_ID,
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
        'ACTIVE' AS STATUS,
        :V_CURRENT_TIMESTAMP AS EFF_START_DT,
        CAST('9999-12-31 23:59:59' AS TIMESTAMP_TZ(9)) AS EFF_END_DT,
        BATCH_ID,
        :V_CURRENT_TIMESTAMP AS CREATED_AT,
        :V_CURRENT_TIMESTAMP AS UPDATED_AT
    FROM TEMP_RESTAURANT_STAGING
    WHERE SCD_ACTION IN ('INSERT', 'UPDATE');

    V_ROWS_INSERTED := SQLROWCOUNT;

    -- Get unchanged count
    SELECT COUNT(*)
    INTO :V_ROWS_UNCHANGED
    FROM TEMP_RESTAURANT_STAGING
    WHERE SCD_ACTION = 'UNCHANGED';

    -- ================================================================
    -- STEP 5: CLEANUP & RETURN
    -- ================================================================

    DROP TABLE IF EXISTS TEMP_RESTAURANT_STAGING;

    V_END_TIME := CURRENT_TIMESTAMP();
    V_EXECUTION_DURATION := DATEDIFF(SECOND, V_START_TIME, V_END_TIME);

    RETURN OBJECT_CONSTRUCT(
        'STATUS', 'SUCCESS',
        'ERROR', 'None',
        'MESSAGE', 'SCD2 processing completed successfully',
        'BATCH_ID', P_BATCH_ID,
        'SOURCE_ROW_COUNT', V_SOURCE_ROW_COUNT,
        'ROWS_INSERTED', V_ROWS_INSERTED,
        'ROWS_UPDATED', V_ROWS_UPDATED,
        'ROWS_DELETED', V_ROWS_DELETED,
        'ROWS_UNCHANGED', V_ROWS_UNCHANGED,
        'TOTAL_PROCESSED', V_SOURCE_ROW_COUNT
    );

EXCEPTION
    WHEN OTHER THEN
        V_ERROR_MESSAGE := SQLERRM;
        V_END_TIME := CURRENT_TIMESTAMP();
        V_EXECUTION_DURATION := DATEDIFF(SECOND, V_START_TIME, V_END_TIME);

        -- Cleanup temp table
        DROP TABLE IF EXISTS TEMP_RESTAURANT_STAGING;

        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', V_ERROR_MESSAGE,
            'BATCH_ID', P_BATCH_ID,
            'EXECUTION_TIME_SEC', V_EXECUTION_DURATION,
            'SOURCE_ROW_COUNT', V_SOURCE_ROW_COUNT,
            'ROWS_INSERTED', V_ROWS_INSERTED,
            'ROWS_UPDATED', V_ROWS_UPDATED,
            'ROWS_DELETED', V_ROWS_DELETED,
            'ROWS_UNCHANGED', V_ROWS_UNCHANGED,
            'TOTAL_PROCESSED', V_SOURCE_ROW_COUNT
        );
END;
$$;
-- =====================================================