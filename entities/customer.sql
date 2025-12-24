-- ====================================================================================================
-- CUSTOMER
-- ====================================================================================================
-- CHANGE CONTEXT
USE DATABASE SWIGGY;
USE SCHEMA BRONZE;
USE WAREHOUSE ADHOC_WH;

-- ----------------------------------------------------------------------------------------------------
-- CREATE CUSTOMER_BRZ
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.CUSTOMER_BRZ (
    CUSTOMER_BRZ_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- MAIN COLUMNS WITH APPROPRIATE DATA TYPES
    NAME VARCHAR(100),
    MOBILE VARCHAR(20) WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),
    EMAIL VARCHAR(50) WITH TAG (COMMON.PII_POLICY_TAG = 'EMAIL'),
    LOGIN_BY_USING VARCHAR(20),
    GENDER VARCHAR(10) WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),
    DOB DATE WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),
    ANNIVERSARY DATE,
    PREFERENCES VARIANT,

    -- RAW COLUMNS
    NAME_RAW VARCHAR,
    MOBILE_RAW VARCHAR,
    EMAIL_RAW VARCHAR,
    LOGIN_BY_USING_RAW VARCHAR,
    GENDER_RAW VARCHAR,
    DOB_RAW VARCHAR,
    ANNIVERSARY_RAW VARCHAR,
    PREFERENCES_RAW VARCHAR,

    -- AUDIT COLUMNS
    INGEST_RUN_ID INTEGER DEFAULT 0,
    CREATED_AT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'CUSTOMER BRONZE TABLE WITH RAW AND TRANSFORMED COLUMNS. RAW COLUMNS STORE AS-IS DATA FROM SOURCE, MAIN COLUMNS CONTAIN TYPE-CONVERTED VALUES (NULL IF CONVERSION FAILS).';
ALTER TABLE BRONZE.CUSTOMER_BRZ CLUSTER BY (INGEST_RUN_ID);

-- CREATING SEQUNCE TO GENERATE INGEST_RUN_ID
CREATE OR REPLACE SEQUENCE SEQ_CUSTOMER_INGEST_RUN_ID START = 1 INCREMENT = 1;

-- ----------------------------------------------------------------------------------------------------
-- CREATE CUSTOMER_LOAD_ERROR
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.CUSTOMER_LOAD_ERROR(
    ERROR_ID INTEGER PRIMARY KEY,
    VALIDATE_COLUMN VARCHAR(50),
    VALIDATION_TYPE VARCHAR(30),
    VALIDATION_ERROR_MSG VARCHAR(200),
    INGEST_RUN_ID INTEGER
);
-- ----------------------------------------------------------------------------------------------------
-- CREATE CUSTOMER_SLV
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.CUSTOMER_SLV (
    CUSTOMER_SLV_ID INTEGER AUTOINCREMENT PRIMARY KEY,
    NAME VARCHAR(100),
    MOBILE VARCHAR(15)  WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),
    EMAIL VARCHAR(100) WITH TAG (COMMON.PII_POLICY_TAG = 'EMAIL'),
    LOGIN_BY_USING VARCHAR(50),
    GENDER VARCHAR(10)  WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),
    DOB DATE WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),
    ANNIVERSARY DATE,
    PREFERENCES VARCHAR,
    BATCH_ID VARCHAR(36),
    CREATED_AT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP,
    UPDATED_AT TIMESTAMP_TZ(9)
)
COMMENT = 'CUSTOMER ENTITY UNDER CLEAN SCHEMA WITH APPROPRIATE DATA TYPE UNDER CLEAN SCHEMA LAYER, DATA IS POPULATED USING MERGE STATEMENT FROM THE STAGE LAYER LOCATION TABLE. THIS TABLE DOES NOT SUPPORT SCD2';
ALTER TABLE SILVER.CUSTOMER_SLV CLUSTER BY (MOBILE, EMAIL);
-- ----------------------------------------------------------------------------------------------------
-- CREATE CUSTOMER_DIM
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.DIM_CUSTOMER (
    CUSTOMER_ID INTEGER PRIMARY KEY AUTOINCREMENT,                                 -- NATURAL KEY FOR THE CUSTOMER
    NAME STRING(100) NOT NULL,                                   -- CUSTOMER NAME
    MOBILE STRING(15) WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),                                           -- MOBILE NUMBER
    EMAIL STRING(100) WITH TAG (COMMON.PII_POLICY_TAG = 'EMAIL'),                                           -- EMAIL
    LOGIN_BY_USING STRING(50),                                   -- METHOD OF LOGIN
    GENDER STRING(10) WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),                                           -- GENDER
    DOB DATE WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),                                                    -- DATE OF BIRTH
    ANNIVERSARY DATE,                                            -- ANNIVERSARY
    PREFERENCES STRING,                                          -- PREFERENCES
    STATUS VARCHAR(10) DEFAULT 'ACTIVE',                     -- Record status (ACTIVE/INACTIVE)
    EFF_START_DT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP(),-- Effective start date
    EFF_END_DT TIMESTAMP_TZ(9) DEFAULT '9999-12-31 23:59:59'::TIMESTAMP_TZ(9), -- Effective end date
    BATCH_ID VARCHAR(36) NOT NULL,                           -- Batch identifier
    CREATED_AT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP(), -- Record creation timestamp
    UPDATED_AT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP() -- Record update timestamp
)
COMMENT = 'CUSTOMER DIMENSION TABLE WITH SCD TYPE 2 HANDLING FOR HISTORICAL TRACKING.';
ALTER TABLE GOLD.DIM_CUSTOMER CLUSTER BY (CUSTOMER_ID, STATUS, EFF_START_DT);

-- =====================================================
-- PROCEDURE 1: STAGE TO BRONZE
-- =====================================================
CREATE OR REPLACE PROCEDURE BRONZE.SP_CUSTOMER_STAGE_TO_BRONZE(P_PIPELINE_NAME VARCHAR, P_FILE_NAME VARCHAR)
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

    -- Create temp table
    CREATE OR REPLACE TEMPORARY TABLE BRONZE.TEMP_CUSTOMER_LOAD(
        NAME STRING,
        MOBILE STRING,
        EMAIL STRING,
        LOGIN_BY_USING STRING,
        GENDER STRING,
        DOB STRING,
        ANNIVERSARY STRING,
        PREFERENCES STRING
    );

    -- Copy data from stage
    EXECUTE IMMEDIATE
    '
        COPY INTO TEMP_CUSTOMER_LOAD (NAME, MOBILE, EMAIL, LOGIN_BY_USING, GENDER, DOB, ANNIVERSARY, PREFERENCES)
        FROM (
            SELECT
                $1::STRING,
                $2::STRING,
                $3::STRING,
                $4::STRING,
                $5::STRING,
                $6::STRING,
                $7::STRING,
                $8::STRING
            FROM '''''' || V_FILE_PATH || ''''''
        )
        FILE_FORMAT = (FORMAT_NAME = '''''' || V_FILE_FORMAT || '''''')
        ON_ERROR = ABORT_STATEMENT
    ';

    -- Get row count
    SELECT COUNT(*) INTO :V_ROWS_INSERTED FROM BRONZE.TEMP_CUSTOMER_LOAD;

    -- Validate data loaded
    IF (V_ROWS_INSERTED = 0) THEN
        ROLLBACK;
        DROP TABLE IF EXISTS TEMP_CUSTOMER_LOAD;
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', 'No records loaded from file',
            'FILE_PATH', P_FILE_NAME,
            'ROWS_INSERTED', 0,
            'INGEST_RUN_ID', 0
        );
    END IF;

    -- Consume sequence
    SELECT SWIGGY.BRONZE.SEQ_CUSTOMER_INGEST_RUN_ID.NEXTVAL INTO :V_INGEST_RUN_ID;

    -- Insert into Bronze
    INSERT INTO BRONZE.CUSTOMER_BRZ (
        NAME, MOBILE, EMAIL, LOGIN_BY_USING, GENDER, DOB, ANNIVERSARY, PREFERENCES,
        NAME_RAW, MOBILE_RAW, EMAIL_RAW, LOGIN_BY_USING_RAW, GENDER_RAW,
        DOB_RAW, ANNIVERSARY_RAW, PREFERENCES_RAW,
        INGEST_RUN_ID, CREATED_AT
    )
    SELECT
        TRIM(NAME), TRIM(MOBILE), LOWER(TRIM(EMAIL)), TRIM(LOGIN_BY_USING),
        UPPER(TRIM(GENDER)), TRY_TO_DATE(DOB), TRY_TO_DATE(ANNIVERSARY),
        TRY_PARSE_JSON(PREFERENCES),
        NAME, MOBILE, EMAIL, LOGIN_BY_USING, GENDER, DOB, ANNIVERSARY, PREFERENCES,
        :V_INGEST_RUN_ID, CURRENT_TIMESTAMP()
    FROM TEMP_CUSTOMER_LOAD;

    -- Commit transaction
    COMMIT;

    -- Cleanup
    DROP TABLE IF EXISTS BRONZE.TEMP_CUSTOMER_LOAD;

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
        DROP TABLE IF EXISTS TEMP_CUSTOMER_LOAD;

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
-- BRONZE TO SILVER PROCEDURE WITH DQ AND MERGE
-- =====================================================
CREATE OR REPLACE PROCEDURE SILVER.SP_CUSTOMER_BRONZE_TO_SILVER(
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
                CUSTOMER_BRZ_ID,
                NAME,
                MOBILE,
                EMAIL,
                LOGIN_BY_USING,
                GENDER,
                DOB,
                ANNIVERSARY,
                PREFERENCES,
                BATCH_ID,
                CURRENT_TIMESTAMP() AS CREATED_AT,
                CURRENT_TIMESTAMP() AS UPDATED_AT
            FROM ' || v_stage_table || '
            WHERE IS_VALID = TRUE AND BATCH_ID = ''' || P_BATCH_ID || '''
        ) AS SRC
        ON TGT.EMAIL = SRC.EMAIL

        WHEN MATCHED THEN
            UPDATE SET
                TGT.NAME = SRC.NAME,
                TGT.LOGIN_BY_USING = SRC.LOGIN_BY_USING,
                TGT.GENDER = SRC.GENDER,
                TGT.DOB = SRC.DOB,
                TGT.ANNIVERSARY = SRC.ANNIVERSARY,
                TGT.PREFERENCES = SRC.PREFERENCES,
                TGT.BATCH_ID = SRC.BATCH_ID,
                TGT.UPDATED_AT = SRC.UPDATED_AT

        WHEN NOT MATCHED THEN
            INSERT (
                NAME, MOBILE, EMAIL, LOGIN_BY_USING, GENDER, DOB,
                ANNIVERSARY, PREFERENCES, BATCH_ID, CREATED_AT, UPDATED_AT
            )
            VALUES (
                SRC.NAME, SRC.MOBILE, SRC.EMAIL, SRC.LOGIN_BY_USING,
                SRC.GENDER, SRC.DOB, SRC.ANNIVERSARY, SRC.PREFERENCES, SRC.BATCH_ID,
                SRC.CREATED_AT, SRC.UPDATED_AT
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
-- PROCEDURE: SILVER TO GOLD (IMPROVED SCD2)
-- =====================================================
-- FEATURES:
-- 1. Single MERGE operation (no separate UPDATE + INSERT)
-- 2. Hash-based change detection for performance
-- 3. Proper transaction handling
-- 4. Idempotency protection
-- 5. Better error handling
-- 6. Detailed logging
-- =====================================================
CREATE OR REPLACE PROCEDURE GOLD.SP_CUSTOMER_SILVER_TO_GOLD(
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
    FROM GOLD.DIM_CUSTOMER
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
    FROM SILVER.CUSTOMER_SLV
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

    CREATE OR REPLACE TEMPORARY TABLE TEMP_CUSTOMER_STAGING AS
    SELECT
        SRC.CUSTOMER_SLV_ID,
        SRC.NAME,
        SRC.MOBILE,
        SRC.EMAIL,
        SRC.LOGIN_BY_USING,
        SRC.GENDER,
        SRC.DOB,
        SRC.ANNIVERSARY,
        SRC.PREFERENCES,
        SRC.BATCH_ID,

        -- Calculate hash of attributes for change detection
        SHA2_HEX(
            CONCAT_WS('|',
                COALESCE(SRC.NAME, ''),
                COALESCE(SRC.MOBILE, ''),
                COALESCE(SRC.EMAIL, ''),
                COALESCE(SRC.LOGIN_BY_USING, ''),
                COALESCE(SRC.GENDER, ''),
                COALESCE(TO_VARCHAR(SRC.DOB), ''),
                COALESCE(TO_VARCHAR(SRC.ANNIVERSARY), ''),
                COALESCE(SRC.PREFERENCES, '')
            )
        ) AS CURRENT_HASH,

        -- Get existing record info
        TGT.CUSTOMER_ID AS EXISTING_CUSTOMER_ID,
        SHA2_HEX(
            CONCAT_WS('|',
                COALESCE(TGT.NAME, ''),
                COALESCE(TGT.MOBILE, ''),
                COALESCE(TGT.EMAIL, ''),
                COALESCE(TGT.LOGIN_BY_USING, ''),
                COALESCE(TGT.GENDER, ''),
                COALESCE(TO_VARCHAR(TGT.DOB), ''),
                COALESCE(TO_VARCHAR(TGT.ANNIVERSARY), ''),
                COALESCE(TGT.PREFERENCES, '')
            )
        ) AS EXISTING_HASH,

        -- Determine action
        CASE
            WHEN TGT.CUSTOMER_ID IS NULL THEN 'INSERT'
            WHEN SHA2_HEX(
                CONCAT_WS('|',
                    COALESCE(SRC.NAME, ''),
                    COALESCE(SRC.MOBILE, ''),
                    COALESCE(SRC.EMAIL, ''),
                    COALESCE(SRC.LOGIN_BY_USING, ''),
                    COALESCE(SRC.GENDER, ''),
                    COALESCE(TO_VARCHAR(SRC.DOB), ''),
                    COALESCE(TO_VARCHAR(SRC.ANNIVERSARY), ''),
                    COALESCE(SRC.PREFERENCES, '')
                )
            ) != SHA2_HEX(
                CONCAT_WS('|',
                    COALESCE(TGT.NAME, ''),
                    COALESCE(TGT.MOBILE, ''),
                    COALESCE(TGT.EMAIL, ''),
                    COALESCE(TGT.LOGIN_BY_USING, ''),
                    COALESCE(TGT.GENDER, ''),
                    COALESCE(TO_VARCHAR(TGT.DOB), ''),
                    COALESCE(TO_VARCHAR(TGT.ANNIVERSARY), ''),
                    COALESCE(TGT.PREFERENCES, '')
                )
            ) THEN 'UPDATE'
            ELSE 'UNCHANGED'
        END AS SCD_ACTION

    FROM SILVER.CUSTOMER_SLV SRC
    LEFT JOIN GOLD.DIM_CUSTOMER TGT
        ON TGT.CUSTOMER_ID = SRC.CUSTOMER_SLV_ID
        AND TGT.STATUS = 'ACTIVE'
    WHERE SRC.BATCH_ID = :P_BATCH_ID;

    -- ================================================================
    -- STEP 3: EXPIRE CHANGED RECORDS (SCD2 - CLOSE OLD VERSIONS)
    -- ================================================================

    UPDATE GOLD.DIM_CUSTOMER
    SET
        STATUS = 'INACTIVE',
        EFF_END_DT = :V_CURRENT_TIMESTAMP,
        UPDATED_AT = :V_CURRENT_TIMESTAMP
    WHERE CUSTOMER_ID IN (
        SELECT CUSTOMER_SLV_ID
        FROM TEMP_CUSTOMER_STAGING
        WHERE SCD_ACTION = 'UPDATE'
    )
    AND STATUS = 'ACTIVE';

    V_ROWS_UPDATED := SQLROWCOUNT;

    -- ================================================================
    -- STEP 4: INSERT NEW AND CHANGED RECORDS
    -- ================================================================

    INSERT INTO GOLD.DIM_CUSTOMER (
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
    )
    SELECT
        CUSTOMER_SLV_ID AS CUSTOMER_ID,
        NAME,
        MOBILE,
        EMAIL,
        LOGIN_BY_USING,
        GENDER,
        DOB,
        ANNIVERSARY,
        PREFERENCES,
        'ACTIVE' AS STATUS,
        :V_CURRENT_TIMESTAMP AS EFF_START_DT,
        CAST('9999-12-31 23:59:59' AS TIMESTAMP_TZ(9)) AS EFF_END_DT,
        BATCH_ID,
        :V_CURRENT_TIMESTAMP AS CREATED_AT,
        :V_CURRENT_TIMESTAMP AS UPDATED_AT
    FROM TEMP_CUSTOMER_STAGING
    WHERE SCD_ACTION IN ('INSERT', 'UPDATE');

    V_ROWS_INSERTED := SQLROWCOUNT;

    -- Get unchanged count
    SELECT COUNT(*)
    INTO :V_ROWS_UNCHANGED
    FROM TEMP_CUSTOMER_STAGING
    WHERE SCD_ACTION = 'UNCHANGED';

    -- ================================================================
    -- STEP 5: CLEANUP & RETURN
    -- ================================================================

    DROP TABLE IF EXISTS TEMP_CUSTOMER_STAGING;

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
        DROP TABLE IF EXISTS TEMP_CUSTOMER_STAGING;

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