-- ====================================================================================================
-- DELIVERY_AGENT
-- ====================================================================================================
-- CHANGE CONVARCHAR
USE DATABASE SWIGGY;
USE SCHEMA BRONZE;
USE WAREHOUSE ADHOC_WH;

-- ----------------------------------------------------------------------------------------------------
-- CREATE DELIVERY_AGENT_BRZ
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.DELIVERY_AGENT_BRZ (
    DELIVERY_AGENT_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    DELIVERY_AGENT_NAME VARCHAR,
    PHONE INTEGER,
    VEHICLE_TYPE VARCHAR,
    LOCATION_ID INTEGER,
    IS_ACTIVE VARCHAR,
    GENDER VARCHAR,
    RATING NUMBER(2,1),

    DELIVERY_AGENT_NAME_RAW VARCHAR,
    PHONE_RAW VARCHAR,
    VEHICLE_TYPE_RAW VARCHAR,
    LOCATION_ID_RAW VARCHAR,
    IS_ACTIVE_RAW VARCHAR,
    GENDER_RAW VARCHAR,
    RATING_RAW VARCHAR,

    INGEST_RUN_ID INTEGER,
    CREATED_AT VARCHAR,
    UPDATED_AT VARCHAR
)
COMMENT = 'THIS IS THE DELIVERY STAGE/RAW TABLE WHERE DATA WILL BE COPIED FROM INTERNAL STAGE USING COPY COMMAND. THIS IS AS-IS DATA REPRESETATION FROM THE SOURCE LOCATION. ALL THE COLUMNS ARE VARCHAR DATA TYPE EXCEPT THE AUDIT COLUMNS THAT ARE ADDED FOR TRACEABILITY.';
ALTER TABLE BRONZE.DELIVERY_AGENT_BRZ CLUSTER BY (INGEST_RUN_ID);

-- CREATING SEQUNCE TO GENERATE INGEST_RUN_ID
CREATE OR REPLACE SEQUENCE SEQ_DELIVERY_AGENT_INGEST_RUN_ID START = 1 INCREMENT = 1;

-- ----------------------------------------------------------------------------------------------------
-- CREATE DELIVERY_AGENT_LOAD_ERROR
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.DELIVERY_AGENT_LOAD_ERROR(
    ERROR_ID INTEGER PRIMARY KEY,
    VALIDATE_COLUMN VARCHAR(50),
    VALIDATION_TYPE VARCHAR(30),
    VALIDATION_ERROR_MSG VARCHAR(200),
    INGEST_RUN_ID INTEGER
);

-- ----------------------------------------------------------------------------------------------------
-- CREATE DELIVERY_AGENT_SLV
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.DELIVERY_AGENT_SLV (
    DELIVERY_AGENT_ID INTEGER PRIMARY KEY,
    DELIVERY_AGENT_NAME VARCHAR,
    PHONE INTEGER,
    VEHICLE_TYPE VARCHAR,
    LOCATION_ID INT,
    IS_ACTIVE VARCHAR,
    GENDER VARCHAR,
    RATING NUMBER(2,1),

    -- AUDIT COLUMNS
    BATCH_ID VARCHAR(36),
    CREATED_AT TIMESTAMP_NTZ,
    UPDATED_AT TIMESTAMP_NTZ
)
COMMENT = 'DELIVERY ENTITY UNDER CLEAN SCHEMA WITH APPROPRIATE DATA TYPE UNDER CLEAN SCHEMA LAYER, DATA IS POPULATED USING MERGE STATEMENT FROM THE STAGE LAYER LOCATION TABLE. THIS TABLE DOES NOT SUPPORT SCD2';

-- ----------------------------------------------------------------------------------------------------
-- CREATE DELIVERY_AGENT_GLD
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.DIM_DELIVERY_AGENT (
    DELIVERY_AGENT_ID INTEGER PRIMARY KEY,
    DELIVERY_AGENT_NAME VARCHAR,
    PHONE VARCHAR,
    VEHICLE_TYPE VARCHAR,
    LOCATION_ID INTEGER,
    IS_ACTIVE VARCHAR,
    GENDER VARCHAR,
    RATING NUMBER(2,1),
    EFF_START_DATE TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP,
    EFF_END_DATE TIMESTAMP_TZ,
    STATUS VARCHAR,
    BATCH_ID VARCHAR(36)
)
COMMENT =  'DIM TABLE FOR DELIVERY AGENT ENTITY WITH SCD2 SUPPORT.';

-- =====================================================
-- PROCEDURE 1: STAGE TO BRONZE - DELIVERY AGENT
-- =====================================================
CREATE OR REPLACE PROCEDURE BRONZE.SP_DELIVERY_AGENT_STAGE_TO_BRONZE(P_PIPELINE_NAME VARCHAR, P_FILE_NAME VARCHAR)
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
    CREATE OR REPLACE TEMPORARY TABLE TEMP_DELIVERY_AGENT_LOAD(
        DELIVERY_AGENT_NAME STRING,
        PHONE STRING,
        VEHICLE_TYPE STRING,
        LOCATION_ID STRING,
        IS_ACTIVE STRING,
        GENDER STRING,
        RATING STRING
    );

    -- Copy data from stage
    EXECUTE IMMEDIATE
    '
    COPY INTO TEMP_DELIVERY_AGENT_LOAD(
        DELIVERY_AGENT_NAME, PHONE, VEHICLE_TYPE, LOCATION_ID, IS_ACTIVE, GENDER, RATING)
    FROM (
        SELECT
            $1::STRING AS DELIVERY_AGENT_NAME,
            $2::STRING AS PHONE,
            $3::STRING AS VEHICLE_TYPE,
            $4::STRING AS LOCATION_ID,
            $5::STRING AS IS_ACTIVE,
            $6::STRING AS GENDER,
            $7::STRING AS RATING
        FROM ''' || V_FILE_PATH || '''
    )
    FILE_FORMAT = (FORMAT_NAME = ''' || V_FILE_FORMAT || ''')
    ON_ERROR = ABORT_STATEMENT
    ';

    -- Get row count
    SELECT COUNT(*) INTO :V_ROWS_INSERTED FROM BRONZE.TEMP_DELIVERY_AGENT_LOAD;

    -- Validate data loaded
    IF (V_ROWS_INSERTED = 0) THEN
        ROLLBACK;
        DROP TABLE IF EXISTS TEMP_DELIVERY_AGENT_LOAD;
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', 'No records loaded from file',
            'FILE_PATH', P_FILE_NAME,
            'ROWS_INSERTED', 0,
            'INGEST_RUN_ID', 0
        );
    END IF;

    -- Consume sequence
    SELECT SWIGGY.BRONZE.SEQ_DELIVERY_AGENT_INGEST_RUN_ID.NEXTVAL INTO :V_INGEST_RUN_ID;

    -- Insert into bronze table (AUTOINCREMENT handles DELIVERY_AGENT_ID)
    INSERT INTO BRONZE.DELIVERY_AGENT_BRZ (
        DELIVERY_AGENT_NAME,
        PHONE,
        VEHICLE_TYPE,
        LOCATION_ID,
        IS_ACTIVE,
        GENDER,
        RATING,
        DELIVERY_AGENT_NAME_RAW,
        PHONE_RAW,
        VEHICLE_TYPE_RAW,
        LOCATION_ID_RAW,
        IS_ACTIVE_RAW,
        GENDER_RAW,
        RATING_RAW,
        INGEST_RUN_ID, CREATED_AT, UPDATED_AT
    )
    SELECT
        TO_VARCHAR(DELIVERY_AGENT_NAME),
        TRY_TO_NUMBER(PHONE),
        TO_VARCHAR(VEHICLE_TYPE),
        TRY_TO_NUMBER(LOCATION_ID),
        TO_VARCHAR(IS_ACTIVE),
        TO_VARCHAR(GENDER),
        TRY_TO_NUMBER(RATING, 2, 1),
        DELIVERY_AGENT_NAME,
        PHONE,
        VEHICLE_TYPE,
        LOCATION_ID,
        IS_ACTIVE,
        GENDER,
        RATING,
        :V_INGEST_RUN_ID,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    FROM TEMP_DELIVERY_AGENT_LOAD;

    -- Commit transaction
    COMMIT;

    -- Cleanup
    DROP TABLE IF EXISTS BRONZE.TEMP_DELIVERY_AGENT_LOAD;

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
        DROP TABLE IF EXISTS TEMP_DELIVERY_AGENT_LOAD;

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
-- PROCEDURE 2: BRONZE TO SILVER - DELIVERY AGENT
-- =====================================================
CREATE OR REPLACE PROCEDURE SILVER.SP_DELIVERY_AGENT_BRONZE_TO_SILVER(
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
                DELIVERY_AGENT_ID,
                DELIVERY_AGENT_NAME,
                PHONE,
                VEHICLE_TYPE,
                LOCATION_ID,
                IS_ACTIVE,
                GENDER,
                RATING,
                BATCH_ID,
                CURRENT_TIMESTAMP() AS CREATED_AT,
                CURRENT_TIMESTAMP() AS UPDATED_AT
            FROM ' || v_stage_table || '
            WHERE IS_VALID = TRUE AND BATCH_ID = ''' || P_BATCH_ID || '''
        ) AS SRC
        ON TGT.DELIVERY_AGENT_ID = SRC.DELIVERY_AGENT_ID
        WHEN MATCHED THEN
            UPDATE SET
                TGT.DELIVERY_AGENT_NAME = SRC.DELIVERY_AGENT_NAME,
                TGT.PHONE = SRC.PHONE,
                TGT.VEHICLE_TYPE = SRC.VEHICLE_TYPE,
                TGT.LOCATION_ID = SRC.LOCATION_ID,
                TGT.IS_ACTIVE = SRC.IS_ACTIVE,
                TGT.GENDER = SRC.GENDER,
                TGT.RATING = SRC.RATING,
                TGT.BATCH_ID = SRC.BATCH_ID,
                TGT.UPDATED_AT = SRC.UPDATED_AT
        WHEN NOT MATCHED THEN
            INSERT (
                DELIVERY_AGENT_ID, DELIVERY_AGENT_NAME, PHONE, VEHICLE_TYPE, LOCATION_ID,
                IS_ACTIVE, GENDER, RATING, BATCH_ID, CREATED_AT, UPDATED_AT
            )
            VALUES (
                SRC.DELIVERY_AGENT_ID, SRC.DELIVERY_AGENT_NAME, SRC.PHONE, SRC.VEHICLE_TYPE,
                SRC.LOCATION_ID, SRC.IS_ACTIVE, SRC.GENDER, SRC.RATING,
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
-- PROCEDURE 3: SILVER TO GOLD (WITH SCD2) - DELIVERY AGENT
-- =====================================================
CREATE OR REPLACE PROCEDURE GOLD.SP_DELIVERY_AGENT_SILVER_TO_GOLD(
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
    FROM GOLD.DIM_DELIVERY_AGENT
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
    FROM SILVER.DELIVERY_AGENT_SLV
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

    CREATE OR REPLACE TEMPORARY TABLE TEMP_DELIVERY_AGENT_STAGING AS
    SELECT
        SRC.DELIVERY_AGENT_ID,
        SRC.DELIVERY_AGENT_NAME,
        SRC.PHONE,
        SRC.VEHICLE_TYPE,
        SRC.LOCATION_ID,
        SRC.IS_ACTIVE,
        SRC.GENDER,
        SRC.RATING,
        SRC.BATCH_ID,

        -- Calculate hash of attributes for change detection
        SHA2_HEX(
            CONCAT_WS('|',
                COALESCE(SRC.DELIVERY_AGENT_NAME, ''),
                COALESCE(SRC.VEHICLE_TYPE, ''),
                COALESCE(TO_VARCHAR(SRC.LOCATION_ID), ''),
                COALESCE(SRC.IS_ACTIVE, ''),
                COALESCE(SRC.GENDER, ''),
                COALESCE(TO_VARCHAR(SRC.RATING), '')
            )
        ) AS CURRENT_HASH,

        -- Get existing record info from target (PHONE is already VARCHAR in Gold)
        TGT.DELIVERY_AGENT_ID AS EXISTING_DELIVERY_AGENT_ID,
        SHA2_HEX(
            CONCAT_WS('|',
                COALESCE(TGT.DELIVERY_AGENT_NAME, ''),
                COALESCE(TGT.VEHICLE_TYPE, ''),
                COALESCE(TO_VARCHAR(TGT.LOCATION_ID), ''),
                COALESCE(TGT.IS_ACTIVE, ''),
                COALESCE(TGT.GENDER, ''),
                COALESCE(TO_VARCHAR(TGT.RATING), '')
            )
        ) AS EXISTING_HASH,

        -- Determine action
        CASE
            WHEN TGT.DELIVERY_AGENT_ID IS NULL THEN 'INSERT'
            WHEN SHA2_HEX(
                CONCAT_WS('|',
                    COALESCE(SRC.DELIVERY_AGENT_NAME, ''),
                    COALESCE(SRC.VEHICLE_TYPE, ''),
                    COALESCE(TO_VARCHAR(SRC.LOCATION_ID), ''),
                    COALESCE(SRC.IS_ACTIVE, ''),
                    COALESCE(SRC.GENDER, ''),
                    COALESCE(TO_VARCHAR(SRC.RATING), '')
                )
            ) != SHA2_HEX(
                CONCAT_WS('|',
                    COALESCE(TGT.DELIVERY_AGENT_NAME, ''),
                    COALESCE(TGT.VEHICLE_TYPE, ''),
                    COALESCE(TO_VARCHAR(TGT.LOCATION_ID), ''),
                    COALESCE(TGT.IS_ACTIVE, ''),
                    COALESCE(TGT.GENDER, ''),
                    COALESCE(TO_VARCHAR(TGT.RATING), '')
                )
            ) THEN 'UPDATE'
            ELSE 'UNCHANGED'
        END AS SCD_ACTION

    FROM SILVER.DELIVERY_AGENT_SLV SRC
    LEFT JOIN GOLD.DIM_DELIVERY_AGENT TGT
        ON TGT.DELIVERY_AGENT_ID = SRC.DELIVERY_AGENT_ID
        AND TGT.STATUS = 'ACTIVE'
    WHERE SRC.BATCH_ID = :P_BATCH_ID;

    -- ================================================================
    -- STEP 3: EXPIRE CHANGED RECORDS (SCD2 - CLOSE OLD VERSIONS)
    -- ================================================================

    UPDATE GOLD.DIM_DELIVERY_AGENT
    SET
        STATUS = 'INACTIVE',
        EFF_END_DATE = :V_CURRENT_TIMESTAMP
    WHERE DELIVERY_AGENT_ID IN (
        SELECT DELIVERY_AGENT_ID
        FROM TEMP_DELIVERY_AGENT_STAGING
        WHERE SCD_ACTION = 'UPDATE'
    )
    AND STATUS = 'ACTIVE';

    V_ROWS_UPDATED := SQLROWCOUNT;

    -- ================================================================
    -- STEP 4: INSERT NEW AND CHANGED RECORDS
    -- ================================================================

    INSERT INTO GOLD.DIM_DELIVERY_AGENT (
        DELIVERY_AGENT_ID,
        DELIVERY_AGENT_NAME,
        PHONE,
        VEHICLE_TYPE,
        LOCATION_ID,
        IS_ACTIVE,
        GENDER,
        RATING,
        STATUS,
        EFF_START_DATE,
        EFF_END_DATE,
        BATCH_ID
    )
    SELECT
        DELIVERY_AGENT_ID,
        DELIVERY_AGENT_NAME,
        TO_VARCHAR(PHONE),
        VEHICLE_TYPE,
        LOCATION_ID,
        IS_ACTIVE,
        GENDER,
        RATING,
        'ACTIVE' AS STATUS,
        :V_CURRENT_TIMESTAMP AS EFF_START_DATE,
        CAST('9999-12-31 23:59:59' AS TIMESTAMP_TZ(9)) AS EFF_END_DATE,
        BATCH_ID
    FROM TEMP_DELIVERY_AGENT_STAGING
    WHERE SCD_ACTION IN ('INSERT', 'UPDATE');

    V_ROWS_INSERTED := SQLROWCOUNT;

    -- Get unchanged count
    SELECT COUNT(*)
    INTO :V_ROWS_UNCHANGED
    FROM TEMP_DELIVERY_AGENT_STAGING
    WHERE SCD_ACTION = 'UNCHANGED';

    -- ================================================================
    -- STEP 5: CLEANUP & RETURN
    -- ================================================================

    DROP TABLE IF EXISTS TEMP_DELIVERY_AGENT_STAGING;

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
        DROP TABLE IF EXISTS TEMP_DELIVERY_AGENT_STAGING;

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