-- ====================================================================================================
-- MENU
-- ====================================================================================================
-- CHANGE CONSTRING
USE DATABASE SWIGGY;
USE SCHEMA BRONZE;
USE WAREHOUSE ADHOC_WH;

-- ----------------------------------------------------------------------------------------------------
-- CREATE MENU_BRZ
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.MENU_BRZ (
    MENU_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    RESTAURANT_ID INTEGER,
    ITEM_NAME VARCHAR,
    DESCRIPTION VARCHAR,
    PRICE NUMBER(10, 2),
    CATEGORY VARCHAR,
    AVAILABILITY VARCHAR,
    ITEM_TYPE VARCHAR,

    RESTAURANT_ID_RAW VARCHAR,
    ITEM_NAME_RAW VARCHAR,
    DESCRIPTION_RAW VARCHAR,
    PRICE_RAW VARCHAR,
    CATEGORY_RAW VARCHAR,
    AVAILABILITY_RAW VARCHAR,
    ITEM_TYPE_RAW VARCHAR,

    -- AUDIT COLUMNS WITH APPROPRIATE DATA TYPES
    INGEST_RUN_ID INTEGER,
    CREATED_AT TIMESTAMP_TZ(9),
    UPDATED_AT TIMESTAMP_TZ(9)
)
COMMENT = 'THIS IS THE MENU STAGE/RAW TABLE WHERE DATA WILL BE COPIED FROM INTERNAL STAGE USING COPY COMMAND. THIS IS AS-IS DATA REPRESETATION FROM THE SOURCE LOCATION. ALL THE COLUMNS ARE STRING DATA TYPE EXCEPT THE AUDIT COLUMNS THAT ARE ADDED FOR TRACEABILITY.';
ALTER TABLE BRONZE.MENU_BRZ CLUSTER BY (INGEST_RUN_ID);

-- CREATING SEQUNCE TO GENERATE INGEST_RUN_ID
CREATE OR REPLACE SEQUENCE SEQ_MENU_INGEST_RUN_ID START = 1 INCREMENT = 1;

-- ----------------------------------------------------------------------------------------------------
-- CREATE MENU_LOAD_ERROR
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.MENU_LOAD_ERROR(
    ERROR_ID INTEGER PRIMARY KEY,
    VALIDATE_COLUMN VARCHAR(50),
    VALIDATION_TYPE VARCHAR(30),
    VALIDATION_ERROR_MSG VARCHAR(200),
    INGEST_RUN_ID INTEGER
);

-- ----------------------------------------------------------------------------------------------------
-- CREATE MENU_SLV
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.MENU_SLV (
    MENU_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    RESTAURANT_ID INTEGER,
    ITEM_NAME VARCHAR,
    DESCRIPTION VARCHAR,
    PRICE DECIMAL(10, 2),
    CATEGORY VARCHAR,
    AVAILABILITY BOOLEAN,
    ITEM_TYPE VARCHAR,
    CREATED_AT TIMESTAMP_TZ(9),
    UPDATED_AT TIMESTAMP_TZ(9),
    BATCH_ID VARCHAR(36)
)
COMMENT = 'MENU ENTITY UNDER CLEAN SCHEMA WITH APPROPRIATE DATA TYPE UNDER CLEAN SCHEMA LAYER, DATA IS POPULATED USING MERGE STATEMENT FROM THE STAGE LAYER LOCATION TABLE. THIS TABLE DOES NOT SUPPORT SCD2';

-- ----------------------------------------------------------------------------------------------------
-- CREATE DIM_MENU
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.DIM_MENU (
    MENU_ID INTEGER PRIMARY KEY,
    RESTAURANT_ID INTEGER,
    ITEM_NAME STRING,
    DESCRIPTION STRING,
    PRICE DECIMAL(10, 2),
    CATEGORY STRING,
    AVAILABILITY BOOLEAN,
    ITEM_TYPE STRING,
    EFF_START_DATE TIMESTAMP_TZ(9),
    EFF_END_DATE TIMESTAMP_TZ(9),
    STATUS BOOLEAN,
    CREATED_AT TIMESTAMP_TZ(9),
    UPDATED_AT TIMESTAMP_TZ(9),
    BATCH_ID VARCHAR(36)
)
COMMENT = 'THIS TABLE STORES THE DIMENSION DATA FOR THE MENU ITEMS, TRACKING HISTORICAL CHANGES USING SCD TYPE 2. EACH MENU ITEM HAS AN EFFECTIVE START AND END DATE, WITH A FLAG INDICATING IF IT IS THE CURRENT RECORD OR HISTORICAL. THE HASH KEY (MENU_DIM_HK) IS GENERATED BASED ON MENU_ID AND RESTAURANT_ID.';

-- =====================================================
-- PROCEDURE 1: STAGE TO BRONZE - MENU
-- =====================================================
CREATE OR REPLACE PROCEDURE BRONZE.SP_MENU_STAGE_TO_BRONZE(P_PIPELINE_NAME VARCHAR, P_FILE_NAME VARCHAR)
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
    CREATE OR REPLACE TEMPORARY TABLE TEMP_MENU_LOAD(
        RESTAURANT_ID STRING,
        ITEM_NAME STRING,
        DESCRIPTION STRING,
        PRICE STRING,
        CATEGORY STRING,
        AVAILABILITY STRING,
        ITEM_TYPE STRING
    );

    -- Copy data from stage
    EXECUTE IMMEDIATE
    '
    COPY INTO TEMP_MENU_LOAD(
        RESTAURANT_ID, ITEM_NAME, DESCRIPTION, PRICE, CATEGORY, AVAILABILITY, ITEM_TYPE)
    FROM (
        SELECT
            $1::STRING AS RESTAURANT_ID,
            $2::STRING AS ITEM_NAME,
            $3::STRING AS DESCRIPTION,
            $4::STRING AS PRICE,
            $5::STRING AS CATEGORY,
            $6::STRING AS AVAILABILITY,
            $7::STRING AS ITEM_TYPE
        FROM ''' || V_FILE_PATH || '''
    )
    FILE_FORMAT = (FORMAT_NAME = ''' || V_FILE_FORMAT || ''')
    ON_ERROR = ABORT_STATEMENT
    ';

    -- Get row count
    SELECT COUNT(*) INTO :V_ROWS_INSERTED FROM BRONZE.TEMP_MENU_LOAD;

    -- Validate data loaded
    IF (V_ROWS_INSERTED = 0) THEN
        ROLLBACK;
        DROP TABLE IF EXISTS TEMP_MENU_LOAD;
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', 'No records loaded from file',
            'FILE_PATH', P_FILE_NAME,
            'ROWS_INSERTED', 0,
            'INGEST_RUN_ID', 0
        );
    END IF;

    -- Consume sequence
    SELECT SWIGGY.BRONZE.SEQ_MENU_INGEST_RUN_ID.NEXTVAL INTO :V_INGEST_RUN_ID;

    -- Insert into bronze table
    INSERT INTO BRONZE.MENU_BRZ (
        RESTAURANT_ID,
        ITEM_NAME,
        DESCRIPTION,
        PRICE,
        CATEGORY,
        AVAILABILITY,
        ITEM_TYPE,
        RESTAURANT_ID_RAW,
        ITEM_NAME_RAW,
        DESCRIPTION_RAW,
        PRICE_RAW,
        CATEGORY_RAW,
        AVAILABILITY_RAW,
        ITEM_TYPE_RAW,
        INGEST_RUN_ID, CREATED_AT, UPDATED_AT
    )
    SELECT
        TRY_TO_NUMBER(RESTAURANT_ID),
        TO_VARCHAR(ITEM_NAME),
        TO_VARCHAR(DESCRIPTION),
        TRY_TO_NUMBER(PRICE, 10, 2),
        TO_VARCHAR(CATEGORY),
        TO_VARCHAR(AVAILABILITY),
        TO_VARCHAR(ITEM_TYPE),
        RESTAURANT_ID,
        ITEM_NAME,
        DESCRIPTION,
        PRICE,
        CATEGORY,
        AVAILABILITY,
        ITEM_TYPE,
        :V_INGEST_RUN_ID,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    FROM TEMP_MENU_LOAD;

    -- Commit transaction
    COMMIT;

    -- Cleanup
    DROP TABLE IF EXISTS BRONZE.TEMP_MENU_LOAD;

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
        DROP TABLE IF EXISTS TEMP_MENU_LOAD;

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
-- PROCEDURE 2: BRONZE TO SILVER - MENU
-- =====================================================
CREATE OR REPLACE PROCEDURE SILVER.SP_MENU_BRONZE_TO_SILVER(
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
                MENU_ID,
                RESTAURANT_ID,
                ITEM_NAME,
                DESCRIPTION,
                PRICE,
                CATEGORY,
                AVAILABILITY,
                ITEM_TYPE,
                BATCH_ID,
                CURRENT_TIMESTAMP() AS CREATED_AT,
                CURRENT_TIMESTAMP() AS UPDATED_AT
            FROM ' || v_stage_table || '
            WHERE IS_VALID = TRUE AND BATCH_ID = ''' || P_BATCH_ID || '''
        ) AS SRC
        ON TGT.RESTAURANT_ID = SRC.RESTAURANT_ID
        AND TGT.ITEM_NAME = SRC.ITEM_NAME
        WHEN MATCHED THEN
            UPDATE SET
                TGT.DESCRIPTION = SRC.DESCRIPTION,
                TGT.PRICE = SRC.PRICE,
                TGT.CATEGORY = SRC.CATEGORY,
                TGT.AVAILABILITY = SRC.AVAILABILITY,
                TGT.ITEM_TYPE = SRC.ITEM_TYPE,
                TGT.BATCH_ID = SRC.BATCH_ID,
                TGT.UPDATED_AT = SRC.UPDATED_AT
        WHEN NOT MATCHED THEN
            INSERT (
                RESTAURANT_ID, ITEM_NAME, DESCRIPTION, PRICE, CATEGORY, AVAILABILITY, ITEM_TYPE,
                BATCH_ID, CREATED_AT, UPDATED_AT
            )
            VALUES (
                SRC.RESTAURANT_ID, SRC.ITEM_NAME, SRC.DESCRIPTION, SRC.PRICE, SRC.CATEGORY,
                SRC.AVAILABILITY, SRC.ITEM_TYPE, SRC.BATCH_ID, SRC.CREATED_AT, SRC.UPDATED_AT
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
-- PROCEDURE 3: SILVER TO GOLD (WITH SCD2) - MENU
-- =====================================================
CREATE OR REPLACE PROCEDURE GOLD.SP_MENU_SILVER_TO_GOLD(
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
    FROM GOLD.DIM_MENU
    WHERE BATCH_ID = :P_BATCH_ID
    AND STATUS = TRUE
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
    FROM SILVER.MENU_SLV
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

    CREATE OR REPLACE TEMPORARY TABLE TEMP_MENU_STAGING AS
    SELECT
        SRC.MENU_ID,
        SRC.RESTAURANT_ID,
        SRC.ITEM_NAME,
        SRC.DESCRIPTION,
        SRC.PRICE,
        SRC.CATEGORY,
        SRC.AVAILABILITY,
        SRC.ITEM_TYPE,
        SRC.BATCH_ID,

        -- Calculate hash of attributes for change detection
        SHA2_HEX(
            CONCAT_WS('|',
                COALESCE(TO_VARCHAR(SRC.RESTAURANT_ID), ''),
                COALESCE(SRC.ITEM_NAME, ''),
                COALESCE(SRC.DESCRIPTION, ''),
                COALESCE(TO_VARCHAR(SRC.PRICE), ''),
                COALESCE(SRC.CATEGORY, ''),
                COALESCE(TO_VARCHAR(SRC.AVAILABILITY), ''),
                COALESCE(SRC.ITEM_TYPE, '')
            )
        ) AS CURRENT_HASH,

        -- Get existing record info
        TGT.MENU_ID AS EXISTING_MENU_ID,
        SHA2_HEX(
            CONCAT_WS('|',
                COALESCE(TO_VARCHAR(TGT.RESTAURANT_ID), ''),
                COALESCE(TGT.ITEM_NAME, ''),
                COALESCE(TGT.DESCRIPTION, ''),
                COALESCE(TO_VARCHAR(TGT.PRICE), ''),
                COALESCE(TGT.CATEGORY, ''),
                COALESCE(TO_VARCHAR(TGT.AVAILABILITY), ''),
                COALESCE(TGT.ITEM_TYPE, '')
            )
        ) AS EXISTING_HASH,

        -- Determine action
        CASE
            WHEN TGT.MENU_ID IS NULL THEN 'INSERT'
            WHEN SHA2_HEX(
                CONCAT_WS('|',
                    COALESCE(TO_VARCHAR(SRC.RESTAURANT_ID), ''),
                    COALESCE(SRC.ITEM_NAME, ''),
                    COALESCE(SRC.DESCRIPTION, ''),
                    COALESCE(TO_VARCHAR(SRC.PRICE), ''),
                    COALESCE(SRC.CATEGORY, ''),
                    COALESCE(TO_VARCHAR(SRC.AVAILABILITY), ''),
                    COALESCE(SRC.ITEM_TYPE, '')
                )
            ) != SHA2_HEX(
                CONCAT_WS('|',
                    COALESCE(TO_VARCHAR(TGT.RESTAURANT_ID), ''),
                    COALESCE(TGT.ITEM_NAME, ''),
                    COALESCE(TGT.DESCRIPTION, ''),
                    COALESCE(TO_VARCHAR(TGT.PRICE), ''),
                    COALESCE(TGT.CATEGORY, ''),
                    COALESCE(TO_VARCHAR(TGT.AVAILABILITY), ''),
                    COALESCE(TGT.ITEM_TYPE, '')
                )
            ) THEN 'UPDATE'
            ELSE 'UNCHANGED'
        END AS SCD_ACTION

    FROM SILVER.MENU_SLV SRC
    LEFT JOIN GOLD.DIM_MENU TGT
        ON TGT.MENU_ID = SRC.MENU_ID
        AND TGT.STATUS = TRUE
    WHERE SRC.BATCH_ID = :P_BATCH_ID;

    -- ================================================================
    -- STEP 3: EXPIRE CHANGED RECORDS (SCD2 - CLOSE OLD VERSIONS)
    -- ================================================================

    UPDATE GOLD.DIM_MENU
    SET
        STATUS = FALSE,
        EFF_END_DATE = :V_CURRENT_TIMESTAMP
    WHERE MENU_ID IN (
        SELECT MENU_ID
        FROM TEMP_MENU_STAGING
        WHERE SCD_ACTION = 'UPDATE'
    )
    AND STATUS = TRUE;

    V_ROWS_UPDATED := SQLROWCOUNT;

    -- ================================================================
    -- STEP 4: INSERT NEW AND CHANGED RECORDS
    -- ================================================================

    INSERT INTO GOLD.DIM_MENU (
        MENU_ID,
        RESTAURANT_ID,
        ITEM_NAME,
        DESCRIPTION,
        PRICE,
        CATEGORY,
        AVAILABILITY,
        ITEM_TYPE,
        EFF_START_DATE,
        EFF_END_DATE,
        STATUS,
        BATCH_ID
    )
    SELECT
        MENU_ID,
        RESTAURANT_ID,
        ITEM_NAME,
        DESCRIPTION,
        PRICE,
        CATEGORY,
        AVAILABILITY,
        ITEM_TYPE,
        :V_CURRENT_TIMESTAMP AS EFF_START_DATE,
        CAST('9999-12-31 23:59:59' AS TIMESTAMP_TZ) AS EFF_END_DATE,
        TRUE AS STATUS,
        BATCH_ID
    FROM TEMP_MENU_STAGING
    WHERE SCD_ACTION IN ('INSERT', 'UPDATE');

    V_ROWS_INSERTED := SQLROWCOUNT;

    -- Get unchanged count
    SELECT COUNT(*)
    INTO :V_ROWS_UNCHANGED
    FROM TEMP_MENU_STAGING
    WHERE SCD_ACTION = 'UNCHANGED';

    -- ================================================================
    -- STEP 5: CLEANUP & RETURN
    -- ================================================================

    DROP TABLE IF EXISTS TEMP_MENU_STAGING;

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
        DROP TABLE IF EXISTS TEMP_MENU_STAGING;

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
-- ====================================================================================================