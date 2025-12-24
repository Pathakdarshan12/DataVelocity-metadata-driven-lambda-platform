-- ====================================================================================================
-- DELIVERY
-- ====================================================================================================
-- CHANGE CONVARCHAR
USE DATABASE SWIGGY;
USE SCHEMA BRONZE;
USE WAREHOUSE ADHOC_WH;

-- ----------------------------------------------------------------------------------------------------
-- CREATE DELIVERY_BRZ
-- ----------------------------------------------------------------------------------------------------
-- BRONZE LAYER
CREATE OR REPLACE TABLE BRONZE.DELIVERY_BRZ (
    DELIVERY_ID VARCHAR,
    ORDER_ID VARCHAR,
    DELIVERY_AGENT_ID INTEGER,
    DELIVERY_STATUS VARCHAR,
    ESTIMATED_TIME VARCHAR,
    CUSTOMER_ADDRESS_ID INTEGER,
    DELIVERY_DATE TIMESTAMP_TZ,

    -- RAW COLUMNS
    ORDER_ID_RAW VARCHAR,
    DELIVERY_AGENT_ID_RAW VARCHAR,
    DELIVERY_STATUS_RAW VARCHAR,
    ESTIMATED_TIME_RAW VARCHAR,
    CUSTOMER_ADDRESS_ID_RAW VARCHAR,
    DELIVERY_DATE_RAW VARCHAR,

    -- AUDIT COLUMNS
    INGEST_RUN_ID VARCHAR,
    CREATED_AT VARCHAR,
    UPDATED_AT VARCHAR
);
ALTER TABLE BRONZE.DELIVERY_BRZ CLUSTER BY (INGEST_RUN_ID);

-- CREATING SEQUNCE TO GENERATE INGEST_RUN_ID
CREATE OR REPLACE SEQUENCE SEQ_DELIVERY_INGEST_RUN_ID START = 1 INCREMENT = 1;

-- ----------------------------------------------------------------------------------------------------
-- CREATE ORDER_ITEM_LOAD_ERROR
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.DELIVERY_LOAD_ERROR(
    ERROR_ID INTEGER PRIMARY KEY,
    VALIDATE_COLUMN VARCHAR(50),
    VALIDATION_TYPE VARCHAR(30),
    VALIDATION_ERROR_MSG VARCHAR(200),
    INGEST_RUN_ID INTEGER
);

-- ----------------------------------------------------------------------------------------------------
-- CREATE DELIVERY_SLV
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.DELIVERY_SLV (
    DELIVERY_ID VARCHAR PRIMARY KEY,
    ORDER_ID VARCHAR,
    DELIVERY_AGENT_ID INTEGER,
    DELIVERY_STATUS VARCHAR(50),
    ESTIMATED_TIME VARCHAR,
    CUSTOMER_ADDRESS_ID INTEGER,
    DELIVERY_DATE TIMESTAMP_TZ,
    CREATED_AT TIMESTAMP_TZ,
    UPDATED_AT TIMESTAMP_TZ,
    BATCH_ID STRING(50)
);

ALTER TABLE SILVER.DELIVERY_SLV CLUSTER BY (DATE(DELIVERY_DATE));

-- ----------------------------------------------------------------------------------------------------
-- CREATE FACT_DELIVERY
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.FACT_DELIVERY (
    DELIVERY_ID VARCHAR PRIMARY KEY,
    ORDER_ID VARCHAR,
    DELIVERY_AGENT_ID INTEGER,
    CUSTOMER_ADDRESS_ID INTEGER,
    CURRENT_STATUS STRING(50),
    INITIAL_STATUS STRING(50),
    ESTIMATED_TIME VARCHAR,
    DELIVERY_DATE TIMESTAMP_TZ,
    STATUS_UPDATED_AT TIMESTAMP_TZ,
    BATCH_ID STRING(50),
    CREATED_AT TIMESTAMP_TZ,
    LOADED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'FACT TABLE FOR DELIVERIES WITH STATUS TRACKING';

ALTER TABLE GOLD.FACT_DELIVERY CLUSTER BY (DATE(DELIVERY_DATE));

-- ----------------------------------------------------------------------------------------------------
-- STATUS HISTORY TABLES (FOR DETAILED AUDIT TRAIL)
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.FACT_DELIVERY_STATUS_HISTORY (
    STATUS_HISTORY_KEY INTEGER PRIMARY KEY AUTOINCREMENT,
    DELIVERY_ID INTEGER,
    OLD_STATUS STRING(50),
    NEW_STATUS STRING(50),
    STATUS_CHANGED_AT TIMESTAMP_TZ,
    BATCH_ID STRING(36)
)
COMMENT = 'AUDIT TRAIL FOR DELIVERY STATUS CHANGES';

-- =====================================================
-- DELIVERY PROCEDURES
-- =====================================================

-- ----------------------------------------------------------------------------------------------------
-- PROCEDURE: DELIVERY STAGE TO BRONZE
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE BRONZE.SP_DELIVERY_STAGE_TO_BRONZE(P_PIPELINE_NAME VARCHAR, P_FILE_NAME VARCHAR)
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
    CREATE OR REPLACE TEMPORARY TABLE TEMP_DELIVERY_LOAD(
        DELIVERY_ID VARCHAR,
        ORDER_ID VARCHAR,
        DELIVERY_AGENT_ID VARCHAR,
        DELIVERY_STATUS VARCHAR,
        ESTIMATED_TIME VARCHAR,
        CUSTOMER_ADDRESS_ID VARCHAR,
        DELIVERY_DATE VARCHAR
    );

    EXECUTE IMMEDIATE
    '
        COPY INTO TEMP_DELIVERY_LOAD (
            ORDER_ID, DELIVERY_AGENT_ID, DELIVERY_STATUS, ESTIMATED_TIME, CUSTOMER_ADDRESS_ID, DELIVERY_DATE
        )
        FROM (
            SELECT
                $1::STRING AS ORDER_ID,
                $2::STRING AS DELIVERY_AGENT_ID,
                $3::STRING AS DELIVERY_STATUS,
                $4::STRING AS ESTIMATED_TIME,
                $5::STRING AS CUSTOMER_ADDRESS_ID,
                $6::STRING AS DELIVERY_DATE
        FROM ''' || V_FILE_PATH || '''
    )
    FILE_FORMAT = (FORMAT_NAME = ''' || V_FILE_FORMAT || ''')
    ON_ERROR = ABORT_STATEMENT
    ';

    -- Get row count
    SELECT COUNT(*) INTO :V_ROWS_INSERTED FROM BRONZE.TEMP_DELIVERY_LOAD;

    -- Validate data loaded
    IF (V_ROWS_INSERTED = 0) THEN
        ROLLBACK;
        DROP TABLE IF EXISTS TEMP_DELIVERY_LOAD;
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', 'No records loaded from file',
            'FILE_PATH', P_FILE_NAME,
            'ROWS_INSERTED', 0,
            'INGEST_RUN_ID', 0
        );
    END IF;

    -- Consume sequence
    SELECT SWIGGY.BRONZE.SEQ_DELIVERY_INGEST_RUN_ID.NEXTVAL INTO :V_INGEST_RUN_ID;

    INSERT INTO BRONZE.DELIVERY_BRZ (
        DELIVERY_ID,
        ORDER_ID,
        DELIVERY_AGENT_ID,
        DELIVERY_STATUS,
        ESTIMATED_TIME,
        CUSTOMER_ADDRESS_ID,
        DELIVERY_DATE,
        ORDER_ID_RAW,
        DELIVERY_AGENT_ID_RAW,
        DELIVERY_STATUS_RAW,
        ESTIMATED_TIME_RAW,
        CUSTOMER_ADDRESS_ID_RAW,
        DELIVERY_DATE_RAW,
        INGEST_RUN_ID,
        CREATED_AT,
        UPDATED_AT
    )
    SELECT
        UUID_STRING(),
        TO_VARCHAR(ORDER_ID),
        TRY_TO_NUMBER(DELIVERY_AGENT_ID),
        TO_VARCHAR(DELIVERY_STATUS),
        TO_VARCHAR(ESTIMATED_TIME),
        TRY_TO_NUMBER(CUSTOMER_ADDRESS_ID),
        TRY_TO_TIMESTAMP (DELIVERY_DATE),
        ORDER_ID,
        DELIVERY_AGENT_ID,
        DELIVERY_STATUS,
        ESTIMATED_TIME,
        CUSTOMER_ADDRESS_ID,
        DELIVERY_DATE,
        :V_INGEST_RUN_ID,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    FROM TEMP_DELIVERY_LOAD;

    -- Commit transaction
    COMMIT;

    -- Cleanup
    DROP TABLE IF EXISTS BRONZE.TEMP_ORDER_LOAD;

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
        DROP TABLE IF EXISTS TEMP_ORDER_LOAD;

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

-- ----------------------------------------------------------------------------------------------------
-- PROCEDURE: DELIVERY BRONZE TO SILVER
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SILVER.SP_DELIVERY_BRONZE_TO_SILVER(
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

    -- Simple INSERT for order items (append-only)
    v_sql := '
    MERGE INTO SILVER.DELIVERY_SLV AS TGT
    USING (
        SELECT
            DELIVERY_ID,
            ORDER_ID,
            DELIVERY_AGENT_ID,
            DELIVERY_STATUS,
            ESTIMATED_TIME,
            CUSTOMER_ADDRESS_ID,
            DELIVERY_DATE,
            CREATED_AT,
            UPDATED_AT,
            ?
        FROM BRONZE.DELIVERY_BRZ
        WHERE INGEST_RUN_ID = ?
    ) AS SRC
    ON TGT.DELIVERY_ID = SRC.DELIVERY_ID

    WHEN MATCHED THEN
        UPDATE SET
            TGT.DELIVERY_STATUS = SRC.DELIVERY_STATUS,
            TGT.DELIVERY_DATE = SRC.DELIVERY_DATE,
            TGT.UPDATED_AT = SRC.UPDATED_AT

    WHEN NOT MATCHED THEN
        INSERT (
            DELIVERY_ID, ORDER_ID, DELIVERY_AGENT_ID, DELIVERY_STATUS,
            ESTIMATED_TIME, CUSTOMER_ADDRESS_ID, DELIVERY_DATE, CREATED_AT, UPDATED_AT, BATCH_ID
        )
        VALUES (
            SRC.DELIVERY_ID, SRC.ORDER_ID, SRC.DELIVERY_AGENT_ID, SRC.DELIVERY_STATUS,
            SRC.ESTIMATED_TIME, SRC.CUSTOMER_ADDRESS_ID, SRC.DELIVERY_DATE,
            SRC.CREATED_AT, SRC.UPDATED_AT, ?
    )';

    EXECUTE IMMEDIATE v_sql USING (P_INGEST_RUN_ID, P_INGEST_RUN_ID, P_BATCH_ID);

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

-- ----------------------------------------------------------------------------------------------------
-- PROCEDURE: DELIVERY SILVER TO GOLD (WITH STATUS TRACKING)
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE GOLD.SP_DELIVERY_SILVER_TO_GOLD(P_BATCH_ID STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    V_CURRENT_TIMESTAMP TIMESTAMP_TZ;
    V_ROWS_INSERTED INTEGER DEFAULT 0;
    V_ROWS_UPDATED INTEGER DEFAULT 0;
    V_STATUS_CHANGES INTEGER DEFAULT 0;
    V_ERROR_MESSAGE VARCHAR(5000);
    V_START_TIME TIMESTAMP_TZ;
    V_END_TIME TIMESTAMP_TZ;
    V_EXECUTION_DURATION INTEGER;
BEGIN
    V_START_TIME := CURRENT_TIMESTAMP();
    V_CURRENT_TIMESTAMP := CURRENT_TIMESTAMP();

    -- Start explicit transaction
    BEGIN TRANSACTION;

    -- Step 1: Update existing deliveries if status changed

    UPDATE GOLD.FACT_DELIVERY AS TGT
    SET
        CURRENT_STATUS = SRC.DELIVERY_STATUS,
        DELIVERY_DATE = SRC.DELIVERY_DATE,
        STATUS_UPDATED_AT = :V_CURRENT_TIMESTAMP
    FROM (
        SELECT
            DELIVERY_ID, DELIVERY_STATUS, DELIVERY_DATE
        FROM SILVER.DELIVERY_SLV
        WHERE BATCH_ID = :P_BATCH_ID
    ) AS SRC
    WHERE TGT.DELIVERY_ID = SRC.DELIVERY_ID
        AND TGT.CURRENT_STATUS != SRC.DELIVERY_STATUS;

    V_ROWS_UPDATED := SQLROWCOUNT;

    -- Step 2: Log status changes to history table
    INSERT INTO GOLD.FACT_DELIVERY_STATUS_HISTORY (
        DELIVERY_ID, OLD_STATUS, NEW_STATUS, STATUS_CHANGED_AT, BATCH_ID
    )
    SELECT
        TGT.DELIVERY_ID,
        TGT.CURRENT_STATUS AS OLD_STATUS,
        SRC.DELIVERY_STATUS AS NEW_STATUS,
        :V_CURRENT_TIMESTAMP AS STATUS_CHANGED_AT,
        :P_BATCH_ID
    FROM GOLD.FACT_DELIVERY TGT
    INNER JOIN SILVER.DELIVERY_SLV SRC ON TGT.DELIVERY_ID = SRC.DELIVERY_ID
    WHERE SRC.BATCH_ID = :P_BATCH_ID
        AND TGT.CURRENT_STATUS != SRC.DELIVERY_STATUS;

    -- Step 3: Insert new deliveries
    INSERT INTO GOLD.FACT_DELIVERY (
        DELIVERY_ID, ORDER_ID, DELIVERY_AGENT_ID, CUSTOMER_ADDRESS_ID,
        CURRENT_STATUS, INITIAL_STATUS, ESTIMATED_TIME, DELIVERY_DATE,
        STATUS_UPDATED_AT, BATCH_ID, CREATED_AT
    )
    SELECT
        SRC.DELIVERY_ID,
        SRC.ORDER_ID,
        SRC.DELIVERY_AGENT_ID,
        SRC.CUSTOMER_ADDRESS_ID,
        SRC.DELIVERY_STATUS AS CURRENT_STATUS,
        SRC.DELIVERY_STATUS AS INITIAL_STATUS,
        SRC.ESTIMATED_TIME,
        SRC.DELIVERY_DATE,
        :V_CURRENT_TIMESTAMP AS STATUS_UPDATED_AT,
        SRC.BATCH_ID,
        SRC.CREATED_AT
    FROM SILVER.DELIVERY_SLV SRC
    WHERE SRC.BATCH_ID = :P_BATCH_ID
        AND NOT EXISTS (
            SELECT 1 FROM GOLD.FACT_DELIVERY TGT
            WHERE TGT.DELIVERY_ID = SRC.DELIVERY_ID
        );

    -- Get target row count for this batch
    SELECT COUNT(*) INTO :V_ROWS_INSERTED FROM GOLD.FACT_ORDER_ITEM WHERE BATCH_ID = :P_BATCH_ID;

    -- Commit transaction
    COMMIT;

    V_END_TIME := CURRENT_TIMESTAMP();
    V_EXECUTION_DURATION := DATEDIFF(SECOND, V_START_TIME, V_END_TIME);

    RETURN OBJECT_CONSTRUCT(
        'STATUS', 'SUCCESS',
        'ERROR', 'NONE',
        'ROWS_INSERTED', V_ROWS_INSERTED::VARCHAR,
        'ROWS_UPDATED', V_ROWS_UPDATED::VARCHAR,
        'STATUS_CHANGES_LOGGED', V_STATUS_CHANGES::VARCHAR,
        'EXECUTION_TIME_SEC', V_EXECUTION_DURATION::VARCHAR,
        'BATCH_ID', P_BATCH_ID,
        'PROCESSED_AT', V_CURRENT_TIMESTAMP::VARCHAR
    );

EXCEPTION
    WHEN OTHER THEN
        -- Rollback transaction on error
        ROLLBACK;

        V_ERROR_MESSAGE := SQLERRM;
        V_END_TIME := CURRENT_TIMESTAMP();
        V_EXECUTION_DURATION := DATEDIFF(SECOND, V_START_TIME, V_END_TIME);

        -- Cleanup temp table
        DROP TABLE IF EXISTS TEMP_STATUS_CHANGES;

        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', V_ERROR_MESSAGE,
            'ROWS_INSERTED', V_ROWS_INSERTED::VARCHAR,
            'ROWS_UPDATED', V_ROWS_UPDATED::VARCHAR,
            'STATUS_CHANGES_LOGGED', V_STATUS_CHANGES::VARCHAR,
            'EXECUTION_TIME_SEC', V_EXECUTION_DURATION::VARCHAR,
            'BATCH_ID', P_BATCH_ID,
            'NOTE', 'Transaction rolled back'
        );
END;
$$;
-- ----------------------------------------------------------------------------------------------------