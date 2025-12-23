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
    ORDER_ITEM_ID VARCHAR PRIMARY KEY,
    ORDER_ID VARCHAR,
    MENU_ID INTEGER,
    QUANTITY NUMBER(10,2),
    PRICE NUMBER(10,2),
    SUBTOTAL NUMBER(10,2),
    ORDER_TIMESTAMP DATE,

    --RAW COLUMNS
    ORDER_ID_RAW VARCHAR,
    MENU_ID_RAW VARCHAR,
    QUANTITY_RAW VARCHAR,
    PRICE_RAW VARCHAR,
    SUBTOTAL_RAW VARCHAR,
    ORDER_TIMESTAMP_RAW VARCHAR,

    -- AUDIT COLUMNS
    INGEST_RUN_ID INTEGER,
    CREATED_AT TIMESTAMP_TZ,
    UPDATED_AT TIMESTAMP_TZ
);
ALTER TABLE BRONZE.ORDER_ITEM_BRZ CLUSTER BY (INGEST_RUN_ID);

-- CREATING SEQUNCE TO GENERATE INGEST_RUN_ID
CREATE OR REPLACE SEQUENCE SEQ_ORDER_ITEM_INGEST_RUN_ID START = 1 INCREMENT = 1;

-- ----------------------------------------------------------------------------------------------------
-- CREATE ORDER_ITEM_LOAD_ERROR
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.ORDER_ITEM_LOAD_ERROR(
    ERROR_ID INTEGER PRIMARY KEY,
    VALIDATE_COLUMN VARCHAR(50),
    VALIDATION_TYPE VARCHAR(30),
    VALIDATION_ERROR_MSG VARCHAR(200),
    INGEST_RUN_ID INTEGER
);

-- ----------------------------------------------------------------------------------------------------
-- CREATE ORDER_ITEM_SLV
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.ORDER_ITEM_SLV (
    ORDER_ITEM_ID VARCHAR PRIMARY KEY,
    ORDER_ID VARCHAR,
    MENU_ID INTEGER,
    QUANTITY INTEGER,
    PRICE NUMBER(10, 2),
    SUBTOTAL NUMBER(10, 2),
    ORDER_TIMESTAMP TIMESTAMP_TZ,

    -- AUDIT COLUMNS
    BATCH_ID STRING(36),
    CREATED_AT TIMESTAMP_TZ,
    UPDATED_AT TIMESTAMP_TZ
);
ALTER TABLE SILVER.ORDER_ITEM_SLV CLUSTER BY (DAY(ORDER_TIMESTAMP),MONTH(ORDER_TIMESTAMP));
-- ----------------------------------------------------------------------------------------------------
-- CREATE FACT_ORDER_ITEM
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.FACT_ORDER_ITEM (
    ORDER_ITEM_ID VARCHAR PRIMARY KEY,
    ORDER_ID VARCHAR,
    MENU_ID INTEGER,
    QUANTITY INTEGER,
    PRICE NUMBER(10, 2),
    SUBTOTAL NUMBER(10, 2),
    ORDER_TIMESTAMP TIMESTAMP_TZ,
    BATCH_ID STRING(36),
    CREATED_AT TIMESTAMP_TZ
)
COMMENT = 'FACT TABLE FOR ORDER ITEMS – APPEND ONLY';
ALTER TABLE GOLD.FACT_ORDER_ITEM CLUSTER BY (DAY(ORDER_TIMESTAMP),MONTH(ORDER_TIMESTAMP));

-- =====================================================
-- ORDER_ITEM PROCEDURES (APPEND-ONLY)
-- =====================================================

-- ----------------------------------------------------------------------------------------------------
-- PROCEDURE: ORDER_ITEM STAGE TO BRONZE
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE BRONZE.SP_ORDER_ITEM_STAGE_TO_BRONZE(P_PIPELINE_NAME VARCHAR, P_FILE_NAME VARCHAR)
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
    CREATE OR REPLACE TEMPORARY TABLE TEMP_ORDER_ITEM_LOAD(
        ORDER_ID TEXT,
        MENU_ID TEXT,
        QUANTITY TEXT,
        PRICE TEXT,
        SUBTOTAL TEXT,
        ORDER_TIMESTAMP STRING
    );

    EXECUTE IMMEDIATE
    '
        COPY INTO TEMP_ORDER_ITEM_LOAD (
            ORDER_ID, MENU_ID, QUANTITY, PRICE, SUBTOTAL, ORDER_TIMESTAMP
        )
        FROM (
            SELECT
                $1::STRING AS ORDER_ID,
                $2::STRING AS MENU_ID,
                $3::STRING AS QUANTITY,
                $4::STRING AS PRICE,
                $5::STRING AS SUBTOTAL,
                $6::STRING AS ORDER_TIMESTAMP
        FROM ''' || V_FILE_PATH || '''
    )
    FILE_FORMAT = (FORMAT_NAME = ''' || V_FILE_FORMAT || ''')
    ON_ERROR = ABORT_STATEMENT
    ';

    -- Get row count
    SELECT COUNT(*) INTO :V_ROWS_INSERTED FROM TEMP_ORDER_ITEM_LOAD;

    -- Validate data loaded
    IF (V_ROWS_INSERTED = 0) THEN
        ROLLBACK;
        DROP TABLE IF EXISTS TEMP_ORDER_ITEM_LOAD;
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', 'No records loaded from file',
            'FILE_PATH', P_FILE_NAME,
            'ROWS_INSERTED', 0,
            'INGEST_RUN_ID', 0
        );
    END IF;

    -- Consume sequence
    SELECT SWIGGY.BRONZE.SEQ_ORDER_ITEM_INGEST_RUN_ID.NEXTVAL INTO :V_INGEST_RUN_ID;
    A
    INSERT INTO BRONZE.ORDER_ITEM_BRZ (
        ORDER_ITEM_ID, ORDER_ID, MENU_ID, QUANTITY, PRICE, SUBTOTAL,ORDER_TIMESTAMP,
        ORDER_ID_RAW, MENU_ID_RAW, QUANTITY_RAW, PRICE_RAW, SUBTOTAL_RAW, ORDER_TIMESTAMP_RAW,
        CREATED_AT, UPDATED_AT, INGEST_RUN_ID
    )
    SELECT
        UUID_STRING(),
        TO_VARCHAR(ORDER_ID),
        TRY_TO_NUMBER(MENU_ID),
        TRY_TO_NUMBER(QUANTITY, 10, 2),
        TRY_TO_NUMBER(PRICE, 10, 2),
        TRY_TO_NUMBER(SUBTOTAL, 10, 2),
        TO_TIMESTAMP_TZ(ORDER_TIMESTAMP),

        ORDER_ID_,
        MENU_ID,
        QUANTITY,
        PRICE,
        SUBTOTAL,
        ORDER_TIMESTAMP,

        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        :V_INGEST_RUN_ID
    FROM TEMP_ORDER_ITEM_LOAD;

    -- Commit transaction
    COMMIT;

    -- Cleanup
    DROP TABLE IF EXISTS BRONZE.TEMP_ORDER_ITEM_LOAD;

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
        DROP TABLE IF EXISTS TEMP_ORDER_ITEM_LOAD;

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
-- PROCEDURE: ORDER_ITEM BRONZE TO SILVER
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SILVER.SP_ORDER_BRONZE_TO_SILVER(
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

    -- Simple INSERT for order items (append-only)
v_sql := '
INSERT INTO SILVER.ORDER_ITEM_SLV (
    ORDER_ITEM_ID, ORDER_ID, MENU_ID, QUANTITY, PRICE, SUBTOTAL, ORDER_TIMESTAMP, CREATED_AT, UPDATED_AT, BATCH_ID
)
SELECT
    ORDER_ITEM_ID,
    ORDER_ID,
    MENU_ID,
    QUANTITY,
    PRICE,
    SUBTOTAL,
    ORDER_TIMESTAMP,
    CREATED_AT,
    UPDATED_AT,
    ?
FROM BRONZE.ORDER_ITEM_BRZ
WHERE INGEST_RUN_ID = ?
    AND NOT EXISTS (
        SELECT 1 FROM SILVER.ORDER_ITEM_SLV TGT
        WHERE TGT.ORDER_ITEM_ID = ORDER_ITEM_BRZ.ORDER_ITEM_ID
    )';

EXECUTE IMMEDIATE v_sql USING (P_BATCH_ID, P_INGEST_RUN_ID);

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
-- PROCEDURE: ORDER_ITEM SILVER TO GOLD
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE GOLD.SP_ORDER_ITEM_SILVER_TO_GOLD(P_BATCH_ID STRING)
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

    -- Simple INSERT - order items are immutable
    INSERT INTO GOLD.FACT_ORDER_ITEM (
        ORDER_ITEM_ID, ORDER_ID, MENU_ID, QUANTITY, PRICE, SUBTOTAL, ORDER_TIMESTAMP, BATCH_ID, CREATED_AT)
    SELECT
        SRC.ORDER_ITEM_ID,
        SRC.ORDER_ID,
        SRC.MENU_ID,
        SRC.QUANTITY,
        SRC.PRICE,
        SRC.SUBTOTAL,
        ORDER_TIMESTAMP,
        SRC.BATCH_ID,
        SRC.CREATED_AT
    FROM SILVER.ORDER_ITEM_SLV SRC
    WHERE SRC.BATCH_ID = :P_BATCH_ID
        AND NOT EXISTS (
            SELECT 1 FROM GOLD.FACT_ORDER_ITEM TGT
            WHERE TGT.ORDER_ITEM_ID = SRC.ORDER_ITEM_ID
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

-- SELECT
--     UUID_STRING()                    AS OrderID,
--     UNIFORM(1, 500, RANDOM())        AS MenuID,
--     UNIFORM(1, 5, RANDOM())          AS Quantity,
--     ROUND(UNIFORM(50, 500, RANDOM()), 2) AS Price,
--     Quantity * Price                 AS Subtotal
-- FROM TABLE(GENERATOR(ROWCOUNT => 1000));