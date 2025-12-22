-- ==============================================================================================================================================================
-- ORDER
-- ==============================================================================================================================================================
-- CHANGE CONVARCHAR
USE DATABASE SWIGGY;
USE SCHEMA BRONZE;
USE WAREHOUSE ADHOC_WH;

-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
-- CREATE ORDER_BRZ
-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.ORDER_BRZ (
    ORDER_ID VARCHAR PRIMARY KEY COMMENT 'PRIMARY KEY (SOURCE SYSTEM)',
    CUSTOMER_ID INTEGER COMMENT 'CUSTOMER FK(SOURCE SYSTEM)',
    RESTAURANT_ID INTEGER COMMENT 'RESTAURANT FK(SOURCE SYSTEM)',
    ORDER_DATE DATE,
    TOTAL_AMOUNT NUMBER(10, 2),
    STATUS VARCHAR,
    PAYMENT_METHOD VARCHAR,

    -- RAW_COLUMNS
    ORDER_ID_RAW VARCHAR,
    CUSTOMER_ID_RAW VARCHAR,
    RESTAURANT_ID_RAW VARCHAR,
    ORDER_DATE_RAW VARCHAR,
    TOTAL_AMOUNT_RAW VARCHAR,
    STATUS_RAW VARCHAR,
    PAYMENT_METHOD_RAW VARCHAR,

    -- AUDIT COLUMNS
    INGEST_RUN_ID INTEGER,
    CREATED_AT VARCHAR,
    UPDATED_AT VARCHAR
);
ALTER TABLE BRONZE.ORDER_BRZ CLUSTER BY (INGEST_RUN_ID);

-- CREATING SEQUNCE TO GENERATE INGEST_RUN_ID
CREATE OR REPLACE SEQUENCE SEQ_ORDER_INGEST_RUN_ID START = 1 INCREMENT = 1;

-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
-- CREATE ORDER_SLV
-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.ORDER_SLV (
    ORDER_ID VARCHAR PRIMARY KEY COMMENT 'PRIMARY KEY (SOURCE SYSTEM)',
    CUSTOMER_ID INTEGER COMMENT 'CUSTOMER FK(SOURCE SYSTEM)',
    RESTAURANT_ID INTEGER COMMENT 'RESTAURANT FK(SOURCE SYSTEM)',
    ORDER_DATE TIMESTAMP_TZ,
    TOTAL_AMOUNT NUMBER(10, 2),
    STATUS STRING(50),
    PAYMENT_METHOD STRING(50),

    -- AUDIT COLUMNS
    BATCH_ID STRING(50),
    CREATED_AT TIMESTAMP_TZ,
    UPDATED_AT TIMESTAMP_TZ
);

-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
-- CREATE FACT_ORDER
-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.FACT_ORDER (
    ORDER_ID VARCHAR PRIMARY KEY COMMENT 'BUSINESS KEY (SOURCE SYSTEM)',
    CUSTOMER_ID INTEGER COMMENT 'CUSTOMER FK(SOURCE SYSTEM)',
    RESTAURANT_ID INTEGER COMMENT 'RESTAURANT FK(SOURCE SYSTEM)',
    ORDER_DATE TIMESTAMP_TZ,
    TOTAL_AMOUNT NUMBER(10, 2),
    CURRENT_STATUS STRING(50),
    INITIAL_STATUS STRING(50),
    PAYMENT_METHOD STRING(50),
    STATUS_UPDATED_AT TIMESTAMP_TZ,
    BATCH_ID STRING(50),
    CREATED_AT TIMESTAMP_TZ,
    UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'FACT TABLE FOR ORDER WITH STATUS TRACKING';

-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
-- STATUS HISTORY TABLES (DETAILED AUDIT TRAIL)
-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.FACT_ORDER_STATUS_HISTORY (
    STATUS_HISTORY_KEY INTEGER PRIMARY KEY AUTOINCREMENT,
    ORDER_ID VARCHAR,  -- Changed from INTEGER to VARCHAR to match FACT_ORDER
    OLD_STATUS STRING(50),
    NEW_STATUS STRING(50),
    STATUS_CHANGED_AT TIMESTAMP_TZ,
    BATCH_ID STRING(50)
)
COMMENT = 'AUDIT TRAIL FOR ORDER STATUS CHANGES';

-- ----------------------------------------------------------------------------------------------------
-- PROCEDURE: ORDER STAGE TO BRONZE
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE BRONZE.SP_ORDER_STAGE_TO_BRONZE(P_PIPELINE_NAME VARCHAR, P_FILE_NAME VARCHAR)
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

    CREATE OR REPLACE TEMPORARY TABLE TEMP_ORDER_LOAD(
        ORDER_ID VARCHAR,
        CUSTOMER_ID VARCHAR,
        RESTAURANT_ID VARCHAR,
        ORDER_DATE VARCHAR,
        TOTAL_AMOUNT VARCHAR,
        STATUS VARCHAR,
        PAYMENT_METHOD VARCHAR
    );

    EXECUTE IMMEDIATE
    '
        COPY INTO TEMP_ORDER_LOAD (
            ORDER_ID, CUSTOMER_ID, RESTAURANT_ID, ORDER_DATE,
            TOTAL_AMOUNT, STATUS, PAYMENT_METHOD
        )
        FROM (
            SELECT
                $1::STRING AS ORDER_ID,
                $2::STRING AS CUSTOMER_ID,
                $3::STRING AS RESTAURANT_ID,
                $4::STRING AS ORDER_DATE,
                $5::STRING AS TOTAL_AMOUNT,
                $6::STRING AS STATUS,
                $7::STRING AS PAYMENT_METHOD
        FROM ''' || V_FILE_PATH || '''
    )
    FILE_FORMAT = (FORMAT_NAME = ''' || V_FILE_FORMAT || ''')
    ON_ERROR = ABORT_STATEMENT
    ';

    -- Get row count
    SELECT COUNT(*) INTO :V_ROWS_INSERTED FROM BRONZE.TEMP_ORDER_LOAD;

    -- Validate data loaded
    IF (V_ROWS_INSERTED = 0) THEN
        ROLLBACK;
        DROP TABLE IF EXISTS TEMP_ORDER_LOAD;
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', 'No records loaded from file',
            'FILE_PATH', P_FILE_NAME,
            'ROWS_INSERTED', 0,
            'INGEST_RUN_ID', 0
        );
    END IF;

    -- Consume sequence
    SELECT SWIGGY.BRONZE.SEQ_ORDER_INGEST_RUN_ID.NEXTVAL INTO :V_INGEST_RUN_ID;

    INSERT INTO BRONZE.ORDER_BRZ (
        ORDER_ID,
        CUSTOMER_ID,
        RESTAURANT_ID,
        ORDER_DATE,
        TOTAL_AMOUNT,
        STATUS,
        PAYMENT_METHOD,
        ORDER_ID_RAW,
        CUSTOMER_ID_RAW,
        RESTAURANT_ID_RAW,
        ORDER_DATE_RAW,
        TOTAL_AMOUNT_RAW,
        STATUS_RAW,
        PAYMENT_METHOD_RAW,
        INGEST_RUN_ID,
        CREATED_AT,
        UPDATED_AT
    )
    SELECT
        TO_VARCHAR(ORDER_ID),
        TRY_TO_NUMBER(CUSTOMER_ID),
        TRY_TO_NUMBER(RESTAURANT_ID),
        TRY_TO_DATE(ORDER_DATE),
        TRY_TO_NUMBER(TOTAL_AMOUNT, 10, 2),
        TO_VARCHAR(STATUS),
        TO_VARCHAR(PAYMENT_METHOD),
        ORDER_ID,
        CUSTOMER_ID,
        RESTAURANT_ID,
        ORDER_DATE,
        TOTAL_AMOUNT,
        STATUS,
        PAYMENT_METHOD,
        :V_INGEST_RUN_ID,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    FROM TEMP_ORDER_LOAD;

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

-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
-- PROCEDURE: ORDER BRONZE TO SILVER
-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
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

    -- Merge only VALID records into Silver table using MERGE statement
    v_sql := '
    MERGE INTO SILVER.ORDER_SLV AS TGT
    USING (
        SELECT
            ORDER_ID,
            CUSTOMER_ID,
            RESTAURANT_ID,
            ORDER_DATE,
            TOTAL_AMOUNT,
            STATUS,
            PAYMENT_METHOD,
            CREATED_AT,
            UPDATED_AT,
            INGEST_RUN_ID
        FROM ' || v_bronze_table || '
        WHERE INGEST_RUN_ID = ' || P_INGEST_RUN_ID || '
    ) AS SRC
    ON TGT.ORDER_ID = SRC.ORDER_ID

    WHEN MATCHED THEN
        UPDATE SET
            TGT.STATUS = SRC.STATUS,
            TGT.TOTAL_AMOUNT = SRC.TOTAL_AMOUNT,
            TGT.UPDATED_AT = SRC.UPDATED_AT

    WHEN NOT MATCHED THEN
        INSERT (
            ORDER_ID, CUSTOMER_ID, RESTAURANT_ID, ORDER_DATE,
            TOTAL_AMOUNT, STATUS, PAYMENT_METHOD, CREATED_AT, UPDATED_AT, BATCH_ID
        )
        VALUES (
            SRC.ORDER_ID, SRC.CUSTOMER_ID, SRC.RESTAURANT_ID, SRC.ORDER_DATE,
            SRC.TOTAL_AMOUNT, SRC.STATUS, SRC.PAYMENT_METHOD, SRC.CREATED_AT,
            SRC.UPDATED_AT, ''' || P_BATCH_ID || '''
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

-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
-- PROCEDURE: SP_ORDER_SILVER_TO_GOLD
-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE GOLD.SP_ORDER_SILVER_TO_GOLD(P_BATCH_ID STRING)
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

    -- ========================================
    -- STEP 1: CAPTURE STATUS CHANGES (BEFORE UPDATE)
    -- ========================================
    -- Create temp table to store old status before merge
    CREATE OR REPLACE TEMPORARY TABLE TEMP_STATUS_CHANGES AS
    SELECT
        TGT.ORDER_ID,
        TGT.CURRENT_STATUS AS OLD_STATUS,
        SRC.STATUS AS NEW_STATUS,
        :V_CURRENT_TIMESTAMP AS STATUS_CHANGED_AT,
        :P_BATCH_ID AS BATCH_ID
    FROM GOLD.FACT_ORDER TGT
    INNER JOIN SILVER.ORDER_SLV SRC
        ON TGT.ORDER_ID = SRC.ORDER_ID
    WHERE SRC.BATCH_ID = :P_BATCH_ID
        AND TGT.CURRENT_STATUS != SRC.STATUS;

    SELECT COUNT(*) INTO :V_STATUS_CHANGES FROM TEMP_STATUS_CHANGES;

    -- ========================================
    -- STEP 2: MERGE DATA FROM SILVER TO GOLD
    -- ========================================
    MERGE INTO GOLD.FACT_ORDER AS TGT
    USING (
        SELECT
            ORDER_ID,
            CUSTOMER_ID,
            RESTAURANT_ID,
            ORDER_DATE,
            TOTAL_AMOUNT,
            STATUS,
            PAYMENT_METHOD,
            BATCH_ID,
            CREATED_AT,
            UPDATED_AT
        FROM SILVER.ORDER_SLV
        WHERE BATCH_ID = :P_BATCH_ID
    ) AS SRC
    ON TGT.ORDER_ID = SRC.ORDER_ID

    -- UPDATE existing orders
    WHEN MATCHED THEN
        UPDATE SET
            TGT.CUSTOMER_ID = SRC.CUSTOMER_ID,
            TGT.RESTAURANT_ID = SRC.RESTAURANT_ID,
            TGT.ORDER_DATE = SRC.ORDER_DATE,
            TGT.TOTAL_AMOUNT = SRC.TOTAL_AMOUNT,
            TGT.CURRENT_STATUS = SRC.STATUS,
            TGT.PAYMENT_METHOD = SRC.PAYMENT_METHOD,
            TGT.STATUS_UPDATED_AT = CASE
                WHEN TGT.CURRENT_STATUS != SRC.STATUS
                THEN :V_CURRENT_TIMESTAMP
                ELSE TGT.STATUS_UPDATED_AT
            END,
            TGT.BATCH_ID = SRC.BATCH_ID,
            TGT.UPDATED_AT = :V_CURRENT_TIMESTAMP

    -- INSERT new orders
    WHEN NOT MATCHED THEN
        INSERT (
            ORDER_ID,
            CUSTOMER_ID,
            RESTAURANT_ID,
            ORDER_DATE,
            TOTAL_AMOUNT,
            CURRENT_STATUS,
            INITIAL_STATUS,
            PAYMENT_METHOD,
            STATUS_UPDATED_AT,
            BATCH_ID,
            CREATED_AT,
            UPDATED_AT
        )
        VALUES (
            SRC.ORDER_ID,
            SRC.CUSTOMER_ID,
            SRC.RESTAURANT_ID,
            SRC.ORDER_DATE,
            SRC.TOTAL_AMOUNT,
            SRC.STATUS,
            SRC.STATUS,  -- Initial status = current status for new records
            SRC.PAYMENT_METHOD,
            :V_CURRENT_TIMESTAMP,
            SRC.BATCH_ID,
            SRC.CREATED_AT,
            :V_CURRENT_TIMESTAMP
        );

    -- Capture merge statistics
    V_ROWS_INSERTED := (SELECT COUNT(*) FROM SILVER.ORDER_SLV
                       WHERE BATCH_ID = :P_BATCH_ID
                       AND ORDER_ID NOT IN (SELECT ORDER_ID FROM GOLD.FACT_ORDER));

    V_ROWS_UPDATED := SQLROWCOUNT - V_ROWS_INSERTED;

    -- ========================================
    -- STEP 3: LOG STATUS CHANGES TO HISTORY
    -- ========================================
    IF (V_STATUS_CHANGES > 0) THEN
        INSERT INTO GOLD.FACT_ORDER_STATUS_HISTORY (
            ORDER_ID,
            OLD_STATUS,
            NEW_STATUS,
            STATUS_CHANGED_AT,
            BATCH_ID
        )
        SELECT
            ORDER_ID,
            OLD_STATUS,
            NEW_STATUS,
            STATUS_CHANGED_AT,
            BATCH_ID
        FROM TEMP_STATUS_CHANGES;
    END IF;

    -- Cleanup temp table
    DROP TABLE IF EXISTS TEMP_STATUS_CHANGES;

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
-- ==============================================================================================================================================================
-- KAFKA INTEGRETION
-- ==============================================================================================================================================================
-- SILVER.ORDERS_STREAM_SLV
CREATE OR REPLACE TABLE SILVER.ORDERS_STREAM_SLV (
    KAFKA_OFFSET NUMBER,                      -- For idempotency
    KAFKA_PARTITION NUMBER,
    KAFKA_TIMESTAMP TIMESTAMP_TZ,
    EVENT_TYPE VARCHAR(50),                   -- ORDER_CREATED, ORDER_UPDATED
    EVENT_TIMESTAMP TIMESTAMP_TZ,

    ORDER_ID INTEGER,
    CUSTOMER_ID INTEGER,
    RESTAURANT_ID INTEGER,
    ORDER_DATE TIMESTAMP_TZ,
    TOTAL_AMOUNT NUMBER(10, 2),
    STATUS VARCHAR(50),
    PAYMENT_METHOD VARCHAR(50),

    METADATA VARIANT,                         -- Store entire Kafka message
    INGESTED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    PROCESSED_TO_GOLD BOOLEAN DEFAULT FALSE,  -- Flag for task processing
    BATCH_ID VARCHAR(36)
);

-- Create streams to track changes
CREATE OR REPLACE STREAM STREAM_ORDERS_CHANGES
ON TABLE SILVER.ORDERS_STREAM_SLV
APPEND_ONLY = TRUE;  -- Only capture INSERTs from Kafka

-- Task to process orders every 5 minutes
CREATE OR REPLACE TASK TASK_ORDERS_STREAM_TO_GOLD
    WAREHOUSE = ADHOC_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('STREAM_ORDERS_CHANGES')
AS
CALL GOLD.SP_ORDERS_STREAM_TO_GOLD();

-- Resume tasks
ALTER TASK TASK_ORDERS_STREAM_TO_GOLD RESUME;

CREATE OR REPLACE PROCEDURE GOLD.SP_ORDERS_STREAM_TO_GOLD()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    V_BATCH_ID STRING;
    V_ROWS_PROCESSED INTEGER DEFAULT 0;
BEGIN
    V_BATCH_ID := 'STREAM_' || UUID_STRING();

    -- Process new orders from stream
    MERGE INTO GOLD.FACT_ORDER AS TGT
    USING (
        SELECT
            ORDER_ID,
            CUSTOMER_ID,
            RESTAURANT_ID,
            ORDER_DATE,
            TOTAL_AMOUNT,
            STATUS,
            PAYMENT_METHOD,
            EVENT_TIMESTAMP
        FROM STREAM_ORDERS_CHANGES
        WHERE EVENT_TYPE IN ('ORDER_CREATED', 'ORDER_UPDATED')
    ) AS SRC
    ON TGT.ORDER_ID = SRC.ORDER_ID

    WHEN MATCHED AND TGT.CURRENT_STATUS != SRC.STATUS THEN
        UPDATE SET
            CURRENT_STATUS = SRC.STATUS,
            TOTAL_AMOUNT = SRC.TOTAL_AMOUNT,
            STATUS_UPDATED_AT = SRC.EVENT_TIMESTAMP

    WHEN NOT MATCHED THEN
        INSERT (
            ORDER_ID, CUSTOMER_ID, RESTAURANT_ID, ORDER_DATE,
            TOTAL_AMOUNT, CURRENT_STATUS, INITIAL_STATUS,
            PAYMENT_METHOD, STATUS_UPDATED_AT, BATCH_ID, CREATED_AT
        )
        VALUES (
            SRC.ORDER_ID, SRC.CUSTOMER_ID, SRC.RESTAURANT_ID,
            SRC.ORDER_DATE, SRC.TOTAL_AMOUNT, SRC.STATUS, SRC.STATUS,
            SRC.PAYMENT_METHOD, SRC.EVENT_TIMESTAMP, V_BATCH_ID,
            CURRENT_TIMESTAMP()
        );

    V_ROWS_PROCESSED := SQLROWCOUNT;

    -- Mark stream records as processed
    UPDATE SILVER.ORDERS_STREAM_SLV
    SET PROCESSED_TO_GOLD = TRUE,
        BATCH_ID = V_BATCH_ID
    WHERE ORDER_ID IN (
        SELECT ORDER_ID FROM STREAM_ORDERS_CHANGES
    );

    RETURN OBJECT_CONSTRUCT(
        'STATUS', 'SUCCESS',
        'BATCH_ID', V_BATCH_ID,
        'ROWS_PROCESSED', V_ROWS_PROCESSED
    );
END;
$$;