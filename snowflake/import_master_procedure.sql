-- =====================================================
-- CONFIGURATION & METADATA TABLES
-- =====================================================
-- Configuration table for pipeline metadata
CREATE OR REPLACE TABLE COMMON.IMPORT_CONFIGURATION (
    CONFIGURATION_IMPORT_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    PIPELINE_NAME VARCHAR(100) NOT NULL UNIQUE,
    SOURCE_TYPE VARCHAR(50) NOT NULL,  -- FILE, DATABASE, API, STREAM
    FILE_FORMAT VARCHAR(50),           -- CSV, JSON, PARQUET, etc.
    SOURCE_LOCATION VARCHAR(500),      -- S3 path, table name, API endpoint
    BRONZE_TABLE VARCHAR(100),
    LOAD_ERROR_TABLE VARCHAR(100),
    STAGE_TABLE VARCHAR(100), -- TEMP TABLE TO PERFORM DQ VALIDATION CHECKS
    SILVER_TABLE VARCHAR(100),
    GOLD_TABLE VARCHAR(100),
    ENABLED BOOLEAN DEFAULT TRUE,
    LOAD_TYPE VARCHAR(20) DEFAULT 'FULL',  -- FULL, INCREMENTAL
    SCHEDULE_CRON VARCHAR(100),        -- Cron expression for scheduling
    STAGE_TO_BRONZE_PROC VARCHAR(200),
    BRONZE_TO_SILVER_PROC VARCHAR(200),
    SILVER_TO_GOLD_PROC VARCHAR(200),
    DEPENDENCY_PIPELINE VARCHAR(100),  -- Pipeline that must complete first
    RETRY_COUNT INTEGER DEFAULT 3,
    TIMEOUT_MINUTES INTEGER DEFAULT 60,
    DESCRIPTION VARCHAR(1000),
    CREATED_BY VARCHAR(100) DEFAULT CURRENT_USER(),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_BY VARCHAR(100),
    UPDATED_AT TIMESTAMP_NTZ
)
COMMENT = 'Metadata configuration for all ETL pipelines';

-- Insert configuration for Location pipeline
INSERT INTO COMMON.IMPORT_CONFIGURATION ( PIPELINE_NAME, SOURCE_TYPE, SOURCE_LOCATION, FILE_FORMAT, BRONZE_TABLE, LOAD_ERROR_TABLE, STAGE_TABLE, SILVER_TABLE, GOLD_TABLE, STAGE_TO_BRONZE_PROC, BRONZE_TO_SILVER_PROC, SILVER_TO_GOLD_PROC, DESCRIPTION)
VALUES
('CUSTOMER_PIPELINE',
	'FILE',
    '@BRONZE.CSV_STG/customer/',
    'BRONZE.FF_CSV_PIPE',
    'BRONZE.CUSTOMER_BRZ',
    'BRONZE.CUSTOMER_LOAD_ERROR',
    'BRONZE.STG_CUSTOMER_DQ',
    'SILVER.CUSTOMER_SLV',
    'GOLD.DIM_CUSTOMER',
    'BRONZE.SP_CUSTOMER_STAGE_TO_BRONZE',
    'SILVER.SP_CUSTOMER_BRONZE_TO_SILVER',
    'GOLD.SP_CUSTOMER_SILVER_TO_GOLD',
    'Customer master data pipeline with SCD Type 2'),

('LOCATION_PIPELINE',
    'FILE',
    '@BRONZE.CSV_STG/location/',
    'BRONZE.FF_CSV_COMMA',
    'BRONZE.LOCATION_BRZ',
    'BRONZE.LOCATION_LOAD_ERROR',
    'BRONZE.STG_LOCATION_DQ',
    'SILVER.LOCATION_SLV',
    'GOLD.DIM_LOCATION',
    'BRONZE.SP_LOCATION_STAGE_TO_BRONZE',
    'SILVER.SP_LOCATION_BRONZE_TO_SILVER',
    'GOLD.SP_LOCATION_SILVER_TO_GOLD',
    'Location master data pipeline with SCD Type 2'),

('CUSTOMER_ADDRESS_PIPELINE',
    'FILE',
    '@BRONZE.CSV_STAGE/customer_address/',
    'BRONZE.FF_CSV_COMMA',
    'BRONZE.CUSTOMER_ADDRESS_BRZ',
    'BRONZE.CUSTOMER_ADDRESS_LOAD_ERROR',
    'BRONZE.STG_CUSTOMER_ADDRESS_DQ',
    'SILVER.LOCATION_SLV',
    'GOLD.DIM_LOCATION',
    'BRONZE.SP_CUSTOMER_ADDRESS_STAGE_TO_BRONZE',
    'SILVER.SP_CUSTOMER_ADDRESS_BRONZE_TO_SILVER',
    'GOLD.SP_CUSTOMER_ADDRESS_SILVER_TO_GOLD',
    'Customer Address master data pipeline with SCD Type 2'),

('DELIVERY_AGENT_PIPELINE',
    'FILE',
    '@BRONZE.CSV_STAGE/delivery_agent/',
    'BRONZE.FF_CSV_COMMA',
    'BRONZE.DELIVERY_AGENT_BRZ',
    'BRONZE.DELIVERY_AGENT_LOAD_ERROR',
    'BRONZE.STG_DELIVERY_AGENT_DQ',
    'SILVER.DELIVERY_AGENT_SLV',
    'GOLD.DIM_DELIVERY_AGENT',
    'BRONZE.SP_DELIVERY_AGENT_STAGE_TO_BRONZE',
    'SILVER.SP_DELIVERY_AGENT_BRONZE_TO_SILVER',
    'GOLD.SP_DELIVERY_AGENT_SILVER_TO_GOLD',
    'Delivery Agent master data pipeline with SCD Type 2'),

('MENU_PIPELINE',
    'FILE',
    '@BRONZE.CSV_STAGE/menu/',
    'BRONZE.FF_CSV_COMMA',
    'BRONZE.MENU_BRZ',
    'BRONZE.MENU_LOAD_ERROR',
    'BRONZE.STG_MENU_DQ',
    'SILVER.MENU_SLV',
    'GOLD.DIM_MENU',
    'BRONZE.SP_MENU_STAGE_TO_BRONZE',
    'SILVER.SP_MENU_BRONZE_TO_SILVER',
    'GOLD.SP_MENU_SILVER_TO_GOLD',
    'Menu master data pipeline with SCD Type 2'),

('RESTAURANT_PIPELINE',
    'FILE',
    '@BRONZE.CSV_STG/restaurant/',
    'BRONZE.FF_CSV_COMMA',
    'BRONZE.RESTAURANT_BRZ',
    'BRONZE.RESTAURANT_LOAD_ERROR',
    'BRONZE.STG_RESTAURANT_DQ',
    'SILVER.RESTAURANT_SLV',
    'GOLD.DIM_RESTAURANT',
    'BRONZE.SP_RESTAURANT_STAGE_TO_BRONZE',
    'SILVER.SP_RESTAURANT_BRONZE_TO_SILVER',
    'GOLD.SP_RESTAURANT_SILVER_TO_GOLD',
    'Menu master data pipeline with SCD Type 2'),

('DELIVERY_PIPELINE',
    'FILE',
    '@CSV_STAGE/delivery/',
    'BRONZE.FF_CSV_COMMA',
    'BRONZE.DELIVERY_BRZ',
    'BRONZE.DELIVERY_LOAD_ERROR',
    'BRONZE.STG_DELIVERY_DQ',
    'SILVER.DELIVERY_SLV',
    'GOLD.DIM_DELIVERY',
    'BRONZE.SP_DELIVERY_STAGE_TO_BRONZE',
    'SILVER.SP_DELIVERY_BRONZE_TO_SILVER',
    'GOLD.SP_DELIVERY_SILVER_TO_GOLD',
    'Delivery master data pipeline with SCD Type 2');

-- ('ORDER_PIPELINE',
--     'FILE',
--     '@CSV_STAGE/order/',
--     'BRONZE.FF_CSV_COMMA',
--     'BRONZE.ORDER_BRZ',
--     'BRONZE.ORDER_LOAD_ERROR',
--     'BRONZE.STG_ORDER_DQ',
--     'GOLD.DIM_ORDER',
--     'BRONZE.ORDER_BRZ',

--     'BRONZE.SP_ORDER_STAGE_TO_BRONZE',
--     'SILVER.SP_ORDER_BRONZE_TO_SILVER',
--     'GOLD.SP_ORDER_SILVER_TO_GOLD',
--     'Order master data pipeline with SCD Type 2'),

-- ('ORDER_ITEM_PIPELINE',
--     'FILE',
--     '@CSV_STAGE/order_item/',
--     'BRONZE.FF_CSV_COMMA',
--     'BRONZE', 'ORDER_ITEM_BRZ', 'CSV',
--     'BRONZE.SP_ORDER_ITEM_STAGE_TO_BRONZE',
--     'SILVER.SP_ORDER_ITEM_BRONZE_TO_SILVER',
--     'GOLD.SP_ORDER_ITEM_SILVER_TO_GOLD',
--     'Order Item master data pipeline with SCD Type 2');

-- =====================================================
-- BATCH TABLE - STORES BATCH EXECUTION DATA
-- =====================================================

CREATE OR REPLACE TABLE COMMON.BATCH (
    BATCH_ID VARCHAR(36) PRIMARY KEY,              -- Generated UUID for each run
    PIPELINE_NAME VARCHAR(100) NOT NULL,
    LAST_BATCH_EXECUTED_AT TIMESTAMP_NTZ,

    -- Stage Execution Tracking
    STAGE_TO_BRONZE_STATUS VARCHAR(20),            -- SUCCESS, FAILED, SKIPPED
    BRONZE_INSERT_RECORDS INTEGER DEFAULT 0,

    -- Bronze to Silver Tracking
    BRONZE_TO_SILVER_STATUS VARCHAR(20),
    SILVER_INSERTED INTEGER DEFAULT 0,
    SILVER_UPDATED INTEGER DEFAULT 0,

    -- Silver to Gold Tracking
    SILVER_TO_GOLD_STATUS VARCHAR(20),
    GOLD_INSERTED INTEGER DEFAULT 0,
    GOLD_UPDATED INTEGER DEFAULT 0,
    GOLD_DELETED INTEGER DEFAULT 0,

    BATCH_LOG VARIANT
)
COMMENT = 'Master tracking table storing all execution details for each batch run';

SELECT * FROM COMMON.IMPORT_CONFIGURATION;

-- =====================================================
-- INGEST RUN TABLE
-- =====================================================
CREATE OR REPLACE TABLE COMMON.INGEST_RUN (
    INGEST_RUN_ID INTEGER PRIMARY KEY,
    PIPELINE_NAME VARCHAR(50),
    SOURCE_TABLE VARCHAR(50),
    LOAD_ERROR_TABLE VARCHAR(50),
    RUN_STATUS VARCHAR(50),
    SOURCE_ROW_COUNT INTEGER,
    VALID_ROW_COUNT INTEGER,
    INVALID_ROW_COUNT INTEGER,
    EXECUTION_DURATION_SEC INTEGER,
    ERROR_MESSAGE VARCHAR(5000),
    EXECUTED_AT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP(),
    EXECUTED_BY VARCHAR(100) DEFAULT CURRENT_USER()
);
-- =====================================================
-- IMPORT MASTER PROCEDURE
-- =====================================================

CREATE OR REPLACE PROCEDURE COMMON.SP_IMPORT_MASTER(
    PIPELINE_NAME_PARAM VARCHAR,
    STAGE_PATH_PARAM VARCHAR
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_batch_id STRING;
    v_stage_to_bronze_proc STRING;
    v_bronze_to_silver_proc STRING;
    v_silver_to_gold_proc STRING;
    v_pipeline_enabled BOOLEAN;
    v_pipeline_file_format STRING;

    -- Stage Execution Tracking
    v_stb_start_time TIMESTAMP_NTZ;
    v_stb_end_time TIMESTAMP_NTZ;
    v_stb_status VARCHAR(20);
    v_b_inserted_records INTEGER DEFAULT 0;
    v_ingest_run_id INTEGER;
    v_stb_duration_sec NUMBER(10,2);
    v_stb_error VARCHAR(1000);
    v_stb_result VARIANT;

    -- Bronze to Silver Tracking
    v_bts_start_time TIMESTAMP_NTZ;
    v_bts_end_time TIMESTAMP_NTZ;
    v_bts_status VARCHAR(20);
    v_s_inserted_records INTEGER DEFAULT 0;
    v_s_updated_records INTEGER DEFAULT 0;
    v_bts_duration_sec NUMBER(10,2);
    v_bts_error VARCHAR(1000);
    v_bts_result VARIANT;

    -- Silver to Gold Tracking
    v_stg_start_time TIMESTAMP_NTZ;
    v_stg_end_time TIMESTAMP_NTZ;
    v_stg_status VARCHAR(20);
    v_g_inserted_records INTEGER DEFAULT 0;
    v_g_updated_records INTEGER DEFAULT 0;
    v_g_deleted_records INTEGER DEFAULT 0;
    v_stg_duration_sec NUMBER(10,2);
    v_stg_error VARCHAR(1000);
    v_stg_result VARIANT;

    v_batch_start_time TIMESTAMP_NTZ;
    v_batch_end_time TIMESTAMP_NTZ;
    v_last_batch_executed_at TIMESTAMP_NTZ;

BEGIN
    ------------------------------------------------------------------
    -- STEP 1: INITIALIZE
    ------------------------------------------------------------------
    v_batch_id := UUID_STRING();
    v_batch_start_time := CURRENT_TIMESTAMP();
    v_last_batch_executed_at := CURRENT_TIMESTAMP();

    SELECT
        ENABLED,
        STAGE_TO_BRONZE_PROC,
        BRONZE_TO_SILVER_PROC,
        SILVER_TO_GOLD_PROC,
        FILE_FORMAT
    INTO
        v_pipeline_enabled,
        v_stage_to_bronze_proc,
        v_bronze_to_silver_proc,
        v_silver_to_gold_proc,
        v_pipeline_file_format
    FROM COMMON.IMPORT_CONFIGURATION
    WHERE PIPELINE_NAME = :PIPELINE_NAME_PARAM;

    -- INITIALIZE BATCH TABLE
    INSERT INTO COMMON.BATCH(BATCH_ID, PIPELINE_NAME)
    VALUES (:v_batch_id, :PIPELINE_NAME_PARAM);

    ------------------------------------------------------------------
    -- STEP 2: STAGE → BRONZE
    ------------------------------------------------------------------
    BEGIN
        v_stb_start_time := CURRENT_TIMESTAMP();

        -- Call stored procedure dynamically
        LET res RESULTSET := (EXECUTE IMMEDIATE 'CALL ' || :v_stage_to_bronze_proc || '(?, ?)' USING (PIPELINE_NAME_PARAM, STAGE_PATH_PARAM));
        LET cur CURSOR FOR res;
        OPEN cur;
        FETCH cur INTO v_stb_result;
        CLOSE cur;

        -- Extract values from the result array
        v_stb_status := v_stb_result:STATUS::VARCHAR;
        v_stb_error := v_stb_result:ERROR::VARCHAR;
        v_ingest_run_id := v_stb_result:INGEST_RUN_ID::INTEGER;
        v_s_inserted_records := v_stb_result:ROWS_INSERTED::INTEGER;
        v_stb_end_time := CURRENT_TIMESTAMP();
        v_stb_duration_sec := DATEDIFF(SECOND, v_stb_start_time, v_stb_end_time);

    EXCEPTION
        WHEN OTHER THEN
            v_stb_status := 'FAILED';
            v_stb_end_time := CURRENT_TIMESTAMP();
            v_stb_duration_sec := DATEDIFF(SECOND, v_stb_start_time, v_stb_end_time);
    END;

    ------------------------------------------------------------------
    -- STEP 3: BRONZE → SILVER
    ------------------------------------------------------------------
    BEGIN
        v_bts_start_time := CURRENT_TIMESTAMP();

        -- Call stored procedure dynamically
        LET res RESULTSET := (EXECUTE IMMEDIATE 'CALL ' || :v_bronze_to_silver_proc || '(?, ?, ?)' USING (PIPELINE_NAME_PARAM, v_ingest_run_id, v_batch_id));
        LET cur CURSOR FOR res;
        OPEN cur;
        FETCH cur INTO v_bts_result;
        CLOSE cur;

        v_bts_status := v_bts_result:STATUS::VARCHAR;
        v_bts_error := v_bts_result:ERROR::VARCHAR;
        v_s_inserted_records := v_bts_result:ROWS_INSERTED::INTEGER;
        v_s_updated_records := v_bts_result:ROWS_UPDATED::INTEGER;
        v_bts_end_time := CURRENT_TIMESTAMP();
        v_bts_duration_sec := DATEDIFF(SECOND, v_bts_start_time, v_bts_end_time);

    EXCEPTION
        WHEN OTHER THEN
            v_bts_status := 'FAILED';
            v_bts_end_time := CURRENT_TIMESTAMP();
            v_bts_duration_sec := DATEDIFF(SECOND, v_bts_start_time, v_bts_end_time);
    END;

    ------------------------------------------------------------------
    -- STEP 4: SILVER → GOLD
    ------------------------------------------------------------------
    BEGIN
        v_stg_start_time := CURRENT_TIMESTAMP();

        -- Call stored procedure dynamically
        LET res RESULTSET := (EXECUTE IMMEDIATE 'CALL ' || :v_silver_to_gold_proc || '(?)' USING (v_batch_id));
        LET cur CURSOR FOR res;
        OPEN cur;
        FETCH cur INTO v_stg_result;
        CLOSE cur;

        v_stg_status := v_stg_result:STATUS::VARCHAR;
        v_stg_error := v_stg_result:ERROR::VARCHAR;
        v_g_inserted_records := v_stg_result:ROWS_INSERTED:INTEGER;
        v_g_updated_records := v_stg_result:ROWS_UPDATED::INTEGER;
        v_g_deleted_records := v_stg_result:ROWS_DELETED::INTEGER;

        v_stg_end_time := CURRENT_TIMESTAMP();
        v_stg_duration_sec := DATEDIFF(SECOND, v_stg_start_time, v_stg_end_time);

    EXCEPTION
        WHEN OTHER THEN
            v_stg_status := 'FAILED';
            v_stg_end_time := CURRENT_TIMESTAMP();
            v_stg_duration_sec := DATEDIFF(SECOND, v_stg_start_time, v_stg_end_time);
    END;

    ------------------------------------------------------------------
    -- STEP 5: FINALIZE
    ------------------------------------------------------------------
    v_batch_end_time := CURRENT_TIMESTAMP();

    -- UPDATE BATCH TABLE with all execution details
    UPDATE COMMON.BATCH
    SET
        PIPELINE_NAME = :PIPELINE_NAME_PARAM,
        LAST_BATCH_EXECUTED_AT = :v_last_batch_executed_at,

        -- Stage to Bronze
        STAGE_TO_BRONZE_STATUS = :v_stb_status,
        BRONZE_INSERT_RECORDS = :v_b_inserted_records,

        -- Bronze to Silver
        BRONZE_TO_SILVER_STATUS = :v_bts_status,
        SILVER_INSERTED = :v_s_inserted_records,
        SILVER_UPDATED = :v_s_updated_records,

        -- Silver to Gold
        SILVER_TO_GOLD_STATUS = :v_stg_status,
        GOLD_INSERTED = :v_g_inserted_records,
        GOLD_UPDATED = :v_g_updated_records,
        GOLD_DELETED = :v_g_deleted_records,

        -- Batch Log with all timing and error details
        BATCH_LOG = OBJECT_CONSTRUCT(
            'batch_start_time', :v_batch_start_time,
            'batch_end_time', :v_batch_end_time,
            'total_duration_sec', DATEDIFF(SECOND, :v_batch_start_time, :v_batch_end_time),
            'stage_path', :STAGE_PATH_PARAM,

            'stage_to_bronze', OBJECT_CONSTRUCT(
                'ingest_run_id', :v_ingest_run_id,
                'status', :v_stb_status,
                'start_time', :v_stb_start_time,
                'end_time', :v_stb_end_time,
                'duration_sec', :v_stb_duration_sec,
                'records_inserted', :v_b_inserted_records,
                'error', :v_stb_error,
                'procedure_name', :v_stage_to_bronze_proc,
                'result', :v_stb_result
            ),

            'bronze_to_silver', OBJECT_CONSTRUCT(
                'status', :v_bts_status,
                'start_time', :v_bts_start_time,
                'end_time', :v_bts_end_time,
                'duration_sec', :v_bts_duration_sec,
                'records_inserted', :v_s_inserted_records,
                'records_updated', :v_s_updated_records,
                'error', :v_bts_error,
                'procedure_name', :v_bronze_to_silver_proc,
                'result', :v_bts_result
            ),

            'silver_to_gold', OBJECT_CONSTRUCT(
                'status', :v_stg_status,
                'start_time', :v_stg_start_time,
                'end_time', :v_stg_end_time,
                'duration_sec', :v_stg_duration_sec,
                'records_inserted', :v_g_inserted_records,
                'records_updated', :v_g_updated_records,
                'records_deleted', :v_g_deleted_records,
                'error', :v_stg_error,
                'procedure_name', :v_silver_to_gold_proc,
                'result', :v_stg_result
            )
        )
    WHERE BATCH_ID = :v_batch_id;

    RETURN  OBJECT_CONSTRUCT(
            'batch_start_time', :v_batch_start_time,
            'batch_end_time', :v_batch_end_time,
            'total_duration_sec', DATEDIFF(SECOND, :v_batch_start_time, :v_batch_end_time),
            'stage_path', :STAGE_PATH_PARAM,

            'stage_to_bronze', OBJECT_CONSTRUCT(
                'ingest_run_id', :v_ingest_run_id,
                'status', :v_stb_status,
                'start_time', :v_stb_start_time,
                'end_time', :v_stb_end_time,
                'duration_sec', :v_stb_duration_sec,
                'records_inserted', :v_b_inserted_records,
                'error', :v_stb_error,
                'procedure_name', :v_stage_to_bronze_proc,
                'result', :v_stb_result
            ),

            'bronze_to_silver', OBJECT_CONSTRUCT(
                'status', :v_bts_status,
                'start_time', :v_bts_start_time,
                'end_time', :v_bts_end_time,
                'duration_sec', :v_bts_duration_sec,
                'records_inserted', :v_s_inserted_records,
                'records_updated', :v_s_updated_records,
                'error', :v_bts_error,
                'procedure_name', :v_bronze_to_silver_proc,
                'result', :v_bts_result
            ),

            'silver_to_gold', OBJECT_CONSTRUCT(
                'status', :v_stg_status,
                'start_time', :v_stg_start_time,
                'end_time', :v_stg_end_time,
                'duration_sec', :v_stg_duration_sec,
                'records_inserted', :v_g_inserted_records,
                'records_updated', :v_g_updated_records,
                'records_deleted', :v_g_deleted_records,
                'error', :v_stg_error,
                'procedure_name', :v_silver_to_gold_proc,
                'result', :v_stg_result
            )
        );

 EXCEPTION
     WHEN OTHER THEN
         -- Log critical failure
         BEGIN
             UPDATE COMMON.BATCH
             SET
                 BATCH_LOG = OBJECT_CONSTRUCT(
                     'critical_error', SQLERRM,
                     'error_timestamp', CURRENT_TIMESTAMP(),
                     'batch_start_time', :v_batch_start_time,
                     'stage_to_bronze_status', :v_stb_status,
                     'bronze_to_silver_status', :v_bts_status,
                     'silver_to_gold_status', :v_stg_status
                 )
             WHERE BATCH_ID = :v_batch_id;
         EXCEPTION
             WHEN OTHER THEN
                 NULL;
         END;

         RETURN 'CRITICAL FAILURE | BATCH_ID=' || :v_batch_id || ' | ' || SQLERRM;
END;
$$;

-- Execute the pipeline
--CALL COMMON.SP_IMPORT_MASTER('CUSTOMER_ADDRESS_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/customer_address/customer_address_01-01-2025.csv');
--CALL COMMON.SP_IMPORT_MASTER('CUSTOMER_ADDRESS_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/customer_address/customer_address_02-01-2025.csv');
--CALL COMMON.SP_IMPORT_MASTER('CUSTOMER_ADDRESS_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/customer_address/customer_address_03-01-2025.csv');

CALL COMMON.SP_IMPORT_MASTER('CUSTOMER_PIPELINE', 'customer_02-01-2025.csv');
CALL COMMON.SP_IMPORT_MASTER('CUSTOMER_PIPELINE', 'customer_02-01-2025.csv');
CALL COMMON.SP_IMPORT_MASTER('CUSTOMER_PIPELINE', 'customer_03-01-2025.csv');
CALL COMMON.SP_IMPORT_MASTER('CUSTOMER_PIPELINE', 'customer_04-01-2025_invalid.csv');

--CALL COMMON.SP_IMPORT_MASTER('DELIVERY_AGENT_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/delivery_agent/delivery_agent_01-01-2025.csv');
--CALL COMMON.SP_IMPORT_MASTER('DELIVERY_AGENT_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/delivery_agent/delivery_agent_02-01-2025.csv');
--CALL COMMON.SP_IMPORT_MASTER('DELIVERY_AGENT_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/delivery_agent/delivery_agent_03-01-2025.csv');

CALL COMMON.SP_IMPORT_MASTER('LOCATION_PIPELINE', 'location_01-01-2025.csv');
CALL COMMON.SP_IMPORT_MASTER('LOCATION_PIPELINE', 'location_02-01-2025.csv');
CALL COMMON.SP_IMPORT_MASTER('LOCATION_PIPELINE', 'location_03-01-2025csv');
CALL COMMON.SP_IMPORT_MASTER('LOCATION_PIPELINE', 'location_04-01-2025.csv');
CALL COMMON.SP_IMPORT_MASTER('LOCATION_PIPELINE', 'location_05-01-2025.csv');
CALL COMMON.SP_IMPORT_MASTER('LOCATION_PIPELINE', 'location_06-01-2025.csv');

--CALL COMMON.SP_IMPORT_MASTER('MENU_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/menu/menu_01-01-2025.csv');
--CALL COMMON.SP_IMPORT_MASTER('MENU_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/menu/menu_02-01-2025.csv.csv');
--CALL COMMON.SP_IMPORT_MASTER('MENU_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/menu/menu_03-01-2025.csv');

CALL COMMON.SP_IMPORT_MASTER('RESTAURANT_PIPELINE', 'restaurant_01-01-2025.csv');
CALL COMMON.SP_IMPORT_MASTER('RESTAURANT_PIPELINE', 'restaurant_02-01-2025.csv');
CALL COMMON.SP_IMPORT_MASTER('RESTAURANT_PIPELINE', 'restaurant_03-01-2025.csv');
--
--CALL COMMON.SP_IMPORT_MASTER('DELIVERY_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/delivery/delivery-initial-load.csv');
--CALL COMMON.SP_IMPORT_MASTER('DELIVERY_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/delivery/day-01-delivery.csv');
--CALL COMMON.SP_IMPORT_MASTER('DELIVERY_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/delivery/day-02-delivery.csv');

--CALL COMMON.SP_IMPORT_MASTER('ORDER_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/order/orders-initial.csv');
--CALL COMMON.SP_IMPORT_MASTER('ORDER_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/order/day-01-orders.csv');
--CALL COMMON.SP_IMPORT_MASTER('ORDER_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/order/day-02-orders.csv');

--CALL COMMON.SP_IMPORT_MASTER('ORDER_ITEM_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/order_item/order-item-initial.csv');
--CALL COMMON.SP_IMPORT_MASTER('ORDER_ITEM_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/order_item/day-01-order-item.csv');
--CALL COMMON.SP_IMPORT_MASTER('ORDER_ITEM_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/order_item/day-02-order-item.csv');
--CALL COMMON.SP_IMPORT_MASTER('ORDER_ITEM_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/order_item/order-item-initial-v2.csv');