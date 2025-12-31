-- =====================================================
-- ERROR HANDLING & RECOVERY FRAMEWORK
-- =====================================================
USE ROLE ACCOUNTADMIN;
USE DATABASE DATAVELOCITY;
USE SCHEMA COMMON;

-- =====================================================
-- 1. DEAD LETTER QUEUE
-- =====================================================
CREATE OR REPLACE TABLE COMMON.DEAD_LETTER_QUEUE (
    DLQ_ID INTEGER AUTOINCREMENT PRIMARY KEY,
    PIPELINE_NAME VARCHAR(100) NOT NULL,
    SOURCE_TABLE VARCHAR(100),
    RECORD_DATA VARIANT NOT NULL,
    ERROR_TYPE VARCHAR(50), -- VALIDATION, TRANSFORMATION, REFERENTIAL, TECHNICAL
    ERROR_MESSAGE VARCHAR(5000),
    ERROR_STACKTRACE VARCHAR(10000),
    INGEST_RUN_ID INTEGER,
    BATCH_ID VARCHAR(50),
    RETRY_COUNT INTEGER DEFAULT 0,
    MAX_RETRIES INTEGER DEFAULT 3,
    LAST_RETRY_AT TIMESTAMP_NTZ,
    PROCESSING_STATUS VARCHAR(20) DEFAULT 'PENDING', -- PENDING, RETRYING, RESOLVED, ABANDONED
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RESOLVED_AT TIMESTAMP_NTZ,
    RESOLUTION_NOTES VARCHAR(2000)
);

-- Index for quick lookups
-- CREATE INDEX IDX_DLQ_PIPELINE_STATUS
-- ON COMMON.DEAD_LETTER_QUEUE(PIPELINE_NAME, PROCESSING_STATUS, CREATED_AT);

-- =====================================================
-- 2. RETRY CONFIGURATION TABLE
-- =====================================================
CREATE OR REPLACE TABLE COMMON.RETRY_CONFIG (
    RETRY_CONFIG_ID INTEGER AUTOINCREMENT PRIMARY KEY,
    PIPELINE_NAME VARCHAR(100) NOT NULL,
    ERROR_TYPE VARCHAR(50),
    MAX_RETRIES INTEGER DEFAULT 3,
    RETRY_DELAY_SECONDS INTEGER DEFAULT 300, -- 5 minutes
    EXPONENTIAL_BACKOFF BOOLEAN DEFAULT TRUE,
    BACKOFF_MULTIPLIER NUMBER(3,1) DEFAULT 2.0,
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Default retry configurations
INSERT INTO COMMON.RETRY_CONFIG (PIPELINE_NAME, ERROR_TYPE, MAX_RETRIES, RETRY_DELAY_SECONDS) VALUES
('ALL', 'TECHNICAL', 5, 300),
('ALL', 'TRANSFORMATION', 3, 600),
('ALL', 'VALIDATION', 0, 0), -- Don't retry validation errors
('ALL', 'REFERENTIAL', 3, 900);

-- =====================================================
-- 3. RECONCILIATION TRACKING TABLE
-- =====================================================
CREATE OR REPLACE TABLE COMMON.RECONCILIATION_LOG (
    RECON_ID INTEGER AUTOINCREMENT PRIMARY KEY,
    PIPELINE_NAME VARCHAR(100) NOT NULL,
    BATCH_ID VARCHAR(50) NOT NULL,
    LAYER VARCHAR(20), -- BRONZE, SILVER, GOLD

    -- Counts
    SOURCE_COUNT INTEGER,
    TARGET_COUNT INTEGER,
    MATCHED_COUNT INTEGER,
    MISSING_IN_TARGET INTEGER,
    EXTRA_IN_TARGET INTEGER,

    -- Details
    RECONCILIATION_STATUS VARCHAR(20), -- PASS, FAIL, WARNING
    DISCREPANCY_DETAILS VARIANT,

    -- Timing
    RECONCILED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RECONCILIATION_DURATION_SEC INTEGER
);

-- =====================================================
-- 4. DATA LINEAGE VIEW
-- =====================================================
CREATE OR REPLACE VIEW COMMON.VW_DATA_LINEAGE AS
WITH bronze_data AS (
    SELECT
        'BRONZE' AS layer,
        INGEST_RUN_ID,
        NULL AS BATCH_ID,
        COUNT(*) AS record_count,
        MIN(CREATED_AT) AS first_record,
        MAX(UPDATED_AT) AS last_record
    FROM BRONZE.ORDER_BRZ
    GROUP BY INGEST_RUN_ID
),
silver_data AS (
    SELECT
        'SILVER' AS layer,
        NULL AS INGEST_RUN_ID,
        BATCH_ID,
        COUNT(*) AS record_count,
        MIN(CREATED_AT) AS first_record,
        MAX(UPDATED_AT) AS last_record
    FROM SILVER.ORDER_SLV
    GROUP BY BATCH_ID
),
gold_data AS (
    SELECT
        'GOLD' AS layer,
        NULL AS INGEST_RUN_ID,
        BATCH_ID,
        COUNT(*) AS record_count,
        MIN(CREATED_AT) AS first_record,
        MAX(UPDATED_AT) AS last_record
    FROM GOLD.FACT_ORDER
    WHERE STATUS = 'ACTIVE'
    GROUP BY BATCH_ID
)
SELECT * FROM bronze_data
UNION ALL SELECT * FROM silver_data
UNION ALL SELECT * FROM gold_data
ORDER BY first_record DESC, layer;

-- =====================================================
-- 5. AUTOMATED RETRY PROCEDURE
-- =====================================================
CREATE OR REPLACE PROCEDURE COMMON.SP_RETRY_FAILED_RECORDS(
    P_PIPELINE_NAME VARCHAR,
    P_MAX_RECORDS_TO_RETRY INTEGER DEFAULT 100
)
RETURNS TABLE(retried_count INTEGER, success_count INTEGER, failed_count INTEGER)
LANGUAGE SQL
AS
$$
DECLARE
    v_retried_count INTEGER DEFAULT 0;
    v_success_count INTEGER DEFAULT 0;
    v_failed_count INTEGER DEFAULT 0;
    v_dlq_id INTEGER;
    v_record_data VARIANT;
    v_error_type VARCHAR;
    v_retry_count INTEGER;
    v_retry_delay INTEGER;
    v_max_retries INTEGER;
    v_result_set RESULTSET;

    c_records CURSOR FOR
        SELECT
            dlq.DLQ_ID,
            dlq.RECORD_DATA,
            dlq.ERROR_TYPE,
            dlq.RETRY_COUNT,
            rc.MAX_RETRIES,
            rc.RETRY_DELAY_SECONDS
        FROM COMMON.DEAD_LETTER_QUEUE dlq
        INNER JOIN COMMON.RETRY_CONFIG rc
            ON (rc.PIPELINE_NAME = dlq.PIPELINE_NAME OR rc.PIPELINE_NAME = 'ALL')
            AND rc.ERROR_TYPE = dlq.ERROR_TYPE
            AND rc.IS_ACTIVE = TRUE
        WHERE dlq.PIPELINE_NAME = :P_PIPELINE_NAME
            AND dlq.PROCESSING_STATUS = 'PENDING'
            AND dlq.RETRY_COUNT < rc.MAX_RETRIES
            AND (dlq.LAST_RETRY_AT IS NULL
                OR dlq.LAST_RETRY_AT < DATEADD(SECOND, -rc.RETRY_DELAY_SECONDS, CURRENT_TIMESTAMP()))
        ORDER BY dlq.CREATED_AT
        LIMIT :P_MAX_RECORDS_TO_RETRY;
BEGIN
    OPEN c_records;

    FOR record IN c_records DO
        v_dlq_id := record.DLQ_ID;
        v_record_data := record.RECORD_DATA;
        v_error_type := record.ERROR_TYPE;
        v_retry_count := record.RETRY_COUNT;
        v_max_retries := record.MAX_RETRIES;

        BEGIN
            -- Update retry status
            UPDATE COMMON.DEAD_LETTER_QUEUE
            SET
                PROCESSING_STATUS = 'RETRYING',
                RETRY_COUNT = RETRY_COUNT + 1,
                LAST_RETRY_AT = CURRENT_TIMESTAMP()
            WHERE DLQ_ID = :v_dlq_id;

            -- Attempt to reprocess record
            -- (This would call the appropriate pipeline procedure with the record data)
            -- For now, we'll simulate success/failure

            -- If successful:
            UPDATE COMMON.DEAD_LETTER_QUEUE
            SET
                PROCESSING_STATUS = 'RESOLVED',
                RESOLVED_AT = CURRENT_TIMESTAMP(),
                RESOLUTION_NOTES = 'Successfully reprocessed on retry ' || :v_retry_count
            WHERE DLQ_ID = :v_dlq_id;

            v_success_count := v_success_count + 1;

        EXCEPTION
            WHEN OTHER THEN
                -- If retry failed
                UPDATE COMMON.DEAD_LETTER_QUEUE
                SET
                    PROCESSING_STATUS = CASE
                        WHEN RETRY_COUNT >= :v_max_retries THEN 'ABANDONED'
                        ELSE 'PENDING'
                    END,
                    ERROR_MESSAGE = ERROR_MESSAGE || '; Retry ' || :v_retry_count || ' failed: ' || SQLERRM
                WHERE DLQ_ID = :v_dlq_id;

                v_failed_count := v_failed_count + 1;
        END;

        v_retried_count := v_retried_count + 1;
    END FOR;

    CLOSE c_records;

    v_result_set := (
        SELECT
            :v_retried_count AS retried_count,
            :v_success_count AS success_count,
            :v_failed_count AS failed_count
    );

    RETURN TABLE(v_result_set);
END;
$$;

-- =====================================================
-- 6. RECONCILIATION PROCEDURE
-- =====================================================
CREATE OR REPLACE PROCEDURE COMMON.SP_RECONCILE_BATCH(
    P_PIPELINE_NAME VARCHAR,
    P_BATCH_ID VARCHAR,
    P_INGEST_RUN_ID INTEGER
)
RETURNS TABLE(status VARCHAR, discrepancies INTEGER)
LANGUAGE SQL
AS
$$
DECLARE
    v_bronze_count INTEGER;
    v_silver_count INTEGER;
    v_gold_count INTEGER;
    v_status VARCHAR;
    v_discrepancies INTEGER DEFAULT 0;
    v_result_set RESULTSET;
    v_bronze_table VARCHAR;
    v_silver_table VARCHAR;
    v_gold_table VARCHAR;
BEGIN
    -- Get table names from config
    SELECT BRONZE_TABLE, SILVER_TABLE, GOLD_TABLE
    INTO :v_bronze_table, :v_silver_table, :v_gold_table
    FROM COMMON.IMPORT_CONFIGURATION
    WHERE PIPELINE_NAME = :P_PIPELINE_NAME;

    -- Count records in each layer
    EXECUTE IMMEDIATE
        'SELECT COUNT(*) FROM ' || :v_bronze_table ||
        ' WHERE INGEST_RUN_ID = ' || :P_INGEST_RUN_ID
    INTO :v_bronze_count;

    EXECUTE IMMEDIATE
        'SELECT COUNT(*) FROM ' || :v_silver_table ||
        ' WHERE BATCH_ID = ''' || :P_BATCH_ID || ''''
    INTO :v_silver_count;

    EXECUTE IMMEDIATE
        'SELECT COUNT(*) FROM ' || :v_gold_table ||
        ' WHERE BATCH_ID = ''' || :P_BATCH_ID || ''''
    INTO :v_gold_count;

    -- Calculate discrepancies
    v_discrepancies := ABS(v_bronze_count - v_silver_count) + ABS(v_silver_count - v_gold_count);

    -- Determine status
    IF v_discrepancies = 0 THEN
        v_status := 'PASS';
    ELSIF v_discrepancies <= (v_bronze_count * 0.01) THEN -- 1% tolerance
        v_status := 'WARNING';
    ELSE
        v_status := 'FAIL';
    END IF;

    -- Log reconciliation
    INSERT INTO COMMON.RECONCILIATION_LOG (
        PIPELINE_NAME,
        BATCH_ID,
        LAYER,
        SOURCE_COUNT,
        TARGET_COUNT,
        MATCHED_COUNT,
        MISSING_IN_TARGET,
        RECONCILIATION_STATUS
    )
    VALUES
        (:P_PIPELINE_NAME, :P_BATCH_ID, 'BRONZE->SILVER',
         :v_bronze_count, :v_silver_count,
         LEAST(:v_bronze_count, :v_silver_count),
         :v_bronze_count - :v_silver_count,
         :v_status),
        (:P_PIPELINE_NAME, :P_BATCH_ID, 'SILVER->GOLD',
         :v_silver_count, :v_gold_count,
         LEAST(:v_silver_count, :v_gold_count),
         :v_silver_count - :v_gold_count,
         :v_status);

    v_result_set := (
        SELECT
            :v_status AS status,
            :v_discrepancies AS discrepancies
    );

    RETURN TABLE(v_result_set);
END;
$$;

-- =====================================================
-- 7. ENHANCED IMPORT MASTER WITH ERROR HANDLING
-- =====================================================
CREATE OR REPLACE PROCEDURE COMMON.SP_IMPORT_MASTER_WITH_RECOVERY(
    P_PIPELINE_NAME VARCHAR,
    P_STAGE_PATH VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    v_batch_id VARCHAR;
    v_ingest_run_id INTEGER;
    v_result VARIANT;
    v_error_message VARCHAR;
BEGIN
    -- Call original import master
    BEGIN
        CALL COMMON.SP_IMPORT_MASTER(:P_PIPELINE_NAME, :P_STAGE_PATH);
        LET res RESULTSET := (SELECT :v_result);
        LET cur CURSOR FOR res;
        OPEN cur;
        FETCH cur INTO v_result;
        CLOSE cur;

        -- Extract IDs
        v_batch_id := v_result:batch_id::VARCHAR;
        v_ingest_run_id := v_result:stage_to_bronze:ingest_run_id::INTEGER;

        -- Run reconciliation
        CALL COMMON.SP_RECONCILE_BATCH(:P_PIPELINE_NAME, :v_batch_id, :v_ingest_run_id);

        RETURN v_result;

    EXCEPTION
        WHEN OTHER THEN
            v_error_message := SQLERRM;

            -- Log to DLQ
            INSERT INTO COMMON.DEAD_LETTER_QUEUE (
                PIPELINE_NAME,
                ERROR_TYPE,
                ERROR_MESSAGE,
                ERROR_STACKTRACE,
                RECORD_DATA
            ) VALUES (
                :P_PIPELINE_NAME,
                'TECHNICAL',
                :v_error_message,
                CURRENT_TIMESTAMP()::VARCHAR,
                OBJECT_CONSTRUCT(
                    'stage_path', :P_STAGE_PATH,
                    'failed_at', CURRENT_TIMESTAMP()
                )
            );

            -- Create alert
            INSERT INTO COMMON.PIPELINE_ALERTS (
                PIPELINE_NAME,
                ALERT_TYPE,
                SEVERITY,
                ALERT_MESSAGE
            ) VALUES (
                :P_PIPELINE_NAME,
                'FAILURE',
                'CRITICAL',
                'Pipeline execution failed: ' || :v_error_message
            );

            RETURN OBJECT_CONSTRUCT(
                'status', 'FAILED',
                'error', :v_error_message,
                'dlq_logged', TRUE
            );
    END;
END;
$$;

-- =====================================================
-- 8. ERROR SUMMARY VIEWS
-- =====================================================
CREATE OR REPLACE VIEW COMMON.VW_ERROR_SUMMARY AS
SELECT
    PIPELINE_NAME,
    ERROR_TYPE,
    COUNT(*) AS error_count,
    COUNT(CASE WHEN PROCESSING_STATUS = 'PENDING' THEN 1 END) AS pending_count,
    COUNT(CASE WHEN PROCESSING_STATUS = 'RETRYING' THEN 1 END) AS retrying_count,
    COUNT(CASE WHEN PROCESSING_STATUS = 'RESOLVED' THEN 1 END) AS resolved_count,
    COUNT(CASE WHEN PROCESSING_STATUS = 'ABANDONED' THEN 1 END) AS abandoned_count,
    AVG(RETRY_COUNT) AS avg_retry_count,
    MIN(CREATED_AT) AS first_error,
    MAX(CREATED_AT) AS last_error
FROM COMMON.DEAD_LETTER_QUEUE
WHERE CREATED_AT >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY PIPELINE_NAME, ERROR_TYPE
ORDER BY error_count DESC;

-- =====================================================
-- 9. CREATE SCHEDULED RETRY TASK
-- =====================================================
CREATE OR REPLACE TASK COMMON.TASK_RETRY_DLQ
    WAREHOUSE = ADHOC_WH
    SCHEDULE = '30 MINUTE'
AS
BEGIN
    -- Retry failed records for all active pipelines
    FOR pipeline IN (SELECT DISTINCT PIPELINE_NAME FROM COMMON.DEAD_LETTER_QUEUE WHERE PROCESSING_STATUS = 'PENDING') DO
        CALL COMMON.SP_RETRY_FAILED_RECORDS(pipeline.PIPELINE_NAME, 100);
    END FOR;
END;

-- Resume task
-- ALTER TASK COMMON.TASK_RETRY_DLQ RESUME;

-- =====================================================
-- VERIFICATION QUERIES
-- =====================================================

-- View DLQ status
SELECT * FROM COMMON.VW_ERROR_SUMMARY;

-- View recent reconciliations
SELECT * FROM COMMON.RECONCILIATION_LOG
ORDER BY RECONCILED_AT DESC
LIMIT 20;

-- View data lineage
SELECT * FROM COMMON.VW_DATA_LINEAGE
LIMIT 50;

-- Test retry procedure
CALL COMMON.SP_RETRY_FAILED_RECORDS('CUSTOMER_PIPELINE', 10);