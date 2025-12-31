-- =======================================================================================================================================
-- PROCEDURE 1: UNIVERSAL_STAGE_TO_BRONZE
-- Purpose: Load data from Stage/Stream to Bronze for ANY entity
-- =======================================================================================================================================
call SP_STAGE_TO_BRONZE('ORDER_STREAM', NULL);
CREATE OR REPLACE PROCEDURE COMMON.SP_STAGE_TO_BRONZE(
    P_SOURCE_NAME VARCHAR,
    P_FILE_NAME VARCHAR DEFAULT NULL  -- NULL for stream sources
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    V_ROWS_INSERTED INTEGER DEFAULT 0;
    V_INGEST_RUN_ID NUMBER;
    V_ERROR_MESSAGE VARCHAR(5000);
    V_START_TIME TIMESTAMP_TZ;
    V_END_TIME TIMESTAMP_TZ;
    V_EXECUTION_DURATION INTEGER;

    -- Dynamic SQL from metadata views
    V_INGESTION_TYPE VARCHAR;
    V_TEMP_TABLE VARCHAR;
    V_TEMP_TABLE_DDL VARCHAR;
    V_COPY_SQL VARCHAR;
    V_STREAM_INSERT_SQL VARCHAR;
    V_BRONZE_INSERT_SQL VARCHAR;
    V_BRONZE_TABLE VARCHAR;
    V_LANDING_PATH VARCHAR;

BEGIN
    V_START_TIME := CURRENT_TIMESTAMP();

    -- Fetch metadata-driven SQL from views
    SELECT
        ingestion_type,
        temp_table,
        temp_table_ddl,
        file_copy_sql,
        stream_insert_sql,
        bronze_insert_sql,
        bronze_table,
        landing_path
    INTO
        V_INGESTION_TYPE,
        V_TEMP_TABLE,
        V_TEMP_TABLE_DDL,
        V_COPY_SQL,
        V_STREAM_INSERT_SQL,
        V_BRONZE_INSERT_SQL,
        V_BRONZE_TABLE,
        V_LANDING_PATH
    FROM COMMON.VW_INGESTION_PIPELINE_SQL
    WHERE source_name = :P_SOURCE_NAME;

    IF (V_TEMP_TABLE_DDL IS NULL) THEN
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', 'Source not found in metadata: ' || :P_SOURCE_NAME
        );
    END IF;

    -- Start transaction
    BEGIN TRANSACTION;

    -- Generate sequence
    SELECT DATAVELOCITY.BRONZE.SEQ_ORDER_INGEST_RUN_ID.NEXTVAL INTO :V_INGEST_RUN_ID;

    -- Step 1: Create temp table
    EXECUTE IMMEDIATE V_TEMP_TABLE_DDL;

    -- Step 2: Load data based on ingestion type
    IF (V_INGESTION_TYPE = 'BATCH') THEN
        -- Replace placeholders
        V_COPY_SQL := REPLACE(V_COPY_SQL, ':P_FILE_NAME', P_FILE_NAME);
        EXECUTE IMMEDIATE V_COPY_SQL;

    ELSEIF (V_INGESTION_TYPE = 'STREAM') THEN
        EXECUTE IMMEDIATE V_STREAM_INSERT_SQL;

    ELSE
        ROLLBACK;
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', 'Unsupported ingestion type: ' || V_INGESTION_TYPE
        );
    END IF;

    -- EXECUTE IMMEDIATE
    -- 'SELECT COUNT(*) FROM ' || V_TEMP_TABLE
    -- INTO :V_ROWS_INSERTED;

    -- IF (V_ROWS_INSERTED = 0) THEN
    --     ROLLBACK;
    --     RETURN OBJECT_CONSTRUCT(
    --         'STATUS', 'FAILED',
    --         'ERROR', 'No records loaded from ' || V_INGESTION_TYPE
    --     );
    -- END IF;

    -- -- Step 3: Insert into Bronze
    V_BRONZE_INSERT_SQL := REPLACE(V_BRONZE_INSERT_SQL, ':P_INGEST_RUN_ID', V_INGEST_RUN_ID::VARCHAR);
    EXECUTE IMMEDIATE V_BRONZE_INSERT_SQL;

    -- -- Commit transaction
    COMMIT;

    -- -- -- Cleanup
    -- -- V_SQL := 'SELECT COUNT(*) FROM IDENTIFIER(?)';

    -- -- EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || V_TEMP_TABLE || ';
    -- -- INTO :V_ROWS_INSERTED;

    V_END_TIME := CURRENT_TIMESTAMP();
    V_EXECUTION_DURATION := DATEDIFF(SECOND, V_START_TIME, V_END_TIME);

    RETURN OBJECT_CONSTRUCT(
        'STATUS', 'SUCCESS',
        'MESSAGE', 'Data loaded successfully from ' || V_INGESTION_TYPE,
        'SOURCE_NAME', :P_SOURCE_NAME,
        'INGESTION_TYPE', :V_INGESTION_TYPE,
        'ROWS_INSERTED', :V_ROWS_INSERTED,
        'INGEST_RUN_ID', :V_INGEST_RUN_ID,
        'EXECUTION_TIME_SEC', :V_EXECUTION_DURATION
    );

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;

        V_ERROR_MESSAGE := SQLERRM;
        V_END_TIME := CURRENT_TIMESTAMP();
        V_EXECUTION_DURATION := DATEDIFF(SECOND, V_START_TIME, V_END_TIME);

        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', V_ERROR_MESSAGE,
            'SOURCE_NAME', P_SOURCE_NAME,
            'EXECUTION_TIME_SEC', V_EXECUTION_DURATION
        );
END;
$$;

-- =======================================================================================================================================
-- PROCEDURE 2: UNIVERSAL_BRONZE_TO_SILVER
-- Purpose: Load data from Bronze to Silver for ANY entity with DQ validation P_SOURCE_NAME
-- =======================================================================================================================================
CREATE OR REPLACE PROCEDURE COMMON.SP_BRONZE_TO_SILVER(
P_SOURCE_NAME VARCHAR,
    P_INGEST_RUN_ID INTEGER,
    P_BATCH_ID VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    V_ROWS_AFFECTED INTEGER DEFAULT 0;
    V_START_TIME TIMESTAMP_TZ;
    V_END_TIME TIMESTAMP_TZ;
    V_EXECUTION_DURATION INTEGER;
    V_ERROR_MESSAGE VARCHAR(5000);

    -- Dynamic SQL from metadata views
    V_BRONZE_TABLE VARCHAR;
    V_LOAD_ERROR_TABLE VARCHAR;
    V_STAGE_TABLE VARCHAR;
    V_SILVER_TABLE VARCHAR;
    V_MERGE_SQL VARCHAR;
    V_APPEND_SQL VARCHAR;
    V_LOAD_TYPE VARCHAR;

    -- DQ tracking
    V_BRONZE_ROW_COUNT INTEGER DEFAULT 0;
    V_VALID_ROW_COUNT INTEGER DEFAULT 0;
    V_INVALID_ROW_COUNT INTEGER DEFAULT 0;
    V_DQ_RESULT VARIANT;

BEGIN
    V_START_TIME := CURRENT_TIMESTAMP();

    -- Fetch metadata-driven SQL
    SELECT
        bronze_table,
        load_error_table,
        stage_table,
        silver_table,
        merge_sql,
        append_sql,
        CASE WHEN merge_sql IS NOT NULL THEN 'MERGE' ELSE 'APPEND' END
    INTO
        :V_BRONZE_TABLE,
        :V_LOAD_ERROR_TABLE,
        :V_STAGE_TABLE,
        :V_SILVER_TABLE,
        :V_MERGE_SQL,
        :V_APPEND_SQL,
        :V_LOAD_TYPE
    FROM COMMON.VW_INGESTION_PIPELINE_SQL
    WHERE source_name = :P_SOURCE_NAME;

    IF (V_BRONZE_TABLE IS NULL) THEN
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', 'Source not found in metadata: ' || P_SOURCE_NAME
        );
    END IF;

    -- -- Get bronze row count
    -- V_COUNT_RESULT := (EXECUTE IMMEDIATE 'SELECT COUNT(*) AS CNT FROM ' || V_BRONZE_TABLE || ' WHERE INGEST_RUN_ID = ' || :P_INGEST_RUN_ID);
    -- LET C1 CURSOR FOR V_COUNT_RESULT;
    -- OPEN C1;
    -- FETCH C1 INTO V_BRONZE_ROW_COUNT;
    -- CLOSE C1;

    -- IF (V_BRONZE_ROW_COUNT = 0) THEN
    --     RETURN OBJECT_CONSTRUCT(
    --         'STATUS', 'FAILED',
    --         'ERROR', 'No records found for INGEST_RUN_ID: ' || :P_INGEST_RUN_ID
    --     );
    -- END IF;

    -- Create staging table with DQ flags
    EXECUTE IMMEDIATE '
        CREATE OR REPLACE TEMPORARY TABLE ' || V_STAGE_TABLE || ' AS
        SELECT *,
               TRUE AS IS_VALID
        FROM ' || V_BRONZE_TABLE || '
        WHERE INGEST_RUN_ID = ' || P_INGEST_RUN_ID;

    -- Run DQ validation (if configured)
    BEGIN
        CALL BRONZE.SP_EXECUTE_DATA_QUALITY_VALIDATION(
            V_STAGE_TABLE,
            V_BRONZE_TABLE,
            V_LOAD_ERROR_TABLE,
            P_INGEST_RUN_ID
        ) INTO V_DQ_RESULT;
    EXCEPTION
        WHEN OTHER THEN
            -- DQ validation not configured, continue
            V_DQ_RESULT := OBJECT_CONSTRUCT('STATUS', 'SKIPPED', 'MESSAGE', 'No DQ rules configured');
    END;

    -- -- Count valid records
    -- V_COUNT_RESULT := (EXECUTE IMMEDIATE 'SELECT COUNT(*) AS CNT FROM ' || V_STAGE_TABLE || ' WHERE IS_VALID = TRUE');
    -- LET C2 CURSOR FOR V_COUNT_RESULT;
    -- OPEN C2;
    -- FETCH C2 INTO V_VALID_ROW_COUNT;
    -- CLOSE C2;

    -- V_INVALID_ROW_COUNT := V_BRONZE_ROW_COUNT - V_VALID_ROW_COUNT;

    IF (V_LOAD_TYPE = 'MERGE') THEN
        -- Replace table reference
        V_MERGE_SQL := REPLACE(V_MERGE_SQL, V_BRONZE_TABLE, V_STAGE_TABLE);
        V_MERGE_SQL := REPLACE(V_MERGE_SQL, ':P_BATCH_ID', '''' || P_BATCH_ID || '''');
        V_MERGE_SQL := REPLACE(V_MERGE_SQL, ':P_INGEST_RUN_ID', '''' || P_INGEST_RUN_ID || '''');

        EXECUTE IMMEDIATE V_MERGE_SQL;
    ELSE
        V_APPEND_SQL := REPLACE(V_APPEND_SQL, V_BRONZE_TABLE, V_STAGE_TABLE);
        V_APPEND_SQL := REPLACE(V_APPEND_SQL, ':P_BATCH_ID', '''' || P_BATCH_ID || '''');
        V_APPEND_SQL := REPLACE(V_APPEND_SQL, ':P_INGEST_RUN_ID', '''' || P_INGEST_RUN_ID || '''');
        EXECUTE IMMEDIATE V_APPEND_SQL;
    END IF;

    V_ROWS_AFFECTED := SQLROWCOUNT;

    -- Log to INGEST_RUN
    V_END_TIME := CURRENT_TIMESTAMP();
    V_EXECUTION_DURATION := DATEDIFF(SECOND, V_START_TIME, V_END_TIME);

    -- INSERT INTO COMMON.INGEST_RUN(
    --     INGEST_RUN_ID, PIPELINE_NAME, SOURCE_TABLE, V_LOAD_ERROR_TABLE, RUN_STATUS,
    --     SOURCE_ROW_COUNT, VALID_ROW_COUNT, INVALID_ROW_COUNT, EXECUTION_DURATION_SEC,
    --     ERROR_MESSAGE, EXECUTED_AT, EXECUTED_BY
    -- )
    -- VALUES(
    --     :P_INGEST_RUN_ID,
    --     :P_SOURCE_NAME,
    --     :V_BRONZE_TABLE,
    --     :V_LOAD_ERROR_TABLE,
    --     'SUCCESS',
    --     :V_BRONZE_ROW_COUNT,
    --     :V_VALID_ROW_COUNT,
    --     :V_INVALID_ROW_COUNT,
    --     :V_EXECUTION_DURATION,
    --     'None',
    --     :V_END_TIME,
    --     CURRENT_USER()
    -- );

    -- Cleanup
    -- EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS ' || V_STAGE_TABLE;

    RETURN OBJECT_CONSTRUCT(
        'STATUS', 'SUCCESS',
        'SOURCE_NAME', P_SOURCE_NAME,
        'LOAD_TYPE', V_LOAD_TYPE,
        'ROWS_AFFECTED', V_ROWS_AFFECTED,
        'BRONZE_ROW_COUNT', V_BRONZE_ROW_COUNT,
        'VALID_ROW_COUNT', V_VALID_ROW_COUNT,
        'INVALID_ROW_COUNT', V_INVALID_ROW_COUNT,
        'EXECUTION_TIME_SEC', V_EXECUTION_DURATION,
        'DQ_RESULT', V_DQ_RESULT
    );

EXCEPTION
    WHEN OTHER THEN
        V_ERROR_MESSAGE := SQLERRM;
        V_END_TIME := CURRENT_TIMESTAMP();
        V_EXECUTION_DURATION := DATEDIFF(SECOND, V_START_TIME, V_END_TIME);

        INSERT INTO COMMON.INGEST_RUN(
            INGEST_RUN_ID, PIPELINE_NAME, SOURCE_TABLE, LOAD_ERROR_TABLE, RUN_STATUS,
            SOURCE_ROW_COUNT, VALID_ROW_COUNT, INVALID_ROW_COUNT, EXECUTION_DURATION_SEC,
            ERROR_MESSAGE, EXECUTED_AT, EXECUTED_BY
        )
        VALUES(
            :P_INGEST_RUN_ID,
            :P_SOURCE_NAME,
            :V_BRONZE_TABLE,
            :V_LOAD_ERROR_TABLE,
            'FAILED',
            :V_BRONZE_ROW_COUNT,
            :V_VALID_ROW_COUNT,
            :V_INVALID_ROW_COUNT,
            :V_EXECUTION_DURATION,
            :V_ERROR_MESSAGE,
            :V_END_TIME,
            CURRENT_USER()
        );

        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', :V_ERROR_MESSAGE,
            'SOURCE_NAME', :P_SOURCE_NAME,
            'EXECUTION_TIME_SEC', :V_EXECUTION_DURATION
        );
END;
$$;

-- =======================================================================================================================================
-- PROCEDURE 3: UNIVERSAL_SILVER_TO_GOLD
-- Purpose: Load data from Silver to Gold with SCD2 support for ANY entity
-- =======================================================================================================================================

CREATE OR REPLACE PROCEDURE COMMON.SP_SILVER_TO_GOLD(
    P_SOURCE_NAME VARCHAR,
    P_BATCH_ID VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    V_ROWS_INSERTED INTEGER DEFAULT 0;
    V_ROWS_UPDATED INTEGER DEFAULT 0;
    V_ROWS_UNCHANGED INTEGER DEFAULT 0;
    V_START_TIME TIMESTAMP_TZ;
    V_END_TIME TIMESTAMP_TZ;
    V_EXECUTION_DURATION INTEGER;
    V_ERROR_MESSAGE VARCHAR(5000);

    -- Dynamic SQL from metadata views
    V_SILVER_TABLE VARCHAR;
    V_GOLD_TABLE VARCHAR;
    V_STAGING_SQL VARCHAR;
    V_EXPIRE_SQL VARCHAR;
    V_INSERT_SQL VARCHAR;
    V_SOURCE_ROW_COUNT INTEGER DEFAULT 0;

BEGIN
    V_START_TIME := CURRENT_TIMESTAMP();

    -- Fetch SCD2 SQL from views
    SELECT
        silver_table,
        gold_table,
        staging_sql,
        expire_sql,
        insert_sql
    INTO
        V_SILVER_TABLE,
        V_GOLD_TABLE,
        V_STAGING_SQL,
        V_EXPIRE_SQL,
        V_INSERT_SQL
    FROM COMMON.VW_INGESTION_PIPELINE_SQL
    WHERE source_name = :P_SOURCE_NAME;

    IF(V_SILVER_TABLE IS NULL)THEN
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', 'Source not configured for SCD2: ' || P_SOURCE_NAME
        );
    END IF;

    -- Check source row count
    -- EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM INTO :V_SOURCE_ROW_COUNT ' || V_SILVER_TABLE || ' WHERE BATCH_ID = ''' || :P_BATCH_ID || ''' ';

    -- IF(V_SOURCE_ROW_COUNT = 0) THEN
    --     RETURN OBJECT_CONSTRUCT(
    --         'STATUS', 'FAILED',
    --         'ERROR', 'No records found for BATCH_ID: ' || :P_BATCH_ID
    --     );
    -- END IF;

    -- Step 1: Create staging table with change detection
    V_STAGING_SQL := REPLACE(V_STAGING_SQL, ':P_BATCH_ID', '''' || P_BATCH_ID || '''');
    EXECUTE IMMEDIATE V_STAGING_SQL USING (P_BATCH_ID);

    -- Step 2: Expire changed records
    EXECUTE IMMEDIATE V_EXPIRE_SQL;
    V_ROWS_UPDATED := SQLROWCOUNT;

    -- Step 3: Insert new versions
    EXECUTE IMMEDIATE V_INSERT_SQL;
    V_ROWS_INSERTED := SQLROWCOUNT;

    -- Get unchanged count
    -- EXECUTE IMMEDIATE 'SELECT COUNT(*) INTO V_ROWS_UNCHANGED FROM TEMP_' || V_STAGE_TABLE || WHERE SCD_ACTION = ''UNCHANGED''' ;

    -- Cleanup
    EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS TEMP_' || UPPER(P_SOURCE_NAME) || '';

    V_END_TIME := CURRENT_TIMESTAMP();
    V_EXECUTION_DURATION := DATEDIFF(SECOND, V_START_TIME, V_END_TIME);

    RETURN OBJECT_CONSTRUCT(
        'STATUS', 'SUCCESS',
        'SOURCE_NAME', P_SOURCE_NAME,
        'BATCH_ID', P_BATCH_ID,
        'SOURCE_ROW_COUNT', V_SOURCE_ROW_COUNT,
        'ROWS_INSERTED', V_ROWS_INSERTED,
        'ROWS_UPDATED', V_ROWS_UPDATED,
        'ROWS_UNCHANGED', V_ROWS_UNCHANGED,
        'EXECUTION_TIME_SEC', V_EXECUTION_DURATION
    );

EXCEPTION
    WHEN OTHER THEN
        V_ERROR_MESSAGE := SQLERRM;
        V_END_TIME := CURRENT_TIMESTAMP();
        V_EXECUTION_DURATION := DATEDIFF(SECOND, V_START_TIME, V_END_TIME);

        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', V_ERROR_MESSAGE,
            'SOURCE_NAME', P_SOURCE_NAME,
            'EXECUTION_TIME_SEC', V_EXECUTION_DURATION
        );
END;
$$;
-- =======================================================================================================================================
-- MASTER ORCHESTRATION PROCEDURE
-- =======================================================================================================================================
CALL SP_ETL_MASTER('ORDER_BATCH', 'order_brz.csv');
select * from gold.fact_order;
CREATE OR REPLACE PROCEDURE COMMON.SP_ETL_MASTER(
    P_SOURCE_NAME VARCHAR,
    P_FILE_NAME   VARCHAR DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    V_BATCH_ID        VARCHAR;
    V_INGEST_RUN_ID   INTEGER;
    V_START_TIME      TIMESTAMP_TZ;
    V_END_TIME        TIMESTAMP_TZ;

    V_STB_RESULT VARIANT;
    V_BTS_RESULT VARIANT;
    V_STG_RESULT VARIANT;
BEGIN
    V_START_TIME := CURRENT_TIMESTAMP();
    V_BATCH_ID   := 'BATCH_' || UUID_STRING();

    /* Phase 1: Stage → Bronze */
    CALL COMMON.SP_STAGE_TO_BRONZE(
        :P_SOURCE_NAME,
        :P_FILE_NAME
    )
    INTO :V_STB_RESULT;

    IF (V_STB_RESULT:STATUS::VARCHAR <> 'SUCCESS') THEN
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'PHASE', 'STAGE_TO_BRONZE',
            'RESULT', V_STB_RESULT
        );
    END IF;

    V_INGEST_RUN_ID := V_STB_RESULT:INGEST_RUN_ID::INTEGER;

    /* Phase 2: Bronze → Silver */
    CALL COMMON.SP_BRONZE_TO_SILVER(
        :P_SOURCE_NAME,
        :V_INGEST_RUN_ID,
        :V_BATCH_ID
    )
    INTO :V_BTS_RESULT;

    IF (V_BTS_RESULT:STATUS::VARCHAR <> 'SUCCESS') THEN
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'PHASE', 'BRONZE_TO_SILVER',
            'RESULT', V_BTS_RESULT
        );
    END IF;

    /* Phase 3: Silver → Gold */
    CALL COMMON.SP_SILVER_TO_GOLD(
        :P_SOURCE_NAME,
        :V_BATCH_ID
    )
    INTO :V_STG_RESULT;

    V_END_TIME := CURRENT_TIMESTAMP();

    RETURN OBJECT_CONSTRUCT(
        'STATUS', 'SUCCESS',
        'SOURCE_NAME', P_SOURCE_NAME,
        'BATCH_ID', V_BATCH_ID,
        'INGEST_RUN_ID', V_INGEST_RUN_ID,
        'TOTAL_DURATION_SEC', DATEDIFF(SECOND, V_START_TIME, V_END_TIME),
        'STAGE_TO_BRONZE', V_STB_RESULT,
        'BRONZE_TO_SILVER', V_BTS_RESULT,
        'SILVER_TO_GOLD', V_STG_RESULT
    );
END;
$$;
