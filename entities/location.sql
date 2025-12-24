-- ====================================================================================================
-- Location
-- ====================================================================================================
-- CHANGE_CONTEXT
USE DATABASE SWIGGY;
USE SCHEMA BRONZE;
USE WAREHOUSE ADHOC_WH;
-- ====================================================================================================
-- CREATE LOCATION_BRZ
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.LOCATION_BRZ (
    LOCATION_BRZ_ID INTEGER AUTOINCREMENT,
    CITY VARCHAR,
    STATE VARCHAR,
    ZIP_CODE VARCHAR,

    -- RAW COLUMNS
    CITY_RAW VARCHAR,
    STATE_RAW VARCHAR,
    ZIP_CODE_RAW VARCHAR,

    INGEST_RUN_ID INTEGER,
    CREATED_AT TIMESTAMP_TZ(9)
);
ALTER TABLE BRONZE.LOCATION_BRZ CLUSTER BY (INGEST_RUN_ID);

-- CREATING SEQUNCE TO GENERATE INGEST_RUN_ID
CREATE OR REPLACE SEQUENCE SEQ_LOCATION_INGEST_RUN_ID START = 1 INCREMENT = 1;

-- ----------------------------------------------------------------------------------------------------
-- CREATE LOCATION_LOAD_ERROR
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.LOCATION_LOAD_ERROR(
    ERROR_ID INTEGER PRIMARY KEY,
    VALIDATE_COLUMN VARCHAR(50),
    VALIDATION_TYPE VARCHAR(30),
    VALIDATION_ERROR_MSG VARCHAR(200),
    INGEST_RUN_ID INTEGER
);

-- ----------------------------------------------------------------------------------------------------
-- CREATING LOCATION_SLV
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.LOCATION_SLV (
    LOCATION_SLV_ID INTEGER AUTOINCREMENT PRIMARY KEY,
    CITY VARCHAR(100) ,
    STATE VARCHAR(100) ,
    STATE_CODE VARCHAR(2),
    IS_UNION_TERRITORY BOOLEAN DEFAULT FALSE,
    CAPITAL_CITY_FLAG BOOLEAN DEFAULT FALSE,
    CITY_TIER VARCHAR(6),
    ZIP_CODE VARCHAR(10),
    STATUS VARCHAR(10),
    BATCH_ID VARCHAR(36),
    CREATED_AT TIMESTAMP_TZ(9),
    UPDATED_AT TIMESTAMP_TZ(9)
)
COMMENT = 'LOCATION ENTITY UNDER CLEAN SCHEMA WITH APPROPRIATE DATA TYPE UNDER CLEAN SCHEMA LAYER, DATA IS POPULATED USING MERGE STATEMENT FROM THE STAGE LAYER LOCATION TABLE. THIS TABLE DOES NOT SUPPORT SCD2';

ALTER TABLE SILVER.LOCATION_SLV CLUSTER BY (STATE);
-- ----------------------------------------------------------------------------------------------------
-- CREAING DIM_LOCATION
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.DIM_LOCATION (
    LOCATION_ID INTEGER PRIMARY KEY,
    CITY VARCHAR(100) NOT NULL,
    STATE VARCHAR(100) NOT NULL,
    STATE_CODE VARCHAR(2),
    IS_UNION_TERRITORY BOOLEAN DEFAULT FALSE,
    CAPITAL_CITY_FLAG BOOLEAN DEFAULT FALSE,
    CITY_TIER VARCHAR(6),
    ZIP_CODE VARCHAR(10) NOT NULL,
    STATUS VARCHAR(10) DEFAULT 'ACTIVE',
    EFF_START_DT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP(),
    EFF_END_DT TIMESTAMP_TZ(9) DEFAULT '9999-12-31 23:59:59'::TIMESTAMP_TZ,
    BATCH_ID VARCHAR(36) NOT NULL,
    CREATED_AT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP()
);

ALTER TABLE GOLD.DIM_LOCATION CLUSTER BY (STATE);
-- ----------------------------------------------------------------------------------------------------
-- STAGE TO BRONZE
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE BRONZE.SP_LOCATION_STAGE_TO_BRONZE(
    P_PIPELINE_NAME VARCHAR,
    P_FILE_NAME VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
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

    -- Construct full file path
    V_FILE_PATH := V_SOURCE_LOCATION || P_FILE_NAME;

    -- Start explicit transaction
    BEGIN TRANSACTION;

    -- CREATE TEMP TABLE
    CREATE OR REPLACE TEMPORARY TABLE BRONZE.TEMP_LOCATION_LOAD (
        CITY VARCHAR,
        STATE VARCHAR,
        ZIP_CODE VARCHAR
    );

    -- Copy data from stage
    EXECUTE IMMEDIATE
    '
        COPY INTO BRONZE.TEMP_LOCATION_LOAD (CITY, STATE, ZIP_CODE)
        FROM (
            SELECT $1::STRING, $2::STRING, $3::STRING
            FROM ''' || V_FILE_PATH || '''
        )
        FILE_FORMAT = (FORMAT_NAME = ''' || V_FILE_FORMAT || ''')
        ON_ERROR = ABORT_STATEMENT
    ';

    -- Get row count
    SELECT COUNT(*) INTO :V_ROWS_INSERTED FROM BRONZE.TEMP_LOCATION_LOAD;

    -- Validate data loaded
    IF (V_ROWS_INSERTED = 0) THEN
        ROLLBACK;
        DROP TABLE IF EXISTS TEMP_LOCATION_LOAD;
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', 'No records loaded from file',
            'FILE_PATH', P_FILE_NAME,
            'ROWS_INSERTED', 0,
            'INGEST_RUN_ID', 0
        );
    END IF;

    -- Consume sequence
    SELECT SWIGGY.BRONZE.SEQ_LOCATION_INGEST_RUN_ID.NEXTVAL INTO :V_INGEST_RUN_ID;

    -- Insert into bronze table
    INSERT INTO BRONZE.LOCATION_BRZ (
    CITY, STATE, ZIP_CODE, CITY_RAW, STATE_RAW, ZIP_CODE_RAW, INGEST_RUN_ID, CREATED_AT
    )
    SELECT
        TRIM(CITY),
        TRIM(STATE),
        TRY_TO_NUMBER(ZIP_CODE),
        CITY,
        STATE,
        ZIP_CODE,
        :V_INGEST_RUN_ID,
        CURRENT_TIMESTAMP()
    FROM TEMP_LOCATION_LOAD;

     -- Commit transaction
    COMMIT;

    -- Cleanup
    DROP TABLE IF EXISTS TEMP_LOCATION_LOAD;

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
        DROP TABLE IF EXISTS TEMP_LOCATION_LOAD;

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
CREATE OR REPLACE PROCEDURE SILVER.SP_LOCATION_BRONZE_TO_SILVER(
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
            SELECT DISTINCT
                UPPER(TRIM(CITY)) AS CITY,
                UPPER(TRIM(STATE)) AS STATE,
                UPPER(TRIM(ZIP_CODE)) AS ZIP_CODE,

                -- State code mapping with proper SQL escaping
                CASE
                    WHEN UPPER(TRIM(STATE)) = ''MAHARASHTRA'' THEN ''MH''
                    WHEN UPPER(TRIM(STATE)) = ''DELHI'' THEN ''DL''
                    WHEN UPPER(TRIM(STATE)) = ''KARNATAKA'' THEN ''KA''
                    WHEN UPPER(TRIM(STATE)) = ''TAMIL NADU'' THEN ''TN''
                    WHEN UPPER(TRIM(STATE)) = ''UTTAR PRADESH'' THEN ''UP''
                    WHEN UPPER(TRIM(STATE)) = ''WEST BENGAL'' THEN ''WB''
                    WHEN UPPER(TRIM(STATE)) = ''RAJASTHAN'' THEN ''RJ''
                    WHEN UPPER(TRIM(STATE)) = ''GUJARAT'' THEN ''GJ''
                    WHEN UPPER(TRIM(STATE)) = ''MADHYA PRADESH'' THEN ''MP''
                    WHEN UPPER(TRIM(STATE)) = ''ANDHRA PRADESH'' THEN ''AP''
                    WHEN UPPER(TRIM(STATE)) = ''TELANGANA'' THEN ''TS''
                    WHEN UPPER(TRIM(STATE)) = ''KERALA'' THEN ''KL''
                    WHEN UPPER(TRIM(STATE)) = ''PUNJAB'' THEN ''PB''
                    WHEN UPPER(TRIM(STATE)) = ''HARYANA'' THEN ''HR''
                    WHEN UPPER(TRIM(STATE)) = ''BIHAR'' THEN ''BR''
                    WHEN UPPER(TRIM(STATE)) = ''ODISHA'' THEN ''OR''
                    WHEN UPPER(TRIM(STATE)) = ''ASSAM'' THEN ''AS''
                    WHEN UPPER(TRIM(STATE)) = ''JHARKHAND'' THEN ''JH''
                    WHEN UPPER(TRIM(STATE)) = ''CHHATTISGARH'' THEN ''CG''
                    WHEN UPPER(TRIM(STATE)) = ''UTTARAKHAND'' THEN ''UK''
                    WHEN UPPER(TRIM(STATE)) = ''GOA'' THEN ''GA''
                    WHEN UPPER(TRIM(STATE)) = ''HIMACHAL PRADESH'' THEN ''HP''
                    WHEN UPPER(TRIM(STATE)) = ''MANIPUR'' THEN ''MN''
                    WHEN UPPER(TRIM(STATE)) = ''MEGHALAYA'' THEN ''ML''
                    WHEN UPPER(TRIM(STATE)) = ''MIZORAM'' THEN ''MZ''
                    WHEN UPPER(TRIM(STATE)) = ''NAGALAND'' THEN ''NL''
                    WHEN UPPER(TRIM(STATE)) = ''SIKKIM'' THEN ''SK''
                    WHEN UPPER(TRIM(STATE)) = ''TRIPURA'' THEN ''TR''
                    WHEN UPPER(TRIM(STATE)) = ''ARUNACHAL PRADESH'' THEN ''AR''
                    -- Union Territories
                    WHEN UPPER(TRIM(STATE)) = ''CHANDIGARH'' THEN ''CH''
                    WHEN UPPER(TRIM(STATE)) = ''PUDUCHERRY'' THEN ''PY''
                    WHEN UPPER(TRIM(STATE)) = ''ANDAMAN AND NICOBAR ISLANDS'' THEN ''AN''
                    WHEN UPPER(TRIM(STATE)) = ''LAKSHADWEEP'' THEN ''LD''
                    WHEN UPPER(TRIM(STATE)) = ''DADRA AND NAGAR HAVELI AND DAMAN AND DIU'' THEN ''DN''
                    WHEN UPPER(TRIM(STATE)) = ''LADAKH'' THEN ''LA''
                    WHEN UPPER(TRIM(STATE)) = ''JAMMU AND KASHMIR'' THEN ''JK''
                    ELSE LEFT(UPPER(TRIM(STATE)), 2)
                END AS STATE_CODE,

                -- Union Territory flag
                CASE
                    WHEN UPPER(TRIM(STATE)) IN (
                        ''DELHI'', ''CHANDIGARH'', ''PUDUCHERRY'',
                        ''ANDAMAN AND NICOBAR ISLANDS'', ''LAKSHADWEEP'',
                        ''DADRA AND NAGAR HAVELI AND DAMAN AND DIU'',
                        ''LADAKH'', ''JAMMU AND KASHMIR''
                    ) THEN TRUE
                    ELSE FALSE
                END AS IS_UNION_TERRITORY,

                -- Capital city flag
                CASE
                    WHEN UPPER(TRIM(CITY)) IN (
                        ''MUMBAI'', ''DELHI'', ''BANGALORE'', ''HYDERABAD'',
                        ''CHENNAI'', ''KOLKATA'', ''PUNE'', ''AHMEDABAD''
                    ) THEN TRUE
                    ELSE FALSE
                END AS CAPITAL_CITY_FLAG,

                -- City tier classification
                CASE
                    WHEN UPPER(TRIM(CITY)) IN (
                        ''MUMBAI'', ''DELHI'', ''BANGALORE'', ''HYDERABAD'',
                        ''CHENNAI'', ''KOLKATA'', ''PUNE'', ''AHMEDABAD''
                    ) THEN ''TIER1''
                    WHEN UPPER(TRIM(CITY)) IN (
                        ''NAGPUR'', ''JAIPUR'', ''LUCKNOW'', ''CHANDIGARH'',
                        ''INDORE'', ''COIMBATORE'', ''KOCHI'', ''VISAKHAPATNAM'',
                        ''SURAT'', ''VADODARA'', ''BHOPAL'', ''PATNA'',
                        ''LUDHIANA'', ''AGRA'', ''NASHIK'', ''FARIDABAD''
                    ) THEN ''TIER2''
                    ELSE ''TIER3''
                END AS CITY_TIER,

                ''ACTIVE'' AS STATUS,
                BATCH_ID,
                CURRENT_TIMESTAMP() AS CREATED_AT

            FROM ' || v_stage_table || '
            WHERE IS_VALID = TRUE
            AND BATCH_ID = ''' || P_BATCH_ID || '''
        ) AS SRC
        ON TGT.CITY = SRC.CITY
           AND TGT.STATE = SRC.STATE
           AND TGT.ZIP_CODE = SRC.ZIP_CODE

        -- Update only if data actually changed
        WHEN MATCHED AND (
            COALESCE(TGT.STATE_CODE, '''') != COALESCE(SRC.STATE_CODE, '''')
            OR COALESCE(TGT.IS_UNION_TERRITORY, FALSE) != COALESCE(SRC.IS_UNION_TERRITORY, FALSE)
            OR COALESCE(TGT.CAPITAL_CITY_FLAG, FALSE) != COALESCE(SRC.CAPITAL_CITY_FLAG, FALSE)
            OR COALESCE(TGT.CITY_TIER, '''') != COALESCE(SRC.CITY_TIER, '''')
        ) THEN UPDATE SET
            TGT.STATE_CODE = SRC.STATE_CODE,
            TGT.IS_UNION_TERRITORY = SRC.IS_UNION_TERRITORY,
            TGT.CAPITAL_CITY_FLAG = SRC.CAPITAL_CITY_FLAG,
            TGT.CITY_TIER = SRC.CITY_TIER,
            TGT.BATCH_ID = SRC.BATCH_ID,
            TGT.UPDATED_AT = CURRENT_TIMESTAMP()

        -- Insert new records
        WHEN NOT MATCHED THEN INSERT (
            CITY,
            STATE,
            STATE_CODE,
            IS_UNION_TERRITORY,
            CAPITAL_CITY_FLAG,
            CITY_TIER,
            ZIP_CODE,
            STATUS,
            BATCH_ID,
            CREATED_AT,
            UPDATED_AT
        ) VALUES (
            SRC.CITY,
            SRC.STATE,
            SRC.STATE_CODE,
            SRC.IS_UNION_TERRITORY,
            SRC.CAPITAL_CITY_FLAG,
            SRC.CITY_TIER,
            SRC.ZIP_CODE,
            SRC.STATUS,
            SRC.BATCH_ID,
            SRC.CREATED_AT,
            CURRENT_TIMESTAMP()
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
-- ----------------------------------------------------------------------------------------------------
-- SILVER TO GOLD
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE GOLD.SP_LOCATION_SILVER_TO_GOLD(
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
    FROM GOLD.DIM_LOCATION
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
    FROM SILVER.LOCATION_SLV
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
    CREATE OR REPLACE TEMPORARY TABLE TEMP_LOCATION_STAGING AS
    SELECT
        SRC.LOCATION_SLV_ID,
        SRC.CITY,
        SRC.STATE,
        SRC.STATE_CODE,
        SRC.IS_UNION_TERRITORY,
        SRC.CAPITAL_CITY_FLAG,
        SRC.CITY_TIER,
        SRC.ZIP_CODE,
        SRC.BATCH_ID,

        -- Get existing Gold record ID
        TGT.LOCATION_ID AS EXISTING_LOCATION_ID,

        -- Calculate hashes once for performance
        SHA2_HEX(
            CONCAT_WS('|',
                SRC.CITY,
                SRC.STATE,
                SRC.ZIP_CODE,
                COALESCE(SRC.STATE_CODE, ''),
                COALESCE(SRC.IS_UNION_TERRITORY::STRING, 'FALSE'),
                COALESCE(SRC.CAPITAL_CITY_FLAG::STRING, 'FALSE'),
                COALESCE(SRC.CITY_TIER, '')
            )
        ) AS CURRENT_HASH,

        SHA2_HEX(
            CONCAT_WS('|',
                SRC.CITY,
                SRC.STATE,
                SRC.ZIP_CODE,
                COALESCE(TGT.STATE_CODE, ''),
                COALESCE(TGT.IS_UNION_TERRITORY::STRING, 'FALSE'),
                COALESCE(TGT.CAPITAL_CITY_FLAG::STRING, 'FALSE'),
                COALESCE(TGT.CITY_TIER, '')
            )
        ) AS EXISTING_HASH,

        -- Determine action
        CASE
            WHEN TGT.LOCATION_ID IS NULL THEN 'INSERT'
            WHEN SHA2_HEX(
                CONCAT_WS('|',
                    SRC.CITY,
                    SRC.STATE,
                    SRC.ZIP_CODE,
                    COALESCE(SRC.STATE_CODE, ''),
                    COALESCE(SRC.IS_UNION_TERRITORY::STRING, 'FALSE'),
                    COALESCE(SRC.CAPITAL_CITY_FLAG::STRING, 'FALSE'),
                    COALESCE(SRC.CITY_TIER, '')
                )
            ) != SHA2_HEX(
                CONCAT_WS('|',
                    SRC.CITY,
                    SRC.STATE,
                    SRC.ZIP_CODE,
                    COALESCE(TGT.STATE_CODE, ''),
                    COALESCE(TGT.IS_UNION_TERRITORY::STRING, 'FALSE'),
                    COALESCE(TGT.CAPITAL_CITY_FLAG::STRING, 'FALSE'),
                    COALESCE(TGT.CITY_TIER, '')
                )
            ) THEN 'UPDATE'
            ELSE 'UNCHANGED'
        END AS SCD_ACTION

    FROM SILVER.LOCATION_SLV AS SRC
    LEFT JOIN GOLD.DIM_LOCATION  AS TGT
        ON TGT.CITY = SRC.CITY
        AND TGT.STATE = SRC.STATE
        AND TGT.ZIP_CODE = SRC.ZIP_CODE
        AND TGT.STATUS = 'ACTIVE'
    WHERE SRC.BATCH_ID = :P_BATCH_ID;

    -- ================================================================
    -- STEP 3: EXPIRE CHANGED RECORDS (SCD2 - CLOSE OLD VERSIONS)
    -- ================================================================

    UPDATE GOLD.DIM_LOCATION
    SET
        STATUS = 'INACTIVE',
        EFF_END_DT = :V_CURRENT_TIMESTAMP,
        UPDATED_AT = :V_CURRENT_TIMESTAMP
    WHERE LOCATION_ID IN (
        SELECT LOCATION_SLV_ID
        FROM TEMP_LOCATION_STAGING
        WHERE SCD_ACTION = 'UPDATE'
    )
    AND STATUS = 'ACTIVE';

    V_ROWS_UPDATED := SQLROWCOUNT;

    -- ================================================================
    -- STEP 4: INSERT NEW AND CHANGED RECORDS
    -- ================================================================

    INSERT INTO GOLD.DIM_LOCATION (
        LOCATION_ID,
        CITY,
        STATE,
        STATE_CODE,
        IS_UNION_TERRITORY,
        CAPITAL_CITY_FLAG,
        CITY_TIER,
        ZIP_CODE,
        STATUS,
        EFF_START_DT,
        EFF_END_DT,
        BATCH_ID,
        CREATED_AT,
        UPDATED_AT
    )
    SELECT
        LOCATION_SLV_ID AS LOCATION_ID,
        CITY,
        STATE,
        STATE_CODE,
        IS_UNION_TERRITORY,
        CAPITAL_CITY_FLAG,
        CITY_TIER,
        ZIP_CODE,
        'ACTIVE' AS STATUS,
        CURRENT_TIMESTAMP() AS EFF_START_DT,
        '9999-12-31 23:59:59'::TIMESTAMP_TZ AS EFF_END_DT,
        BATCH_ID,
        CURRENT_TIMESTAMP() AS CREATED_AT,
        CURRENT_TIMESTAMP() AS UPDATED_AT
    FROM TEMP_LOCATION_STAGING
    WHERE SCD_ACTION IN ('INSERT', 'UPDATE');

    V_ROWS_INSERTED := SQLROWCOUNT;

    -- Get unchanged count
    SELECT COUNT(*)
    INTO :V_ROWS_UNCHANGED
    FROM TEMP_LOCATION_STAGING
    WHERE SCD_ACTION = 'UNCHANGED';

    -- ================================================================
    -- STEP 5: CLEANUP & RETURN
    -- ================================================================

    DROP TABLE IF EXISTS TEMP_LOCATION_STAGING;

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
        DROP TABLE IF EXISTS TEMP_LOCATION_STAGING;

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