-- ====================================================================================================
-- CUSTOMER_ADDRESS
-- ====================================================================================================
-- CHANGE CONSTRING(100)
USE DATABASE SWIGGY;
USE SCHEMA BRONZE;
USE WAREHOUSE ADHOC_WH;
-- ----------------------------------------------------------------------------------------------------
-- CREATE CUSTOMER_ADDRESS
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.CUSTOMER_ADDRESS_BRZ (
    CUSTOMER_ADDRESS_BRZ_ID STRING(100),
    CUSTOMER_ID INTEGER,
    FLAT_NO INTEGER,
    HOUSE_NO INTEGER,
    FLOOR_NO INTEGER,
    BUILDING VARCHAR(100),
    LANDMARK VARCHAR(100),
    LOCALITY VARCHAR(100),
    CITY VARCHAR(100),
    STATE VARCHAR(100),
    ZIPCODE INTEGER,
    COORDINATES VARCHAR(100),
    PRIMARYFLAG VARCHAR(100),
    ADDRESSTYPE VARCHAR(100),

    -- RAW_COLUMNS
    CUSTOMER_ID_RAW VARCHAR,
    FLAT_NO_RAW VARCHAR,
    HOUSE_NO_RAW VARCHAR,
    FLOOR_NO_RAW VARCHAR,
    BUILDING_RAW VARCHAR,
    LANDMARK_RAW VARCHAR,
    LOCALITY_RAW VARCHAR,
    CITY_RAW VARCHAR,
    STATE_RAW VARCHAR,
    ZIPCODE_RAW VARCHAR,
    COORDINATES_RAW VARCHAR,
    PRIMARYFLAG_RAW VARCHAR,
    ADDRESSTYPE_RAW VARCHAR,

    -- AUDIT COLUMNS
    INGEST_RUN_ID INTEGER,
    CREATED_AT TIMESTAMP_TZ,
    UPDATED_AT TIMESTAMP_TZ
    )
COMMENT = 'THIS IS THE CUSTOMER ADDRESS STAGE/RAW TABLE WHERE DATA WILL BE COPIED FROM INTERNAL STAGE USING COPY COMMAND. THIS IS AS-IS DATA REPRESETATION FROM THE SOURCE LOCATION. ALL THE COLUMNS ARE STRING(100) DATA TYPE EXCEPT THE AUDIT COLUMNS THAT ARE ADDED FOR TRACEABILITY.';
ALTER TABLE BRONZE.CUSTOMER_ADDRESS_BRZ CLUSTER BY (STATE);

-- CREATING SEQUNCE TO GENERATE INGEST_RUN_ID
CREATE OR REPLACE SEQUENCE SEQ_CUSTOMER_ADDRESS_INGEST_RUN_ID START = 1 INCREMENT = 1;

-- ----------------------------------------------------------------------------------------------------
-- CREATE CUSTOMER_ADDRESS_LOAD_ERROR
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.CUSTOMER_ADDRESS_LOAD_ERROR(
    ERROR_ID INTEGER PRIMARY KEY,
    VALIDATE_COLUMN VARCHAR(50),
    VALIDATION_TYPE VARCHAR(30),
    VALIDATION_ERROR_MSG VARCHAR(200),
    INGEST_RUN_ID INTEGER
);

-- ----------------------------------------------------------------------------------------------------
-- CREATE CUSTOMER_ADDRESS_SLV
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.CUSTOMER_ADDRESS_SLV (
    CUSTOMER_ADDRESS_SLV_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    CUSTOMER_ID INTEGER,
    FLAT_NO INTEGER,
    HOUSE_NO INTEGER,
    FLOOR_NO INTEGER,
    BUILDING VARCHAR,
    LANDMARK VARCHAR,
    LOCALITY VARCHAR,
    CITY VARCHAR,
    STATE VARCHAR,
    ZIPCODE INTEGER,
    COORDINATES VARCHAR,
    PRIMARY_FLAG VARCHAR,
    ADDRESS_TYPE VARCHAR,

    -- AUDIT COLUMNS
    BATCH_ID VARCHAR(36),
    CREATED_AT TIMESTAMP_TZ,
    UPDATED_AT TIMESTAMP_TZ

)
COMMENT = 'CUSTOMER ADDRESS ENTITY UNDER CLEAN SCHEMA WITH APPROPRIATE DATA TYPE UNDER CLEAN SCHEMA LAYER, DATA IS POPULATED USING MERGE STATEMENT FROM THE STAGE LAYER LOCATION TABLE. THIS TABLE DOES NOT SUPPORT SCD2';

-- ----------------------------------------------------------------------------------------------------
-- CREATE DIM_CUSTOMER_ADDRESS
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.DIM_CUSTOMER_ADDRESS (
    CUSTOMER_ADDRESS_ID INTEGER PRIMARY KEY,
    CUSTOMER_ID INTEGER,
    FLAT_NO INTEGER,
    HOUSE_NO INTEGER,
    FLOOR_NO INTEGER,
    BUILDING VARCHAR,
    LANDMARK VARCHAR,
    LOCALITY VARCHAR,
    CITY VARCHAR,
    STATE VARCHAR,
    ZIPCODE INTEGER,
    COORDINATES VARCHAR,
    PRIMARY_FLAG VARCHAR,
    ADDRESS_TYPE VARCHAR,
    -- SCD2 COLUMNS
    BATCH_ID STRING(36),
    EFF_START_DATE TIMESTAMP_TZ,
    EFF_END_DATE TIMESTAMP_TZ,
    STATUS VARCHAR,
    CREATED_AT TIMESTAMP_TZ,
    UPDATED_AT TIMESTAMP_TZ
);

-- =====================================================
-- PROCEDURE 1: STAGE TO BRONZE - CUSTOMER_ADDRESS
-- =====================================================
CREATE OR REPLACE PROCEDURE BRONZE.SP_CUSTOMER_ADDRESS_STAGE_TO_BRONZE(P_PIPELINE_NAME VARCHAR, P_FILE_NAME VARCHAR)
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
    CREATE OR REPLACE TEMPORARY TABLE TEMP_CUSTOMER_ADDRESS_LOAD(
        CUSTOMER_ID STRING,
        FLAT_NO STRING,
        HOUSE_NO STRING,
        FLOOR_NO STRING,
        BUILDING STRING,
        LANDMARK STRING,
        LOCALITY STRING,
        CITY STRING,
        STATE STRING,
        ZIPCODE STRING,
        COORDINATES STRING,
        PRIMARYFLAG STRING,
        ADDRESSTYPE STRING
    );

    -- Copy data from stage
    EXECUTE IMMEDIATE
    '
    COPY INTO TEMP_CUSTOMER_ADDRESS_LOAD(
        CUSTOMER_ID, FLAT_NO, HOUSE_NO, FLOOR_NO, BUILDING, LANDMARK, LOCALITY,
        CITY, STATE, ZIPCODE, COORDINATES, PRIMARYFLAG, ADDRESSTYPE)
    FROM (
        SELECT
            $1::STRING AS CUSTOMER_ID,
            $2::STRING AS FLAT_NO,
            $3::STRING AS HOUSE_NO,
            $4::STRING AS FLOOR_NO,
            $5::STRING AS BUILDING,
            $6::STRING AS LANDMARK,
            $7::STRING AS LOCALITY,
            $8::STRING AS CITY,
            $9::STRING AS STATE,
            $10::STRING AS ZIPCODE,
            $11::STRING AS COORDINATES,
            $12::STRING AS PRIMARYFLAG,
            $13::STRING AS ADDRESSTYPE
        FROM ''' || V_FILE_PATH || '''
    )
    FILE_FORMAT = (FORMAT_NAME = ''' || V_FILE_FORMAT || ''')
    ON_ERROR = ABORT_STATEMENT
    ';

    -- Get row count
    SELECT COUNT(*) INTO :V_ROWS_INSERTED FROM BRONZE.TEMP_CUSTOMER_ADDRESS_LOAD;

    -- Validate data loaded
    IF (V_ROWS_INSERTED = 0) THEN
        ROLLBACK;
        DROP TABLE IF EXISTS TEMP_CUSTOMER_ADDRESS_LOAD;
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', 'No records loaded from file',
            'FILE_PATH', P_FILE_NAME,
            'ROWS_INSERTED', 0,
            'INGEST_RUN_ID', 0
        );
    END IF;

    -- Consume sequence
    SELECT SWIGGY.BRONZE.SEQ_CUSTOMER_ADDRESS_INGEST_RUN_ID.NEXTVAL INTO :V_INGEST_RUN_ID;

    -- Insert into bronze table
    INSERT INTO BRONZE.CUSTOMER_ADDRESS_BRZ (
        CUSTOMER_ADDRESS_BRZ_ID,
        CUSTOMER_ID,
        FLAT_NO,
        HOUSE_NO,
        FLOOR_NO,
        BUILDING,
        LANDMARK,
        LOCALITY,
        CITY,
        STATE,
        ZIPCODE,
        COORDINATES,
        PRIMARYFLAG,
        ADDRESSTYPE,
        CUSTOMER_ID_RAW,
        FLAT_NO_RAW,
        HOUSE_NO_RAW,
        FLOOR_NO_RAW,
        BUILDING_RAW,
        LANDMARK_RAW,
        LOCALITY_RAW,
        CITY_RAW,
        STATE_RAW,
        ZIPCODE_RAW,
        COORDINATES_RAW,
        PRIMARYFLAG_RAW,
        ADDRESSTYPE_RAW,
        INGEST_RUN_ID, CREATED_AT, UPDATED_AT
    )
    SELECT
        UUID_STRING() AS CUSTOMER_ADDRESS_BRZ_ID,
        TRY_TO_NUMBER(CUSTOMER_ID),
        TRY_TO_NUMBER(FLAT_NO),
        TRY_TO_NUMBER(HOUSE_NO),
        TRY_TO_NUMBER(FLOOR_NO),
        TO_VARCHAR(BUILDING),
        TO_VARCHAR(LANDMARK),
        TO_VARCHAR(LOCALITY),
        TO_VARCHAR(CITY),
        TO_VARCHAR(STATE),
        TRY_TO_NUMBER(ZIPCODE),
        TO_VARCHAR(COORDINATES),
        TO_VARCHAR(PRIMARYFLAG),
        TO_VARCHAR(ADDRESSTYPE),
        CUSTOMER_ID,
        FLAT_NO,
        HOUSE_NO,
        FLOOR_NO,
        BUILDING,
        LANDMARK,
        LOCALITY,
        CITY,
        STATE,
        ZIPCODE,
        COORDINATES,
        PRIMARYFLAG,
        ADDRESSTYPE,
        :V_INGEST_RUN_ID,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    FROM TEMP_CUSTOMER_ADDRESS_LOAD;

    -- Commit transaction
    COMMIT;

    -- Cleanup
    DROP TABLE IF EXISTS BRONZE.TEMP_CUSTOMER_ADDRESS_LOAD;

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
        DROP TABLE IF EXISTS TEMP_CUSTOMER_ADDRESS_LOAD;

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
-- PROCEDURE 2: BRONZE TO SILVER - CUSTOMER_ADDRESS
-- =====================================================
CREATE OR REPLACE PROCEDURE SILVER.SP_CUSTOMER_ADDRESS_BRONZE_TO_SILVER(
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
                CUSTOMER_ADDRESS_BRZ_ID,
                CUSTOMER_ID,
                FLAT_NO,
                HOUSE_NO,
                FLOOR_NO,
                BUILDING,
                LANDMARK,
                LOCALITY,
                CITY,
                STATE,
                ZIPCODE,
                COORDINATES,
                PRIMARYFLAG AS PRIMARY_FLAG,
                ADDRESSTYPE AS ADDRESS_TYPE,
                BATCH_ID,
                CURRENT_TIMESTAMP() AS CREATED_AT,
                CURRENT_TIMESTAMP() AS UPDATED_AT
            FROM ' || v_stage_table || '
            WHERE IS_VALID = TRUE AND BATCH_ID = ''' || P_BATCH_ID || '''
        ) AS SRC
        ON TGT.CUSTOMER_ID = SRC.CUSTOMER_ID
        WHEN MATCHED THEN
            UPDATE SET
                TGT.FLAT_NO = SRC.FLAT_NO,
                TGT.HOUSE_NO = SRC.HOUSE_NO,
                TGT.FLOOR_NO = SRC.FLOOR_NO,
                TGT.BUILDING = SRC.BUILDING,
                TGT.LANDMARK = SRC.LANDMARK,
                TGT.LOCALITY = SRC.LOCALITY,
                TGT.CITY = SRC.CITY,
                TGT.STATE = SRC.STATE,
                TGT.ZIPCODE = SRC.ZIPCODE,
                TGT.COORDINATES = SRC.COORDINATES,
                TGT.PRIMARY_FLAG = SRC.PRIMARY_FLAG,
                TGT.ADDRESS_TYPE = SRC.ADDRESS_TYPE,
                TGT.BATCH_ID = SRC.BATCH_ID,
                TGT.UPDATED_AT = SRC.UPDATED_AT
        WHEN NOT MATCHED THEN
            INSERT (
                CUSTOMER_ID, FLAT_NO, HOUSE_NO, FLOOR_NO, BUILDING, LANDMARK, LOCALITY,
                CITY, STATE, ZIPCODE, COORDINATES, PRIMARY_FLAG, ADDRESS_TYPE,
                BATCH_ID, CREATED_AT, UPDATED_AT
            )
            VALUES (
                SRC.CUSTOMER_ID, SRC.FLAT_NO, SRC.HOUSE_NO, SRC.FLOOR_NO, SRC.BUILDING,
                SRC.LANDMARK, SRC.LOCALITY, SRC.CITY, SRC.STATE, SRC.ZIPCODE,
                SRC.COORDINATES, SRC.PRIMARY_FLAG, SRC.ADDRESS_TYPE,
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
-- PROCEDURE 3: SILVER TO GOLD (WITH SCD2) - CUSTOMER_ADDRESS
-- =====================================================
CREATE OR REPLACE PROCEDURE GOLD.SP_CUSTOMER_ADDRESS_SILVER_TO_GOLD(
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
    FROM GOLD.DIM_CUSTOMER_ADDRESS
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
    FROM SILVER.CUSTOMER_ADDRESS_SLV
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

    CREATE OR REPLACE TEMPORARY TABLE TEMP_CUSTOMER_ADDRESS_STAGING AS
    SELECT
        SRC.CUSTOMER_ADDRESS_SLV_ID,
        SRC.CUSTOMER_ID,
        SRC.FLAT_NO,
        SRC.HOUSE_NO,
        SRC.FLOOR_NO,
        SRC.BUILDING,
        SRC.LANDMARK,
        SRC.LOCALITY,
        SRC.CITY,
        SRC.STATE,
        SRC.ZIPCODE,
        SRC.COORDINATES,
        SRC.PRIMARY_FLAG,
        SRC.ADDRESS_TYPE,
        SRC.BATCH_ID,

        -- Calculate hash of attributes for change detection
        SHA2_HEX(
            CONCAT_WS('|',
                COALESCE(TO_VARCHAR(SRC.CUSTOMER_ID), ''),
                COALESCE(TO_VARCHAR(SRC.FLAT_NO), ''),
                COALESCE(TO_VARCHAR(SRC.HOUSE_NO), ''),
                COALESCE(TO_VARCHAR(SRC.FLOOR_NO), ''),
                COALESCE(SRC.BUILDING, ''),
                COALESCE(SRC.LANDMARK, ''),
                COALESCE(SRC.LOCALITY, ''),
                COALESCE(SRC.CITY, ''),
                COALESCE(SRC.STATE, ''),
                COALESCE(TO_VARCHAR(SRC.ZIPCODE), ''),
                COALESCE(SRC.COORDINATES, ''),
                COALESCE(SRC.PRIMARY_FLAG, ''),
                COALESCE(SRC.ADDRESS_TYPE, '')
            )
        ) AS CURRENT_HASH,

        -- Get existing record info
        TGT.CUSTOMER_ADDRESS_ID AS EXISTING_CUSTOMER_ADDRESS_ID,
        SHA2_HEX(
            CONCAT_WS('|',
                COALESCE(TGT.CUSTOMER_ID, ''),
                COALESCE(TGT.FLAT_NO, ''),
                COALESCE(TGT.HOUSE_NO, ''),
                COALESCE(TGT.FLOOR_NO, ''),
                COALESCE(TGT.BUILDING, ''),
                COALESCE(TGT.LANDMARK, ''),
                COALESCE(TGT.LOCALITY, ''),
                COALESCE(TGT.CITY, ''),
                COALESCE(TGT.STATE, ''),
                COALESCE(TGT.ZIPCODE, ''),
                COALESCE(TGT.COORDINATES, ''),
                COALESCE(TGT.PRIMARY_FLAG, ''),
                COALESCE(TGT.ADDRESS_TYPE, '')
            )
        ) AS EXISTING_HASH,

        -- Determine action
        CASE
            WHEN TGT.CUSTOMER_ADDRESS_ID IS NULL THEN 'INSERT'
            WHEN SHA2_HEX(
                CONCAT_WS('|',
                    COALESCE(TO_VARCHAR(SRC.CUSTOMER_ID), ''),
                    COALESCE(TO_VARCHAR(SRC.FLAT_NO), ''),
                    COALESCE(TO_VARCHAR(SRC.HOUSE_NO), ''),
                    COALESCE(TO_VARCHAR(SRC.FLOOR_NO), ''),
                    COALESCE(SRC.BUILDING, ''),
                    COALESCE(SRC.LANDMARK, ''),
                    COALESCE(SRC.LOCALITY, ''),
                    COALESCE(SRC.CITY, ''),
                    COALESCE(SRC.STATE, ''),
                    COALESCE(TO_VARCHAR(SRC.ZIPCODE), ''),
                    COALESCE(SRC.COORDINATES, ''),
                    COALESCE(SRC.PRIMARY_FLAG, ''),
                    COALESCE(SRC.ADDRESS_TYPE, '')
                )
            ) != SHA2_HEX(
                CONCAT_WS('|',
                    COALESCE(TGT.CUSTOMER_ID, ''),
                    COALESCE(TGT.FLAT_NO, ''),
                    COALESCE(TGT.HOUSE_NO, ''),
                    COALESCE(TGT.FLOOR_NO, ''),
                    COALESCE(TGT.BUILDING, ''),
                    COALESCE(TGT.LANDMARK, ''),
                    COALESCE(TGT.LOCALITY, ''),
                    COALESCE(TGT.CITY, ''),
                    COALESCE(TGT.STATE, ''),
                    COALESCE(TGT.ZIPCODE, ''),
                    COALESCE(TGT.COORDINATES, ''),
                    COALESCE(TGT.PRIMARY_FLAG, ''),
                    COALESCE(TGT.ADDRESS_TYPE, '')
                )
            ) THEN 'UPDATE'
            ELSE 'UNCHANGED'
        END AS SCD_ACTION

    FROM SILVER.CUSTOMER_ADDRESS_SLV SRC
    LEFT JOIN GOLD.DIM_CUSTOMER_ADDRESS TGT
        ON TGT.CUSTOMER_ADDRESS_ID = SRC.CUSTOMER_ADDRESS_SLV_ID
        AND TGT.STATUS = TRUE
    WHERE SRC.BATCH_ID = :P_BATCH_ID;

    -- ================================================================
    -- STEP 3: EXPIRE CHANGED RECORDS (SCD2 - CLOSE OLD VERSIONS)
    -- ================================================================

    UPDATE GOLD.DIM_CUSTOMER_ADDRESS
    SET
        STATUS = FALSE,
        EFF_END_DATE = :V_CURRENT_TIMESTAMP,
        UPDATED_AT = :V_CURRENT_TIMESTAMP
    WHERE CUSTOMER_ADDRESS_ID IN (
        SELECT CUSTOMER_ADDRESS_SLV_ID
        FROM TEMP_CUSTOMER_ADDRESS_STAGING
        WHERE SCD_ACTION = 'UPDATE'
    )
    AND STATUS = TRUE;

    V_ROWS_UPDATED := SQLROWCOUNT;

    -- ================================================================
    -- STEP 4: INSERT NEW AND CHANGED RECORDS
    -- ================================================================

    INSERT INTO GOLD.DIM_CUSTOMER_ADDRESS (
        CUSTOMER_ADDRESS_ID,
        CUSTOMER_ID,
        FLAT_NO,
        HOUSE_NO,
        FLOOR_NO,
        BUILDING,
        LANDMARK,
        LOCALITY,
        CITY,
        STATE,
        ZIPCODE,
        COORDINATES,
        PRIMARY_FLAG,
        ADDRESS_TYPE,
        BATCH_ID,
        EFF_START_DATE,
        EFF_END_DATE,
        STATUS,
        CREATED_AT,
        UPDATED_AT
    )
    SELECT
        CUSTOMER_ADDRESS_SLV_ID AS CUSTOMER_ADDRESS_ID,
        TO_VARCHAR(CUSTOMER_ID) AS CUSTOMER_ID,
        TO_VARCHAR(FLAT_NO) AS FLAT_NO,
        TO_VARCHAR(HOUSE_NO) AS HOUSE_NO,
        TO_VARCHAR(FLOOR_NO) AS FLOOR_NO,
        TO_VARCHAR(BUILDING) AS BUILDING,
        TO_VARCHAR(LANDMARK) AS LANDMARK,
        TO_VARCHAR(LOCALITY) AS LOCALITY,
        TO_VARCHAR(CITY) AS CITY,
        TO_VARCHAR(STATE) AS STATE,
        TO_VARCHAR(ZIPCODE) AS ZIPCODE,
        TO_VARCHAR(COORDINATES) AS COORDINATES,
        TO_VARCHAR(PRIMARY_FLAG) AS PRIMARY_FLAG,
        TO_VARCHAR(ADDRESS_TYPE) AS ADDRESS_TYPE,
        BATCH_ID,
        :V_CURRENT_TIMESTAMP AS EFF_START_DATE,
        CAST('9999-12-31 23:59:59' AS TIMESTAMP_TZ) AS EFF_END_DATE,
        TRUE AS STATUS,
        :V_CURRENT_TIMESTAMP AS CREATED_AT,
        :V_CURRENT_TIMESTAMP AS UPDATED_AT
    FROM TEMP_CUSTOMER_ADDRESS_STAGING
    WHERE SCD_ACTION IN ('INSERT', 'UPDATE');

    V_ROWS_INSERTED := SQLROWCOUNT;

    -- Get unchanged count
    SELECT COUNT(*)
    INTO :V_ROWS_UNCHANGED
    FROM TEMP_CUSTOMER_ADDRESS_STAGING
    WHERE SCD_ACTION = 'UNCHANGED';

    -- ================================================================
    -- STEP 5: CLEANUP & RETURN
    -- ================================================================

    DROP TABLE IF EXISTS TEMP_CUSTOMER_ADDRESS_STAGING;

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
        DROP TABLE IF EXISTS TEMP_CUSTOMER_ADDRESS_STAGING;

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