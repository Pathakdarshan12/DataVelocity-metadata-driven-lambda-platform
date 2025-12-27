-- =======================================================================================================================================
-- ============================================== CONFIGURATION & METADATA TABLES ========================================================
-- =======================================================================================================================================
-- FILE_FORMAT_MASTER
-- Purpose: Defines reusable file format specifications
-- Answers: How to parse it?
-- ---------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE COMMON.FILE_FORMAT_MASTER (
    file_format_id      INTEGER PRIMARY KEY,
    format_type         VARCHAR(20),        -- CSV | JSON | PARQUET | XML
    delimiter           VARCHAR(5),         -- , | | | \t | NULL
    quote_char          VARCHAR(1),         -- " | ' | NULL
    escape_char         VARCHAR(1),
    header_present      CHAR(1),            -- Y/N
    compression_type    VARCHAR(10),        -- GZIP | BZIP2 | NONE
    record_separator    VARCHAR(5),         -- \n | \r\n
    encoding            VARCHAR(20),        -- UTF-8 | ISO-8859-1
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- =======================================================================================================================================
-- SOURCE_FILE_CONFIG
-- Purpose: Defines file sources and their ingestion rules
-- Answers: What arrives and where?
-- ---------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE COMMON.SOURCE_FILE_CONFIG (
    source_id           INTEGER PRIMARY KEY,
    source_name         VARCHAR(100) UNIQUE,
    source_system       VARCHAR(50),        -- SAP | SALESFORCE | API
    file_format_id      INTEGER,            -- FK → FILE_FORMAT_MASTER
    landing_path        VARCHAR(500),       -- s3://bucket/raw/orders/, INSERT CDC_STREAM NAME IN CASE OF STREAM EX- BRONZE.STREAM_ORDERS_CHANGES:
    file_pattern        VARCHAR(100),       -- orders_*.csv | orders_{YYYY}{MM}{DD}.csv
    ingestion_type      VARCHAR(20),        -- BATCH | STREAM | INCREMENTAL
    schedule_cron       VARCHAR(50),        -- 0 2 * * * (daily 2 AM)
    active_flag         CHAR(1) DEFAULT 'Y',
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (file_format_id) REFERENCES COMMON.FILE_FORMAT_MASTER(file_format_id)
);
-- ======================================================================================================================================
-- DATA_FIELD_MASTER
-- Purpose: Canonical definition of business fields (logical schema)
-- Answers: How it is structured?
-- ---------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE COMMON.DATA_FIELD_MASTER (
    field_id                INTEGER PRIMARY KEY,
    field_name              VARCHAR(100) UNIQUE,
    data_type               VARCHAR(20),        -- VARCHAR | INTEGER | DATE | TIMESTAMP | NUMBER
    length                  INTEGER,
    precision               INTEGER,
    scale                   INTEGER,
    nullable_flag           CHAR(1) DEFAULT 'Y',
    business_description    VARCHAR(500),
    data_domain             VARCHAR(50),        -- CUSTOMER | ORDER | PRODUCT
    pii_flag                CHAR(1) DEFAULT 'N',
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- =======================================================================================================================================
-- FILE_COLUMN_MAPPING
-- Purpose: Maps physical file columns to logical business fields
-- Answers: How to transform source → target?
-- ---------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE COMMON.FILE_COLUMN_MAPPING (
    mapping_id              INTEGER PRIMARY KEY,
    source_id               INTEGER,
    file_column_name        VARCHAR(100),
    field_id                INTEGER,
    column_position         INTEGER,
    is_primary_key          BOOLEAN DEFAULT FALSE,
    auto_increment_flag     BOOLEAN DEFAULT FALSE,  -- NEW: Tracks if PK is auto-generated
    transformation_rule     VARCHAR(500),
    default_value           VARCHAR(100),           -- NEW: Use UUID_STRING() for non-auto PKs
    validation_rule         VARCHAR(500),
    active_flag             CHAR(1) DEFAULT 'Y',
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (source_id) REFERENCES COMMON.SOURCE_FILE_CONFIG(source_id),
    FOREIGN KEY (field_id) REFERENCES COMMON.DATA_FIELD_MASTER(field_id)
);
-- =======================================================================================================================================
-- TARGET_TABLE_MAPPING
-- Purpose: Defines where parsed data lands
-- Answers: Where it lands?
-- ---------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE COMMON.TARGET_TABLE_MAPPING (
    target_id               INTEGER PRIMARY KEY,
    source_id               INTEGER,            -- FK → SOURCE_FILE_CONFIG
    target_database         VARCHAR(50),
    target_schema           VARCHAR(50),
    target_table            VARCHAR(100),
    load_type               VARCHAR(20),        -- APPEND | MERGE | OVERWRITE | SCD2
    primary_key_fields      VARCHAR(200),       -- Comma-separated: ORDER_ID,ORDER_DATE
    partition_columns       VARCHAR(200),       -- order_date,region
    clustering_columns      VARCHAR(200),
    merge_key_fields        VARCHAR(200),       -- For MERGE operations
    enable_raw_columns      CHAR(1) DEFAULT 'Y', -- Store *_RAW columns
    enable_audit_columns    CHAR(1) DEFAULT 'Y', -- Add INGEST_RUN_ID, CREATED_AT, etc.
    active_flag             CHAR(1) DEFAULT 'Y',
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (source_id) REFERENCES COMMON.SOURCE_FILE_CONFIG(source_id)
);
-- =======================================================================================================================================
-- UPDATED SP_BUILD_COPY_SQL - Excludes Auto-Increment PKs
-- =======================================================================================================================================
CALL COMMON.SP_BUILD_COPY_SQL(103, 'ORDERS', 1);
CREATE OR REPLACE PROCEDURE COMMON.SP_BUILD_COPY_SQL(
    P_SOURCE_ID        NUMBER,
    P_FILE_NAME        STRING,
    P_FILE_FORMAT_ID   NUMBER
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    V_SOURCE_NAME      STRING;
    V_LANDING_PATH     STRING;
    V_INGESTION_TYPE   STRING;
    V_FORMAT_TYPE      STRING;
    V_DELIMITER        STRING;
    V_QUOTE_CHAR       STRING;
    V_HEADER_PRESENT   STRING;
    V_FILE_PATH        STRING;
    V_COPY_SQL         STRING;
    V_TEMP_TABLE_NAME  STRING;
BEGIN
    -- Get metadata
    SELECT
        S.SOURCE_NAME,
        S.LANDING_PATH,
        S.INGESTION_TYPE,
        F.FORMAT_TYPE,
        F.DELIMITER,
        F.QUOTE_CHAR,
        F.HEADER_PRESENT
    INTO
        V_SOURCE_NAME, V_LANDING_PATH, V_INGESTION_TYPE,
        V_FORMAT_TYPE, V_DELIMITER, V_QUOTE_CHAR, V_HEADER_PRESENT
    FROM COMMON.SOURCE_FILE_CONFIG S
    JOIN COMMON.FILE_FORMAT_MASTER F ON F.FILE_FORMAT_ID = :P_FILE_FORMAT_ID
    WHERE S.SOURCE_ID = :P_SOURCE_ID AND S.ACTIVE_FLAG = 'Y';

    V_TEMP_TABLE_NAME := 'TEMP_' || V_SOURCE_NAME;
    V_INGESTION_TYPE := UPPER(V_INGESTION_TYPE);
    V_FORMAT_TYPE := UPPER(V_FORMAT_TYPE);

    IF (V_INGESTION_TYPE = 'BATCH') THEN
        V_FILE_PATH := V_LANDING_PATH || P_FILE_NAME;
    END IF;

    -- =======================================================================
    -- BATCH + CSV
    -- =======================================================================
    IF (V_INGESTION_TYPE = 'BATCH' AND V_FORMAT_TYPE = 'CSV') THEN
        SELECT
            'COPY INTO ' || :V_TEMP_TABLE_NAME || ' (' ||

            -- COLUMNS TO LOAD (Exclude auto-increment PKs)
            LISTAGG(file_column_name, ', ')
            WITHIN GROUP (ORDER BY new_position) ||

            ') FROM (SELECT ' ||

            -- SOURCE COLUMN POSITIONS using renumbered positions
            LISTAGG('$' || new_position || '::STRING AS ' || file_column_name, ', ')
            WITHIN GROUP (ORDER BY new_position) ||

            ' FROM ''' || :V_FILE_PATH || ''') ' ||
            'FILE_FORMAT = (TYPE = CSV ' ||
            'FIELD_DELIMITER = ''' || :V_DELIMITER || ''' ' ||
            IFF(:V_HEADER_PRESENT = 'Y', 'SKIP_HEADER = 1 ', '') ||
            'FIELD_OPTIONALLY_ENCLOSED_BY = ''' || COALESCE(:V_QUOTE_CHAR, '"') || ''') ' ||
            'ON_ERROR = ABORT_STATEMENT'
        INTO V_COPY_SQL
        FROM (
            SELECT
                fcm.file_column_name,
                ROW_NUMBER() OVER (ORDER BY fcm.column_position) AS new_position
            FROM COMMON.FILE_COLUMN_MAPPING fcm
            WHERE fcm.source_id = :P_SOURCE_ID
              AND fcm.active_flag = 'Y'
              AND COALESCE(fcm.auto_increment_flag, FALSE) = FALSE  -- Exclude auto-increment
        );

    -- =======================================================================
    -- BATCH + JSON
    -- =======================================================================
    ELSEIF (V_INGESTION_TYPE = 'BATCH' AND V_FORMAT_TYPE = 'JSON') THEN
        SELECT
            'COPY INTO ' || :V_TEMP_TABLE_NAME || ' (' ||
            LISTAGG(dfm.field_name, ', ')
            WITHIN GROUP (ORDER BY new_position) ||
            ') FROM (SELECT ' ||
            LISTAGG('$1:' || fcm.file_column_name || '::STRING AS ' || dfm.field_name, ', ')
            WITHIN GROUP (ORDER BY new_position) ||
            ' FROM ''' || :V_FILE_PATH || ''') ' ||
            'FILE_FORMAT = (TYPE = JSON) ' ||
            'ON_ERROR = ABORT_STATEMENT'
        INTO V_COPY_SQL
        FROM (
            SELECT
                fcm.file_column_name,
                dfm.field_name,
                ROW_NUMBER() OVER (ORDER BY fcm.column_position) AS new_position
            FROM COMMON.FILE_COLUMN_MAPPING fcm
            JOIN COMMON.DATA_FIELD_MASTER dfm ON fcm.field_id = dfm.field_id
            WHERE fcm.source_id = :P_SOURCE_ID
              AND fcm.active_flag = 'Y'
              AND COALESCE(fcm.auto_increment_flag, FALSE) = FALSE
        );

    -- =======================================================================
    -- STREAM
    -- =======================================================================
    ELSEIF (V_INGESTION_TYPE = 'STREAM') THEN
        SELECT
            'INSERT INTO ' || :V_TEMP_TABLE_NAME || ' (' ||
            LISTAGG(dfm.field_name, ', ')
            WITHIN GROUP (ORDER BY new_position) ||
            ') SELECT ' ||
            LISTAGG('RECORD_CONTENT:' || fcm.file_column_name || '::STRING AS ' || dfm.field_name, ', ')
            WITHIN GROUP (ORDER BY new_position) ||
            ' FROM ' || :V_LANDING_PATH || ' ' ||
            'WHERE METADATA$ACTION = ''INSERT'' ' ||
            'AND METADATA$ISUPDATE = FALSE'
        INTO V_COPY_SQL
        FROM (
            SELECT
                fcm.file_column_name,
                dfm.field_name,
                ROW_NUMBER() OVER (ORDER BY fcm.column_position) AS new_position
            FROM COMMON.FILE_COLUMN_MAPPING fcm
            JOIN COMMON.DATA_FIELD_MASTER dfm ON fcm.field_id = dfm.field_id
            WHERE fcm.source_id = :P_SOURCE_ID
              AND fcm.active_flag = 'Y'
              AND COALESCE(fcm.auto_increment_flag, FALSE) = FALSE
        );

    ELSE
        V_COPY_SQL := 'ERROR: Unsupported ingestion_type (' || V_INGESTION_TYPE ||
                      ') / format_type (' || V_FORMAT_TYPE || ')';
    END IF;

    RETURN V_COPY_SQL;
END;
$$;
-- =======================================================================================================================================
-- UPDATED SP_BUILD_INSERT_SQL - Handles All Three Cases
-- =======================================================================================================================================
CREATE OR REPLACE PROCEDURE COMMON.SP_BUILD_INSERT_SQL(
    P_SOURCE_ID NUMBER,
    P_INGEST_RUN_ID NUMBER
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    V_FULL_TARGET_TABLE     STRING;
    V_SOURCE_NAME           STRING;
    V_ENABLE_RAW_COLUMNS    STRING;
    V_ENABLE_AUDIT_COLUMNS  STRING;
    V_TARGET_COLUMNS        STRING;
    V_TRANSFORM_LIST        STRING;
    V_INSERT_SQL            STRING;
    V_TEMP_TABLE_NAME       STRING;
BEGIN
    -- Get metadata
    SELECT
        target_database || '.' || target_schema || '.' || target_table,
        sfc.source_name,
        ttm.enable_raw_columns,
        ttm.enable_audit_columns
    INTO
        V_FULL_TARGET_TABLE,
        V_SOURCE_NAME,
        V_ENABLE_RAW_COLUMNS,
        V_ENABLE_AUDIT_COLUMNS
    FROM COMMON.TARGET_TABLE_MAPPING ttm
    JOIN COMMON.SOURCE_FILE_CONFIG sfc ON ttm.source_id = sfc.source_id
    WHERE ttm.source_id = :P_SOURCE_ID;

    V_TEMP_TABLE_NAME := 'TEMP_' || V_SOURCE_NAME;

    -- =======================================================================
    -- BUILD TARGET COLUMNS (Exclude auto-increment PKs)
    -- =======================================================================
    SELECT
        -- Regular columns (SKIP auto-increment, INCLUDE non-auto PKs)
        LISTAGG(
            CASE
                WHEN fcm.auto_increment_flag = TRUE THEN NULL  -- SKIP auto-increment
                ELSE dfm.field_name                             -- INCLUDE everything else
            END,
            ', '
        ) WITHIN GROUP (ORDER BY fcm.column_position)

        -- Raw columns (non-PK only)
        || CASE
            WHEN :V_ENABLE_RAW_COLUMNS = 'Y' THEN
                COALESCE(
                    ', ' || NULLIF(
                        LISTAGG(
                            CASE
                                WHEN fcm.is_primary_key = FALSE
                                THEN dfm.field_name || '_RAW'
                            END,
                            ', '
                        ) WITHIN GROUP (ORDER BY fcm.column_position),
                        ''
                    ),
                    ''
                )
            ELSE ''
        END

        -- Audit columns
        || CASE
            WHEN :V_ENABLE_AUDIT_COLUMNS = 'Y' THEN
                ', INGEST_RUN_ID, CREATED_AT, UPDATED_AT'
            ELSE ''
        END

    INTO V_TARGET_COLUMNS
    FROM COMMON.FILE_COLUMN_MAPPING fcm
    JOIN COMMON.DATA_FIELD_MASTER dfm ON fcm.field_id = dfm.field_id
    WHERE fcm.source_id = :P_SOURCE_ID
      AND fcm.active_flag = 'Y';

    -- =======================================================================
    -- BUILD TRANSFORMATION LIST (SELECT expressions)
    -- =======================================================================
    SELECT
        LISTAGG(
            CASE
                -- CASE 1: Auto-increment PK → SKIP
                WHEN fcm.auto_increment_flag = TRUE THEN NULL

                -- CASE 2: Non-auto PK → Apply transformation or default_value
                WHEN fcm.is_primary_key = TRUE AND fcm.auto_increment_flag = FALSE THEN
                    COALESCE(fcm.transformation_rule, fcm.default_value, 'NULL')

                -- CASE 3: Regular column → Apply transformation or use column as-is
                ELSE
                    COALESCE(fcm.transformation_rule, fcm.file_column_name)
            END,
            ', '
        ) WITHIN GROUP (ORDER BY fcm.column_position)

        -- Raw columns
        || CASE
            WHEN :V_ENABLE_RAW_COLUMNS = 'Y' THEN
                COALESCE(
                    ', ' || NULLIF(
                        LISTAGG(
                            CASE
                                WHEN fcm.is_primary_key = FALSE
                                THEN fcm.file_column_name
                            END,
                            ', '
                        ) WITHIN GROUP (ORDER BY fcm.column_position),
                        ''
                    ),
                    ''
                )
            ELSE ''
        END

        -- Audit columns
        || CASE
            WHEN :V_ENABLE_AUDIT_COLUMNS = 'Y' THEN
                ', ' || :P_INGEST_RUN_ID || ', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()'
            ELSE ''
        END

    INTO V_TRANSFORM_LIST
    FROM COMMON.FILE_COLUMN_MAPPING fcm
    JOIN COMMON.DATA_FIELD_MASTER dfm ON fcm.field_id = dfm.field_id
    WHERE fcm.source_id = :P_SOURCE_ID
      AND fcm.active_flag = 'Y';

    -- Build final INSERT
    V_INSERT_SQL :=
        'INSERT INTO ' || V_FULL_TARGET_TABLE ||
        ' (' || V_TARGET_COLUMNS || ') ' ||
        'SELECT ' || V_TRANSFORM_LIST ||
        ' FROM ' || V_TEMP_TABLE_NAME;

    RETURN V_INSERT_SQL;
END;
$$;
-- =====================================================
-- BATCH TABLE - STORES BATCH EXECUTION DATA
-- =====================================================

CREATE OR REPLACE TABLE COMMON.BATCH (
    BATCH_ID VARCHAR PRIMARY KEY,
    PIPELINE_NAME VARCHAR(100),
    LAST_BATCH_EXECUTED_AT TIMESTAMP_NTZ,

    -- Stage Execution Tracking
    STAGE_TO_BRONZE_STATUS VARCHAR(20),
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
CREATE OR REPLACE PROCEDURE BRONZE.SP_METADATA_DRIVEN_LOAD(
    P_SOURCE_ID INTEGER,              -- FK to SOURCE_FILE_CONFIG
    P_FILE_NAME VARCHAR DEFAULT NULL  -- For FILE sources (required for BATCH)
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    -- Runtime variables
    V_ROWS_INSERTED INTEGER DEFAULT 0;
    V_INGEST_RUN_ID INTEGER DEFAULT 0;
    V_ERROR_MESSAGE VARCHAR(5000);
    V_START_TIME TIMESTAMP_TZ(9);
    V_END_TIME TIMESTAMP_TZ(9);
    V_EXECUTION_DURATION INTEGER;

    -- Metadata variables
    V_SOURCE_NAME VARCHAR;
    V_SOURCE_SYSTEM VARCHAR;
    V_FILE_FORMAT_ID INTEGER;
    V_INGESTION_TYPE VARCHAR;
    V_LANDING_PATH VARCHAR;
    V_FORMAT_TYPE VARCHAR;

    -- Target variables
    V_TARGET_DATABASE VARCHAR;
    V_TARGET_SCHEMA VARCHAR;
    V_TARGET_TABLE VARCHAR;
    V_LOAD_TYPE VARCHAR;

    -- Dynamic SQL
    V_TEMP_TABLE_DDL VARCHAR(10000);
    V_COPY_SQL VARCHAR(10000);
    V_INSERT_SQL VARCHAR(10000);
    V_FILE_PATH VARCHAR(1000);
    V_FIELD_LIST VARCHAR(5000);

BEGIN
    V_START_TIME := CURRENT_TIMESTAMP();

    -- Generate new run ID
    SELECT BRONZE.SEQ_ORDER_INGEST_RUN_ID.NEXTVAL INTO :V_INGEST_RUN_ID;

    BEGIN TRANSACTION;

    -- ============================================================
    -- STEP 1: QUERY METADATA - Get Source Configuration
    -- ============================================================
    SELECT
        sfc.source_name,
        sfc.source_system,
        sfc.file_format_id,
        sfc.ingestion_type,
        sfc.landing_path,
        ffm.format_type
    INTO
        V_SOURCE_NAME,
        V_SOURCE_SYSTEM,
        V_FILE_FORMAT_ID,
        V_INGESTION_TYPE,
        V_LANDING_PATH,
        V_FORMAT_TYPE
    FROM COMMON.SOURCE_FILE_CONFIG sfc
    JOIN COMMON.FILE_FORMAT_MASTER ffm
        ON sfc.file_format_id = ffm.file_format_id
    WHERE sfc.source_id = :P_SOURCE_ID
    AND sfc.active_flag = 'Y';

    -- Validate source found
    IF (V_SOURCE_NAME IS NULL) THEN
        ROLLBACK;
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', 'Source ID not found or inactive: ' || P_SOURCE_ID,
            'SOURCE_ID', P_SOURCE_ID
        );
    END IF;

    -- Validate required parameters for BATCH ingestion
    IF (UPPER(V_INGESTION_TYPE) = 'BATCH' AND P_FILE_NAME IS NULL) THEN
        ROLLBACK;
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', 'P_FILE_NAME is required for BATCH ingestion',
            'SOURCE_ID', P_SOURCE_ID,
            'INGESTION_TYPE', V_INGESTION_TYPE
        );
    END IF;

    -- Build file path for BATCH
    IF (UPPER(V_INGESTION_TYPE) = 'BATCH') THEN
        V_FILE_PATH := V_LANDING_PATH || P_FILE_NAME;
    END IF;

    -- ============================================================
    -- STEP 2: Get Target Configuration
    -- ============================================================
    SELECT
        target_database,
        target_schema,
        target_table,
        load_type
    INTO
        V_TARGET_DATABASE,
        V_TARGET_SCHEMA,
        V_TARGET_TABLE,
        V_LOAD_TYPE
    FROM COMMON.TARGET_TABLE_MAPPING
    WHERE source_id = :P_SOURCE_ID
    AND active_flag = 'Y';

    -- ============================================================
    -- STEP 3: Build Dynamic Temp Table DDL
    -- CRITICAL FIX: Exclude auto-increment PKs from temp table
    -- ============================================================
    SELECT
        LISTAGG(
            CASE
                WHEN fcm.auto_increment_flag = TRUE THEN NULL  -- SKIP auto-increment PKs
                ELSE fcm.file_column_name || ' VARCHAR'
            END,
            ', '
        ) WITHIN GROUP (ORDER BY fcm.column_position)
    INTO V_FIELD_LIST
    FROM COMMON.FILE_COLUMN_MAPPING fcm
    WHERE fcm.source_id = :P_SOURCE_ID
    AND fcm.active_flag = 'Y';

    V_TEMP_TABLE_DDL := 'CREATE OR REPLACE TEMPORARY TABLE TEMP_' || V_SOURCE_NAME || ' (' || V_FIELD_LIST || ')';

    EXECUTE IMMEDIATE V_TEMP_TABLE_DDL;

    -- ============================================================
    -- STEP 4: Build and Execute COPY Statement
    -- ============================================================
    CALL COMMON.SP_BUILD_COPY_SQL(:P_SOURCE_ID, :P_FILE_NAME, :V_FILE_FORMAT_ID) INTO V_COPY_SQL;

    -- Log the generated SQL for debugging
    -- EXECUTE IMMEDIATE 'SELECT ''' || V_COPY_SQL || '''';  -- Uncomment for troubleshooting

    EXECUTE IMMEDIATE V_COPY_SQL;

    -- ============================================================
    -- STEP 5: Validate Data Loaded
    -- ============================================================
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM TEMP_' || V_SOURCE_NAME;

    SELECT COUNT(*)
    INTO V_ROWS_INSERTED
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    IF (V_ROWS_INSERTED = 0) THEN
        ROLLBACK;
        EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS TEMP_' || V_SOURCE_NAME;
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', 'No records loaded from source',
            'SOURCE_NAME', V_SOURCE_NAME,
            'FILE_PATH', V_FILE_PATH,
            'COPY_SQL', V_COPY_SQL,  -- Include for debugging
            'ROWS_INSERTED', 0
        );
    END IF;

    -- ============================================================
    -- STEP 6: Build and Execute INSERT to Target Table
    -- ============================================================
    CALL COMMON.SP_BUILD_INSERT_SQL(:P_SOURCE_ID, :V_INGEST_RUN_ID) INTO V_INSERT_SQL;

    -- Log the generated SQL for debugging
    -- EXECUTE IMMEDIATE 'SELECT ''' || V_INSERT_SQL || '''';  -- Uncomment for troubleshooting

    EXECUTE IMMEDIATE V_INSERT_SQL;

    -- ============================================================
    -- STEP 7: Cleanup and Commit
    -- ============================================================
    EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS TEMP_' || V_SOURCE_NAME;

    COMMIT;

    V_END_TIME := CURRENT_TIMESTAMP();
    V_EXECUTION_DURATION := DATEDIFF(SECOND, V_START_TIME, V_END_TIME);

    RETURN OBJECT_CONSTRUCT(
        'STATUS', 'SUCCESS',
        'MESSAGE', 'Data loaded successfully',
        'SOURCE_ID', P_SOURCE_ID,
        'SOURCE_NAME', V_SOURCE_NAME,
        'SOURCE_SYSTEM', V_SOURCE_SYSTEM,
        'INGESTION_TYPE', V_INGESTION_TYPE,
        'TARGET_TABLE', V_TARGET_DATABASE || '.' || V_TARGET_SCHEMA || '.' || V_TARGET_TABLE,
        'FILE_PATH', V_FILE_PATH,
        'ROWS_INSERTED', V_ROWS_INSERTED,
        'INGEST_RUN_ID', V_INGEST_RUN_ID,
        'EXECUTION_TIME_SEC', V_EXECUTION_DURATION,
        'COPY_SQL', V_COPY_SQL,
        'INSERT_SQL', V_INSERT_SQL
    );

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;

        V_ERROR_MESSAGE := SQLERRM;
        V_END_TIME := CURRENT_TIMESTAMP();
        V_EXECUTION_DURATION := DATEDIFF(SECOND, V_START_TIME, V_END_TIME);

        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS TEMP_' || V_SOURCE_NAME;
        EXCEPTION
            WHEN OTHER THEN
                NULL; -- Ignore cleanup errors
        END;

        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', V_ERROR_MESSAGE,
            'SOURCE_ID', P_SOURCE_ID,
            'SOURCE_NAME', V_SOURCE_NAME,
            'FILE_PATH', V_FILE_PATH,
            'ROWS_INSERTED', 0,
            'INGEST_RUN_ID', V_INGEST_RUN_ID,
            'EXECUTION_TIME_SEC', V_EXECUTION_DURATION,
            'COPY_SQL', V_COPY_SQL,
            'INSERT_SQL', V_INSERT_SQL,
            'NOTE', 'Transaction rolled back'
        );
END;
$$;