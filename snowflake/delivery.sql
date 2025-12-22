-- ====================================================================================================
-- DELIVERY
-- ====================================================================================================
-- CHANGE CONTEXT
USE DATABASE SWIGGY;
USE SCHEMA BRONZE;
USE WAREHOUSE ADHOC_WH;

-- ----------------------------------------------------------------------------------------------------
-- CREATE DELIVERY_BRZ
-- ----------------------------------------------------------------------------------------------------
-- BRONZE LAYER
CREATE OR REPLACE TABLE BRONZE.DELIVERY_BRZ (
    DELIVERY_ID TEXT,
    ORDER_ID TEXT COMMENT 'ORDER FK (SOURCE SYSTEM)',
    DELIVERY_AGENT_ID TEXT COMMENT 'DELIVERY AGENT FK(SOURCE SYSTEM)',
    DELIVERY_STATUS TEXT,
    ESTIMATED_TIME TEXT,
    ADDRESS_ID TEXT COMMENT 'CUSTOMER ADDRESS FK(SOURCE SYSTEM)',
    DELIVERY_DATE TEXT,
    CREATED_AT TEXT,
    UPDATED_AT TEXT,
    BATCH_ID STRING(36)
);

-- ----------------------------------------------------------------------------------------------------
-- CREATE DELIVERY_SLV
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.DELIVERY_SLV (
    DELIVERY_SLV_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    DELIVERY_ID INTEGER,
    ORDER_ID INTEGER COMMENT 'ORDER FK (SOURCE SYSTEM)',
    DELIVERY_AGENT_ID INTEGER COMMENT 'DELIVERY AGENT FK(SOURCE SYSTEM)',
    DELIVERY_STATUS STRING(50),
    ESTIMATED_TIME TIMESTAMP_TZ,
    CUSTOMER_ADDRESS_ID INTEGER COMMENT 'CUSTOMER ADDRESS FK(SOURCE SYSTEM)',
    DELIVERY_DATE TIMESTAMP_TZ,
    CREATED_AT TIMESTAMP_TZ,
    UPDATED_AT TIMESTAMP_TZ,
    BATCH_ID STRING(36)
);

-- ----------------------------------------------------------------------------------------------------
-- CREATE FACT_DELIVERY
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.FACT_DELIVERY (
    DELIVERY_KEY INTEGER PRIMARY KEY AUTOINCREMENT COMMENT 'SURROGATE KEY (EDW)',
    DELIVERY_ID INTEGER UNIQUE COMMENT 'BUSINESS KEY (SOURCE SYSTEM)',
    ORDER_ID INTEGER COMMENT 'ORDER FK (SOURCE SYSTEM)',
    DELIVERY_AGENT_ID INTEGER COMMENT 'DELIVERY AGENT FK(SOURCE SYSTEM)',
    CUSTOMER_ADDRESS_ID INTEGER COMMENT 'CUSTOMER ADDRESS FK(SOURCE SYSTEM)',
    CURRENT_STATUS STRING(50),
    INITIAL_STATUS STRING(50),
    ESTIMATED_TIME TIMESTAMP_TZ,
    DELIVERY_DATE TIMESTAMP_TZ,
    STATUS_UPDATED_AT TIMESTAMP_TZ,
    BATCH_ID STRING(36),
    CREATED_AT TIMESTAMP_TZ,
    LOADED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'FACT TABLE FOR DELIVERIES WITH STATUS TRACKING';

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
CREATE OR REPLACE PROCEDURE BRONZE.SP_DELIVERY_STAGE_TO_BRONZE(
    P_BATCH_ID STRING,
    P_FILE_PATH STRING
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER DEFAULT 0;
BEGIN
    CREATE OR REPLACE TEMPORARY TABLE TEMP_DELIVERY_LOAD(
        DELIVERY_ID TEXT,
        ORDER_ID TEXT,
        DELIVERY_AGENT_ID TEXT,
        DELIVERY_STATUS TEXT,
        ESTIMATED_TIME TEXT,
        ADDRESS_ID TEXT,
        DELIVERY_DATE TEXT
    );

    EXECUTE IMMEDIATE
    '
        COPY INTO TEMP_DELIVERY_LOAD (
            DELIVERY_ID, ORDER_ID, DELIVERY_AGENT_ID, DELIVERY_STATUS,
            ESTIMATED_TIME, ADDRESS_ID, DELIVERY_DATE
        )
        FROM (
            SELECT
                $1::STRING AS DELIVERY_ID,
                $2::STRING AS ORDER_ID,
                $3::STRING AS DELIVERY_AGENT_ID,
                $4::STRING AS DELIVERY_STATUS,
                $5::STRING AS ESTIMATED_TIME,
                $6::STRING AS ADDRESS_ID,
                $7::STRING AS DELIVERY_DATE
            FROM ' || P_FILE_PATH || '
        )
        FILE_FORMAT = (FORMAT_NAME = ''BRONZE.FF_CSV_COMMA'')
        ON_ERROR = ABORT_STATEMENT
    ';

    INSERT INTO BRONZE.DELIVERY_BRZ (
        DELIVERY_ID, ORDER_ID, DELIVERY_AGENT_ID, DELIVERY_STATUS,
        ESTIMATED_TIME, ADDRESS_ID, DELIVERY_DATE, CREATED_AT, UPDATED_AT, BATCH_ID
    )
    SELECT
        DELIVERY_ID, ORDER_ID, DELIVERY_AGENT_ID, DELIVERY_STATUS,
        ESTIMATED_TIME, ADDRESS_ID, DELIVERY_DATE,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        :P_BATCH_ID
    FROM TEMP_DELIVERY_LOAD;

    SELECT COUNT(*) INTO :v_rows_inserted FROM TEMP_DELIVERY_LOAD;
    DROP TABLE IF EXISTS TEMP_DELIVERY_LOAD;

    RETURN ARRAY_CONSTRUCT(
            'SUCCESSFUL',
            'ERROR: None',
            v_rows_inserted
        );

EXCEPTION
    WHEN OTHER THEN
        RETURN ARRAY_CONSTRUCT(
            'FAILED',
            'ERROR: '|| SQLERRM,
            v_rows_inserted
        );
END;
$$;

-- ----------------------------------------------------------------------------------------------------
-- PROCEDURE: DELIVERY BRONZE TO SILVER
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SILVER.SP_DELIVERY_BRONZE_TO_SILVER(
    P_BATCH_ID STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER DEFAULT 0;
    v_rows_updated INTEGER DEFAULT 0;
BEGIN
    MERGE INTO SILVER.DELIVERY_SLV AS TGT
    USING (
        SELECT
            TRY_CAST(DELIVERY_ID AS INTEGER) AS DELIVERY_ID,
            TRY_CAST(ORDER_ID AS INTEGER) AS ORDER_ID,
            TRY_CAST(DELIVERY_AGENT_ID AS INTEGER) AS DELIVERY_AGENT_ID,
            DELIVERY_STATUS,
            TRY_CAST(ESTIMATED_TIME AS TIMESTAMP_TZ) AS ESTIMATED_TIME,
            TRY_CAST(ADDRESS_ID AS INTEGER) AS CUSTOMER_ADDRESS_ID,
            TRY_CAST(DELIVERY_DATE AS TIMESTAMP_TZ) AS DELIVERY_DATE,
            TRY_CAST(CREATED_AT AS TIMESTAMP_TZ) AS CREATED_AT,
            TRY_CAST(UPDATED_AT AS TIMESTAMP_TZ) AS UPDATED_AT,
            BATCH_ID
        FROM BRONZE.DELIVERY_BRZ
        WHERE BATCH_ID = :P_BATCH_ID
    ) AS SRC
    ON TGT.DELIVERY_ID = SRC.DELIVERY_ID

    WHEN MATCHED THEN
        UPDATE SET
            TGT.DELIVERY_STATUS = SRC.DELIVERY_STATUS,
            TGT.DELIVERY_DATE = SRC.DELIVERY_DATE,
            TGT.UPDATED_AT = SRC.UPDATED_AT,
            TGT.BATCH_ID = SRC.BATCH_ID

    WHEN NOT MATCHED THEN
        INSERT (
            DELIVERY_ID, ORDER_ID, DELIVERY_AGENT_ID, DELIVERY_STATUS,
            ESTIMATED_TIME, CUSTOMER_ADDRESS_ID, DELIVERY_DATE, CREATED_AT, UPDATED_AT, BATCH_ID
        )
        VALUES (
            SRC.DELIVERY_ID, SRC.ORDER_ID, SRC.DELIVERY_AGENT_ID, SRC.DELIVERY_STATUS,
            SRC.ESTIMATED_TIME, SRC.CUSTOMER_ADDRESS_ID, SRC.DELIVERY_DATE,
            SRC.CREATED_AT, SRC.UPDATED_AT, SRC.BATCH_ID
        );

    v_rows_inserted := (SELECT COUNT(*) FROM SILVER.CUSTOMER_SLV WHERE BATCH_ID = :P_BATCH_ID);

    RETURN ARRAY_CONSTRUCT(
            'SUCCESSFUL',
            'ERROR: None',
            v_rows_inserted,
            v_rows_updated
        );

EXCEPTION
    WHEN OTHER THEN
        RETURN ARRAY_CONSTRUCT(
            'FAILED',
            'ERROR: '|| SQLERRM,
            v_rows_inserted,
            v_rows_updated
        );
END;
$$;

-- ----------------------------------------------------------------------------------------------------
-- PROCEDURE: DELIVERY SILVER TO GOLD (WITH STATUS TRACKING)
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE GOLD.SP_DELIVERY_SILVER_TO_GOLD(
    P_BATCH_ID STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    V_CURRENT_TIMESTAMP TIMESTAMP_TZ;
    V_ROWS_INSERTED INTEGER DEFAULT 0;
    V_ROWS_UPDATED INTEGER DEFAULT 0;
    V_ROWS_DELETED INTEGER DEFAULT 0;
BEGIN
    V_CURRENT_TIMESTAMP := CURRENT_TIMESTAMP();

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

    V_ROWS_INSERTED := SQLROWCOUNT;


    RETURN ARRAY_CONSTRUCT(
            'SUCCESSFUL',
            'ERROR: None',
            v_rows_inserted,
            v_rows_updated,
            v_rows_deleted
        );

EXCEPTION
    WHEN OTHER THEN
        RETURN ARRAY_CONSTRUCT(
            'FAILED',
            'ERROR: '|| SQLERRM,
            v_rows_inserted,
            v_rows_updated,
            v_rows_deleted
        );
END;
$$;
-- ----------------------------------------------------------------------------------------------------