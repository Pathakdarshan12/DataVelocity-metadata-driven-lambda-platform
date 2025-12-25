SELECT * FROM COMMON.DQ_CONFIG;
-- =====================================================
-- STEP 1: DQ CONFIG TABLE
-- =====================================================
CREATE OR REPLACE TABLE COMMON.DQ_CONFIG (
    DQ_CONFIG_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    VALIDATE_TABLE VARCHAR(200) NOT NULL,
    PRIMARY_KEY_COLUMN STRING NOT NULL,
    ERROR_TABLE VARCHAR(200) NOT NULL,
    VALIDATION_NAME VARCHAR(200) UNIQUE NOT NULL,
    VALIDATION_TYPE VARCHAR(50) NOT NULL,
    VALIDATE_COLUMN VARCHAR(200),
    VALIDATION_QUERY VARCHAR(5000),
    VALIDATION_ERROR_MSG VARCHAR(1000),
    STATUS BOOLEAN DEFAULT TRUE,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(100) DEFAULT CURRENT_USER()
);


-- =====================================================
-- STEP 2: INSERT DQ RULES FOR CUSTOMER
-- =====================================================
-- =====================================================
-- DQ RULES FOR ORDER_ITEM TABLE
-- =====================================================

-- =====================================================
-- DQ RULES FOR ORDER_ITEM TABLE (MATCHING PROCEDURE)
-- =====================================================

INSERT INTO COMMON.DQ_CONFIG (
    VALIDATE_TABLE, 
    PRIMARY_KEY_COLUMN,
    ERROR_TABLE, 
    VALIDATION_NAME, 
    VALIDATION_TYPE, 
    VALIDATE_COLUMN, 
    VALIDATION_QUERY, 
    VALIDATION_ERROR_MSG,
    STATUS
)
VALUES 
-- ============= MANDATORY CHECKS =============

('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
 'DQ_OI_MAN_ORDER_ID', 'MANDATORY_CHECK', 'ORDER_ID', 
 'ORDER_ID IS NOT NULL AND TRIM(ORDER_ID) != ''''', 
 'ORDER_ID is mandatory', TRUE),

('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
 'DQ_OI_MAN_MENU_ID', 'MANDATORY_CHECK', 'MENU_ID', 
 'MENU_ID IS NOT NULL', 
 'MENU_ID is mandatory', TRUE),

('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
 'DQ_OI_MAN_QUANTITY', 'MANDATORY_CHECK', 'QUANTITY', 
 'QUANTITY IS NOT NULL', 
 'QUANTITY is mandatory', TRUE),

('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
 'DQ_OI_MAN_PRICE', 'MANDATORY_CHECK', 'PRICE', 
 'PRICE IS NOT NULL', 
 'PRICE is mandatory', TRUE),

('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
 'DQ_OI_MAN_SUBTOTAL', 'MANDATORY_CHECK', 'SUBTOTAL', 
 'SUBTOTAL IS NOT NULL', 
 'SUBTOTAL is mandatory', TRUE),

('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
 'DQ_OI_MAN_ORDER_TIMESTAMP', 'MANDATORY_CHECK', 'ORDER_TIMESTAMP', 
 'ORDER_TIMESTAMP IS NOT NULL', 
 'ORDER_TIMESTAMP is mandatory', TRUE),

-- ============= VALUE CHECKS (Data Type & Range) =============

-- ('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
--  'DQ_OI_VAL_MENU_ID_TYPE', 'VALUE_CHECK', 'MENU_ID', 
--  'TRY_CAST(MENU_ID AS INTEGER) IS NOT NULL', 
--  'MENU_ID is not a valid integer', TRUE),

-- ('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
--  'DQ_OI_VAL_QUANTITY_TYPE', 'VALUE_CHECK', 'QUANTITY', 
--  'TRY_CAST(QUANTITY AS NUMBER(10,2)) IS NOT NULL', 
--  'QUANTITY is not a valid number', TRUE),

-- ('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
--  'DQ_OI_VAL_PRICE_TYPE', 'VALUE_CHECK', 'PRICE', 
--  'TRY_CAST(PRICE AS NUMBER(10,2)) IS NOT NULL', 
--  'PRICE is not a valid number', TRUE),

-- ('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
--  'DQ_OI_VAL_SUBTOTAL_TYPE', 'VALUE_CHECK', 'SUBTOTAL', 
--  'TRY_CAST(SUBTOTAL AS NUMBER(10,2)) IS NOT NULL', 
--  'SUBTOTAL is not a valid number', TRUE),

-- ('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
--  'DQ_OI_VAL_ORDER_TIMESTAMP_TYPE', 'VALUE_CHECK', 'ORDER_TIMESTAMP', 
--  'TRY_CAST(ORDER_TIMESTAMP AS DATE) IS NOT NULL', 
--  'ORDER_TIMESTAMP is not a valid date', TRUE),

-- ('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
--  'DQ_OI_VAL_QUANTITY_POSITIVE', 'VALUE_CHECK', 'QUANTITY', 
--  'QUANTITY > 0', 
--  'QUANTITY must be greater than 0', TRUE),

-- ('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
--  'DQ_OI_VAL_QUANTITY_MAX', 'VALUE_CHECK', 'QUANTITY', 
--  'QUANTITY <= 1000', 
--  'QUANTITY exceeds maximum allowed value of 1000', TRUE),

-- ('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
--  'DQ_OI_VAL_PRICE_POSITIVE', 'VALUE_CHECK', 'PRICE', 
--  'PRICE > 0', 
--  'PRICE must be greater than 0', TRUE),

-- ('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
--  'DQ_OI_VAL_PRICE_MAX', 'VALUE_CHECK', 'PRICE', 
--  'PRICE <= 100000', 
--  'PRICE exceeds maximum allowed value', TRUE),

-- ('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
--  'DQ_OI_VAL_SUBTOTAL_POSITIVE', 'VALUE_CHECK', 'SUBTOTAL', 
--  'SUBTOTAL > 0', 
--  'SUBTOTAL must be greater than 0', TRUE),

-- ('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
--  'DQ_OI_VAL_MENU_ID_POSITIVE', 'VALUE_CHECK', 'MENU_ID', 
--  'MENU_ID > 0', 
--  'MENU_ID must be greater than 0', TRUE),

-- ('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
--  'DQ_OI_VAL_SUBTOTAL_CALCULATION', 'VALUE_CHECK', 'SUBTOTAL', 
--  'ABS(SUBTOTAL - (QUANTITY * PRICE)) < 0.01', 
--  'SUBTOTAL does not match QUANTITY * PRICE calculation', TRUE),

-- ('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
--  'DQ_OI_VAL_ORDER_TIMESTAMP_PAST', 'VALUE_CHECK', 'ORDER_TIMESTAMP', 
--  'ORDER_TIMESTAMP <= CURRENT_DATE()', 
--  'ORDER_TIMESTAMP cannot be in the future', TRUE),

-- ('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
--  'DQ_OI_VAL_ORDER_TIMESTAMP_VALID_RANGE', 'VALUE_CHECK', 'ORDER_TIMESTAMP', 
--  'ORDER_TIMESTAMP >= ''2020-01-01''', 
--  'ORDER_TIMESTAMP is before valid business start date', TRUE),

-- ('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
--  'DQ_OI_VAL_ORDER_ITEM_ID_LENGTH', 'VALUE_CHECK', 'ORDER_ITEM_ID', 
--  'LENGTH(TRIM(ORDER_ITEM_ID)) BETWEEN 1 AND 200', 
--  'ORDER_ITEM_ID length is invalid', TRUE),

-- ('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
--  'DQ_OI_VAL_ORDER_ID_LENGTH', 'VALUE_CHECK', 'ORDER_ID', 
--  'LENGTH(TRIM(ORDER_ID)) BETWEEN 1 AND 200', 
--  'ORDER_ID length is invalid', TRUE),

-- ('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
--  'DQ_OI_VAL_QUANTITY_PRECISION', 'VALUE_CHECK', 'QUANTITY', 
--  'QUANTITY = ROUND(QUANTITY, 2)', 
--  'QUANTITY has more than 2 decimal places', TRUE),

-- ('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
--  'DQ_OI_VAL_PRICE_PRECISION', 'VALUE_CHECK', 'PRICE', 
--  'PRICE = ROUND(PRICE, 2)', 
--  'PRICE has more than 2 decimal places', TRUE),

-- ('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
--  'DQ_OI_VAL_SUBTOTAL_PRECISION', 'VALUE_CHECK', 'SUBTOTAL', 
--  'SUBTOTAL = ROUND(SUBTOTAL, 2)', 
--  'SUBTOTAL has more than 2 decimal places', TRUE);

-- ============= LOOKUP CHECKS (Referential Integrity) =============
-- Note: Enable these only after related tables are populated
('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
 'DQ_OI_LKP_ORDER_EXISTS', 'LOOKUP_CHECK', 'ORDER_ID', 
 'ORDER_ID IN (SELECT ORDER_ID FROM BRONZE.ORDERS_BRZ WHERE ORDER_ID IS NOT NULL)', 
 'ORDER_ID does not exist in ORDERS table', FALSE),

('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 
 'DQ_OI_LKP_MENU_EXISTS', 'LOOKUP_CHECK', 'MENU_ID', 
 'MENU_ID IN (SELECT MENU_ID FROM BRONZE.MENU_BRZ WHERE MENU_ID IS NOT NULL)', 
 'MENU_ID does not exist in MENU table', FALSE);

 SELECT * FROM COMMON.DQ_CONFIG;
-- =====================================================
-- DATA QUALITY VALIDATION PROCEDURE
-- =====================================================
-- Clear previous errors
TRUNCATE TABLE BRONZE.ORDER_ITEM_LOAD_ERROR;
SELECT * FROM BRONZE.ORDER_ITEM_LOAD_ERROR;
SELECT * FROM BRONZE.STG_ORDER_ITEM_DQ;

-- Recreate staging table
CREATE OR REPLACE TEMPORARY TABLE STG_ORDER_ITEM_DQ AS
SELECT *,
       TRUE AS IS_VALID,
       '123' AS BATCH_ID
FROM BRONZE.ORDER_ITEM_BRZ
WHERE INGEST_RUN_ID = 1;

UPDATE STG_ORDER_ITEM_DQ
SET ORDER_ITEM_ID = '123';


-- Execute validation
CALL BRONZE.SP_EXECUTE_DATA_QUALITY_VALIDATION(
    'STG_ORDER_ITEM_DQ', 
    'BRONZE.ORDER_ITEM_BRZ', 
    'BRONZE.ORDER_ITEM_LOAD_ERROR', 
    1
);

-- Check results
SELECT * FROM BRONZE.ORDER_ITEM_LOAD_ERROR;
SELECT IS_VALID, COUNT(*) FROM STG_ORDER_ITEM_DQ GROUP BY IS_VALID;



CREATE OR REPLACE PROCEDURE BRONZE.SP_EXECUTE_DATA_QUALITY_VALIDATION(
    P_STAGE_TABLE VARCHAR,
    P_VALIDATE_TABLE VARCHAR,
    P_ERROR_TABLE VARCHAR,
    P_INGEST_RUN_ID NUMBER
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    v_sql STRING;
    v_rs RESULTSET;
    v_pk_rs RESULTSET;
    v_dq_config_id INTEGER; 
    v_primary_key_column VARCHAR;
    v_validation_name VARCHAR;
    v_validation_type VARCHAR;
    v_validate_column VARCHAR;
    v_validation_query VARCHAR;
    v_validation_error_msg VARCHAR;
    v_check_count INTEGER DEFAULT 0;
    
BEGIN
    -- Fetch the primary key column first
    v_sql := '
        SELECT PRIMARY_KEY_COLUMN
        FROM COMMON.DQ_CONFIG
        WHERE VALIDATE_TABLE = ?
          AND ERROR_TABLE = ?
          AND STATUS = TRUE
        LIMIT 1
    ';
    
    v_pk_rs := (
        EXECUTE IMMEDIATE :v_sql
        USING (P_VALIDATE_TABLE, P_ERROR_TABLE)
    );
    
    FOR pk_record IN v_pk_rs DO
        v_primary_key_column := pk_record.PRIMARY_KEY_COLUMN;
    END FOR;
    
    -- Validate that primary key column was found
    IF (v_primary_key_column IS NULL) THEN
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', 'Primary key column not configured for table: ' || P_VALIDATE_TABLE,
            'ROWS_VALIDATED', '0'
        );
    END IF;

    -- Fetch validation rules
    v_sql := '
        SELECT 
            DQ_CONFIG_ID,
            VALIDATION_NAME,
            VALIDATION_TYPE,
            VALIDATE_COLUMN,
            VALIDATION_QUERY,
            VALIDATION_ERROR_MSG
        FROM COMMON.DQ_CONFIG
        WHERE VALIDATE_TABLE = ?
          AND ERROR_TABLE = ?
          AND STATUS = TRUE
        ORDER BY 
            CASE VALIDATION_TYPE
                WHEN ''MANDATORY_CHECK'' THEN 1
                WHEN ''VALUE_CHECK'' THEN 2
                WHEN ''LOOKUP_CHECK'' THEN 3
                WHEN ''DUPLICATE_ALLOW_ONE_CHECK'' THEN 4
                ELSE 5
            END,
            DQ_CONFIG_ID
    ';

    v_rs := (
        EXECUTE IMMEDIATE :v_sql
        USING (P_VALIDATE_TABLE, P_ERROR_TABLE)
    );

    FOR record IN v_rs DO
        v_dq_config_id := record.DQ_CONFIG_ID;
        v_validation_name := record.VALIDATION_NAME;
        v_validation_type := record.VALIDATION_TYPE;
        v_validate_column := record.VALIDATE_COLUMN;
        v_validation_query := record.VALIDATION_QUERY;
        v_validation_error_msg := record.VALIDATION_ERROR_MSG;
        
        v_check_count := v_check_count + 1;
        
        CASE v_validation_type
            -- ============= MANDATORY CHECK =============
            WHEN 'MANDATORY_CHECK' THEN
                v_sql := '
                    UPDATE ' || P_STAGE_TABLE || '
                    SET IS_VALID = FALSE
                    WHERE IS_VALID = TRUE
                      AND NOT (' || v_validation_query || ')';
                EXECUTE IMMEDIATE v_sql;
                
                v_sql := '
                    INSERT INTO ' || P_ERROR_TABLE || ' (
                        ERROR_ID,
                        VALIDATE_COLUMN,
                        VALIDATION_TYPE,
                        VALIDATION_ERROR_MSG,
                        INGEST_RUN_ID
                    )
                    SELECT 
                        ' || v_primary_key_column || ',
                        ''' || v_validate_column || ''',
                        ''' || v_validation_type || ''',
                        ''' || v_validation_error_msg || ''',
                        ' || P_INGEST_RUN_ID || '
                    FROM ' || P_STAGE_TABLE || '
                    WHERE IS_VALID = FALSE
                      AND NOT (' || v_validation_query || ')
                      AND ' || v_primary_key_column || ' NOT IN (
                          SELECT ERROR_ID 
                          FROM ' || P_ERROR_TABLE || '
                          WHERE INGEST_RUN_ID = ' || P_INGEST_RUN_ID || '
                            AND VALIDATE_COLUMN = ''' || v_validate_column || '''
                            AND VALIDATION_TYPE = ''' || v_validation_type || '''
                      )';
                EXECUTE IMMEDIATE v_sql;
            
            -- ============= VALUE CHECK =============
            WHEN 'VALUE_CHECK' THEN
                v_sql := '
                    UPDATE ' || P_STAGE_TABLE || '
                    SET IS_VALID = FALSE
                    WHERE IS_VALID = TRUE
                      AND ' || v_validate_column || ' IS NOT NULL
                      AND NOT (' || v_validation_query || ')';
                EXECUTE IMMEDIATE v_sql;
                
                v_sql := '
                    INSERT INTO ' || P_ERROR_TABLE || ' (
                        ERROR_ID,
                        VALIDATE_COLUMN,
                        VALIDATION_TYPE,
                        VALIDATION_ERROR_MSG,
                        INGEST_RUN_ID
                    )
                    SELECT 
                        ' || v_primary_key_column || ',
                        ''' || v_validate_column || ''',
                        ''' || v_validation_type || ''',
                        ''' || v_validation_error_msg || ' - Value: '' || COALESCE(TO_VARCHAR(' || v_validate_column || '), ''NULL''),
                        ' || P_INGEST_RUN_ID || '
                    FROM ' || P_STAGE_TABLE || '
                    WHERE IS_VALID = FALSE
                      AND ' || v_validate_column || ' IS NOT NULL
                      AND NOT (' || v_validation_query || ')
                      AND ' || v_primary_key_column || ' NOT IN (
                          SELECT ERROR_ID 
                          FROM ' || P_ERROR_TABLE || '
                          WHERE INGEST_RUN_ID = ' || P_INGEST_RUN_ID || '
                            AND VALIDATE_COLUMN = ''' || v_validate_column || '''
                            AND VALIDATION_TYPE = ''' || v_validation_type || '''
                      )';
                EXECUTE IMMEDIATE v_sql;
            
            -- ============= LOOKUP CHECK =============
            WHEN 'LOOKUP_CHECK' THEN
                v_sql := '
                    UPDATE ' || P_STAGE_TABLE || '
                    SET IS_VALID = FALSE
                    WHERE IS_VALID = TRUE
                      AND ' || v_validate_column || ' IS NOT NULL
                      AND NOT (' || v_validation_query || ')';
                EXECUTE IMMEDIATE v_sql;
                
                v_sql := '
                    INSERT INTO ' || P_ERROR_TABLE || ' (
                        ERROR_ID,
                        VALIDATE_COLUMN,
                        VALIDATION_TYPE,
                        VALIDATION_ERROR_MSG,
                        INGEST_RUN_ID
                    )
                    SELECT 
                        ' || v_primary_key_column || ',
                        ''' || v_validate_column || ''',
                        ''' || v_validation_type || ''',
                        ''' || v_validation_error_msg || ' - Value: '' || COALESCE(TO_VARCHAR(' || v_validate_column || '), ''NULL''),
                        ' || P_INGEST_RUN_ID || '
                    FROM ' || P_STAGE_TABLE || '
                    WHERE IS_VALID = FALSE
                      AND ' || v_validate_column || ' IS NOT NULL
                      AND NOT (' || v_validation_query || ')
                      AND ' || v_primary_key_column || ' NOT IN (
                          SELECT ERROR_ID 
                          FROM ' || P_ERROR_TABLE || '
                          WHERE INGEST_RUN_ID = ' || P_INGEST_RUN_ID || '
                            AND VALIDATE_COLUMN = ''' || v_validate_column || '''
                            AND VALIDATION_TYPE = ''' || v_validation_type || '''
                      )';
                EXECUTE IMMEDIATE v_sql;
            
            -- ============= DUPLICATE_ALLOW_ONE CHECK =============
            WHEN 'DUPLICATE_ALLOW_ONE_CHECK' THEN
                v_sql := '
                    UPDATE ' || P_STAGE_TABLE || ' t1
                    SET IS_VALID = FALSE
                    WHERE IS_VALID = TRUE
                      AND ' || v_validate_column || ' IS NOT NULL
                      AND ' || v_validate_column || ' IN (
                          SELECT ' || v_validate_column || '
                          FROM ' || P_STAGE_TABLE || '
                          WHERE ' || v_validate_column || ' IS NOT NULL
                          GROUP BY ' || v_validate_column || '
                          HAVING COUNT(*) > 1
                      )
                      AND ' || v_primary_key_column || ' NOT IN (
                          SELECT MIN(' || v_primary_key_column || ')
                          FROM ' || P_STAGE_TABLE || '
                          WHERE ' || v_validate_column || ' IS NOT NULL
                          GROUP BY ' || v_validate_column || '
                      )';
                EXECUTE IMMEDIATE v_sql;
                
                v_sql := '
                    INSERT INTO ' || P_ERROR_TABLE || ' (
                        ERROR_ID,
                        VALIDATE_COLUMN,
                        VALIDATION_TYPE,
                        VALIDATION_ERROR_MSG,
                        INGEST_RUN_ID
                    )
                    SELECT 
                        ' || v_primary_key_column || ',
                        ''' || v_validate_column || ''',
                        ''' || v_validation_type || ''',
                        ''' || v_validation_error_msg || ' - Value: '' || COALESCE(TO_VARCHAR(' || v_validate_column || '), ''NULL''),
                        ' || P_INGEST_RUN_ID || '
                    FROM ' || P_STAGE_TABLE || '
                    WHERE IS_VALID = FALSE
                      AND ' || v_validate_column || ' IS NOT NULL
                      AND ' || v_validate_column || ' IN (
                          SELECT ' || v_validate_column || '
                          FROM ' || P_STAGE_TABLE || '
                          WHERE ' || v_validate_column || ' IS NOT NULL
                          GROUP BY ' || v_validate_column || '
                          HAVING COUNT(*) > 1
                      )
                      AND ' || v_primary_key_column || ' NOT IN (
                          SELECT MIN(' || v_primary_key_column || ')
                          FROM ' || P_STAGE_TABLE || '
                          WHERE ' || v_validate_column || ' IS NOT NULL
                          GROUP BY ' || v_validate_column || '
                      )
                      AND ' || v_primary_key_column || ' NOT IN (
                          SELECT ERROR_ID 
                          FROM ' || P_ERROR_TABLE || '
                          WHERE INGEST_RUN_ID = ' || P_INGEST_RUN_ID || '
                            AND VALIDATE_COLUMN = ''' || v_validate_column || '''
                            AND VALIDATION_TYPE = ''' || v_validation_type || '''
                      )';
                EXECUTE IMMEDIATE v_sql;
            
            ELSE
                RETURN OBJECT_CONSTRUCT(
                    'STATUS', 'FAILED',
                    'ERROR', 'Unknown validation type: ' || v_validation_type,
                    'ROWS_VALIDATED', v_check_count::VARCHAR
                );
        END CASE;      
    END FOR;
    
    RETURN OBJECT_CONSTRUCT(
        'STATUS', 'SUCCESS',
        'ERROR', 'NONE',
        'ROWS_VALIDATED', v_check_count::VARCHAR,
        'MESSAGE', 'validation checks executed'
    );
    
EXCEPTION
    WHEN OTHER THEN
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', SQLERRM,
            'ROWS_VALIDATED', v_check_count::VARCHAR,
            'MESSAGE', 'validation checks executed'
        );
END;
$$;


-- =====================================================
-- DQ CHECK DEFINATION
-- =====================================================
SELECT * FROM COMMON.DQ_CONFIG;
    -- NAME,
    --             $2::STRING AS MOBILE,
    --             $3::STRING AS EMAIL,
    --             $4::STRING AS LOGIN_BY_USING,
    --             $5::STRING AS GENDER,
    --             $6::STRING AS DOB,
    --             $7::STRING AS ANNIVERSARY,
    --             $8::STRING AS PREFERENCES
    
    -- -- ============= VALUE RANGE CHECKS =============
    -- ('bronze.customers', 'Age Range', 'VALUE_RANGE', 
    --  'age', 'age BETWEEN 18 AND 120', 
    --  NULL, NULL, 'Age must be between 18 and 120', 'ERROR', 20),
    
    -- ('bronze.customers', 'Salary Range', 'VALUE_RANGE', 
    --  'salary', 'salary >= 0 AND salary <= 10000000', 
    --  NULL, NULL, 'Salary must be between 0 and 10M', 'WARNING', 21),
    
    -- ('bronze.customers', 'Valid Status', 'VALUE_RANGE', 
    --  'status', 'status IN (''ACTIVE'', ''INACTIVE'', ''PENDING'', ''SUSPENDED'')', 
    --  NULL, NULL, 'Invalid customer status', 'ERROR', 22),
    
    -- ('bronze.customers', 'Credit Score Range', 'VALUE_RANGE', 
    --  'credit_score', 'credit_score BETWEEN 300 AND 850', 
    --  NULL, NULL, 'Credit score out of valid range', 'WARNING', 23),
    
    -- -- ============= DUPLICATE CHECKS =============
    -- ('bronze.customers', 'Unique Customer ID', 'DUPLICATE', 
    --  'customer_id', 'customer_id', 
    --  NULL, NULL, 'Duplicate customer_id found', 'CRITICAL', 30),
    
    -- ('bronze.customers', 'Unique Email', 'DUPLICATE', 
    --  'email', 'email', 
    --  NULL, NULL, 'Duplicate email address found', 'ERROR', 31),
    
    -- ('bronze.customers', 'Unique SSN', 'DUPLICATE', 
    --  'ssn', 'ssn', 
    --  NULL, NULL, 'Duplicate SSN found', 'CRITICAL', 32),
    
    -- -- ============= PATTERN/FORMAT CHECKS =============
    -- ('bronze.customers', 'Email Format', 'PATTERN', 
    --  'email', 'REGEXP_LIKE(email, ''^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'')', 
    --  NULL, NULL, 'Invalid email format', 'ERROR', 40),
    
    -- ('bronze.customers', 'Phone Format', 'PATTERN', 
    --  'phone', 'REGEXP_LIKE(phone, ''^\\+?[1-9]\\d{1,14}$'') OR phone IS NULL', 
    --  NULL, NULL, 'Invalid phone number format', 'WARNING', 41),
    
    -- ('bronze.customers', 'Postal Code Format', 'PATTERN', 
    --  'postal_code', 'REGEXP_LIKE(postal_code, ''^[0-9]{5}(-[0-9]{4})?$'') OR postal_code IS NULL', 
    --  NULL, NULL, 'Invalid US postal code format', 'WARNING', 42),
    
    -- ('bronze.customers', 'SSN Format', 'PATTERN', 
    --  'ssn', 'REGEXP_LIKE(ssn, ''^[0-9]{3}-[0-9]{2}-[0-9]{4}$'') OR ssn IS NULL', 
    --  NULL, NULL, 'Invalid SSN format (XXX-XX-XXXX)', 'ERROR', 43),
    
    -- -- ============= REFERENTIAL INTEGRITY CHECKS =============
    -- ('bronze.orders', 'Valid Customer Reference', 'REFERENTIAL_INTEGRITY', 
    --  'customer_id', 'customer_id', 
    --  'silver.customers', NULL, 'Customer ID does not exist in customer table', 'CRITICAL', 50),
    
    -- ('bronze.orders', 'Valid Product Reference', 'REFERENTIAL_INTEGRITY', 
    --  'product_id', 'product_id', 
    --  'silver.products', NULL, 'Product ID does not exist in product table', 'CRITICAL', 51),
    
    -- ('bronze.transactions', 'Valid Account Reference', 'REFERENTIAL_INTEGRITY', 
    --  'account_id', 'account_id', 
    --  'silver.accounts', NULL, 'Account ID not found', 'CRITICAL', 52),
    
    -- -- ============= DATA TYPE CHECKS =============
    -- ('bronze.customers', 'Email Data Type', 'DATA_TYPE', 
    --  'email', 'TRY_CAST(email AS VARCHAR) IS NOT NULL', 
    --  NULL, NULL, 'Email is not a valid string', 'ERROR', 60),
    
    -- ('bronze.customers', 'Age Numeric', 'DATA_TYPE', 
    --  'age', 'TRY_CAST(age AS INTEGER) IS NOT NULL', 
    --  NULL, NULL, 'Age must be numeric', 'ERROR', 61),
    
    -- ('bronze.orders', 'Order Date Valid', 'DATA_TYPE', 
    --  'order_date', 'TRY_CAST(order_date AS DATE) IS NOT NULL', 
    --  NULL, NULL, 'Invalid date format', 'CRITICAL', 62),
    
    -- -- ============= COMPLETENESS CHECKS =============
    -- ('bronze.customers', 'Address Completeness', 'COMPLETENESS', 
    --  'address', 'address IS NOT NULL AND city IS NOT NULL AND state IS NOT NULL', 
    --  NULL, '0.95', 'Address information incomplete', 'WARNING', 70),
    
    -- ('bronze.customers', 'Contact Info Completeness', 'COMPLETENESS', 
    --  'phone,email', 'phone IS NOT NULL OR email IS NOT NULL', 
    --  NULL, NULL, 'At least one contact method required', 'ERROR', 71),
    
    -- -- ============= CONSISTENCY CHECKS =============
    -- ('bronze.orders', 'Order Amount Consistency', 'CONSISTENCY', 
    --  'total_amount', 'total_amount = (quantity * unit_price)', 
    --  NULL, NULL, 'Total amount does not match quantity × price', 'ERROR', 80),
    
    -- ('bronze.customers', 'Age vs DOB Consistency', 'CONSISTENCY', 
    --  'age', 'age = YEAR(CURRENT_DATE()) - YEAR(date_of_birth)', 
    --  NULL, NULL, 'Age does not match date of birth', 'WARNING', 81),
    
    -- ('bronze.orders', 'Date Logic Consistency', 'CONSISTENCY', 
    --  'ship_date', 'ship_date >= order_date OR ship_date IS NULL', 
    --  NULL, NULL, 'Ship date cannot be before order date', 'ERROR', 82),
    
    -- -- ============= FRESHNESS CHECKS =============
    -- ('bronze.customers', 'Data Freshness', 'FRESHNESS', 
    --  'last_updated', 'last_updated >= DATEADD(day, -30, CURRENT_DATE())', 
    --  NULL, '30', 'Record not updated in last 30 days', 'WARNING', 90),
    
    -- ('bronze.transactions', 'Transaction Recency', 'FRESHNESS', 
    --  'transaction_date', 'transaction_date >= DATEADD(day, -7, CURRENT_DATE())', 
    --  NULL, '7', 'Transaction older than 7 days', 'WARNING', 91),
    
    -- -- ============= VOLUME/COUNT CHECKS =============
    -- ('bronze.customers', 'Minimum Record Count', 'VOLUME', 
    --  NULL, 'COUNT(*) >= 100', 
    --  NULL, '100', 'Insufficient records loaded', 'CRITICAL', 100),
    
    -- ('bronze.orders', 'Expected Daily Volume', 'VOLUME', 
    --  NULL, 'COUNT(*) BETWEEN 1000 AND 50000', 
    --  NULL, '1000-50000', 'Order volume outside expected range', 'WARNING', 101),
    
    -- -- ============= STATISTICAL CHECKS =============
    -- ('bronze.customers', 'Salary Outlier Detection', 'STATISTICAL', 
    --  'salary', 'salary <= (SELECT AVG(salary) + (3 * STDDEV(salary)) FROM bronze.customers)', 
    --  NULL, '3', 'Salary is statistical outlier (3 sigma)', 'WARNING', 110),
    
    -- ('bronze.orders', 'Order Amount Distribution', 'STATISTICAL', 
    --  'order_amount', 'order_amount BETWEEN 1 AND 10000', 
    --  NULL, '10000', 'Order amount exceeds typical range', 'WARNING', 111),
    
    -- -- ============= CUSTOM BUSINESS RULE CHECKS =============
    -- ('bronze.customers', 'VIP Status Business Rule', 'CUSTOM_QUERY', 
    --  'customer_type', 'NOT (customer_type = ''VIP'' AND total_purchases < 10000)', 
    --  NULL, NULL, 'VIP customers must have purchases >= $10,000', 'ERROR', 120),
    
    -- ('bronze.orders', 'Discount Logic', 'CUSTOM_QUERY', 
    --  'discount_amount', 'discount_amount <= (total_amount * 0.5)', 
    --  NULL, NULL, 'Discount cannot exceed 50% of total amount', 'ERROR', 121),
    
    -- ('bronze.employees', 'Salary Grade Alignment', 'CUSTOM_QUERY', 
    --  'salary', 'salary BETWEEN (SELECT min_salary FROM ref.salary_grades WHERE grade = employee_grade) 
    --                        AND (SELECT max_salary FROM ref.salary_grades WHERE grade = employee_grade)', 
    --  'ref.salary_grades', NULL, 'Salary not aligned with employee grade', 'WARNING', 122);

-- =====================================================
-- DQ CHECK VALIDATION     
-- =====================================================
            -- WHEN 'VALUE_RANGE' THEN
            --     -- Value range check using validation_query
            --     v_sql := 'UPDATE ' || p_stage_table || '
            --              SET is_valid = FALSE,
            --                  dq_errors = ARRAY_APPEND(dq_errors, 
            --                      OBJECT_CONSTRUCT(
            --                          ''check_id'', ' || v_dq_config_id || ',
            --                          ''check_name'', ''' || v_validation_name || ''',
            --                          ''type'', ''' || v_validation_type || ''',
            --                          ''column'', ''' || v_validation_column || ''',
            --                          ''message'', ''' || v_validation_error_msg || ''',
            --                          ''severity'', ''' || v_severity || ''',
            --                          ''value'', TO_VARCHAR(' || v_validation_column || ')
            --                      ))
            --              WHERE NOT (' || v_validation_query || ') AND is_valid = TRUE';
            --     EXECUTE IMMEDIATE v_sql;
                
            -- WHEN 'PATTERN' THEN
            --     -- Pattern validation using validation_query
            --     v_sql := 'UPDATE ' || p_stage_table || '
            --              SET is_valid = FALSE,
            --                  dq_errors = ARRAY_APPEND(dq_errors, 
            --                      OBJECT_CONSTRUCT(
            --                          ''check_id'', ' || v_dq_config_id || ',
            --                          ''check_name'', ''' || v_validation_name || ''',
            --                          ''type'', ''' || v_validation_type || ''',
            --                          ''column'', ''' || v_validation_column || ''',
            --                          ''message'', ''' || v_validation_error_msg || ''',
            --                          ''severity'', ''' || v_severity || ''',
            --                          ''value'', TO_VARCHAR(' || v_validation_column || ')
            --                      ))
            --              WHERE ' || v_validation_column || ' IS NOT NULL 
            --                AND NOT (' || v_validation_query || ') 
            --                AND is_valid = TRUE';
            --     EXECUTE IMMEDIATE v_sql;
                
            -- WHEN 'DUPLICATE' THEN
            --     -- Duplicate check using validation_column
            --     v_sql := 'UPDATE ' || p_stage_table || ' stg
            --              SET is_valid = FALSE,
            --                  dq_errors = ARRAY_APPEND(dq_errors, 
            --                      OBJECT_CONSTRUCT(
            --                          ''check_id'', ' || v_dq_config_id || ',
            --                          ''check_name'', ''' || v_validation_name || ''',
            --                          ''type'', ''' || v_validation_type || ''',
            --                          ''column'', ''' || v_validation_column || ''',
            --                          ''message'', ''' || v_validation_error_msg || ''',
            --                          ''severity'', ''' || v_severity || ''',
            --                          ''value'', TO_VARCHAR(' || v_validation_column || ')
            --                      ))
            --              WHERE is_valid = TRUE
            --                AND ' || v_validation_column || ' IN (
            --                    SELECT ' || v_validation_column || '
            --                    FROM ' || p_stage_table || '
            --                    WHERE ' || v_validation_column || ' IS NOT NULL
            --                    GROUP BY ' || v_validation_column || '
            --                    HAVING COUNT(*) > 1
            --                )';
            --     EXECUTE IMMEDIATE v_sql;
                
            -- WHEN 'REFERENTIAL_INTEGRITY' THEN
            --     -- FK check using validation_table and validation_column
            --     v_sql := 'UPDATE ' || p_stage_table || ' stg
            --              SET is_valid = FALSE,
            --                  dq_errors = ARRAY_APPEND(dq_errors, 
            --                      OBJECT_CONSTRUCT(
            --                          ''check_id'', ' || v_dq_config_id || ',
            --                          ''check_name'', ''' || v_validation_name || ''',
            --                          ''type'', ''' || v_validation_type || ''',
            --                          ''column'', ''' || v_validation_column || ''',
            --                          ''reference_table'', ''' || v_validation_table || ''',
            --                          ''message'', ''' || v_validation_error_msg || ''',
            --                          ''severity'', ''' || v_severity || ''',
            --                          ''value'', TO_VARCHAR(' || v_validation_column || ')
            --                      ))
            --              WHERE is_valid = TRUE
            --                AND ' || v_validation_column || ' IS NOT NULL
            --                AND ' || v_validation_column || ' NOT IN (
            --                    SELECT ' || v_validation_column || ' 
            --                    FROM ' || v_validation_table || '
            --                )';
            --     EXECUTE IMMEDIATE v_sql;
                
            -- WHEN 'DATA_TYPE' THEN
            --     -- Data type validation using validation_query
            --     v_sql := 'UPDATE ' || p_stage_table || '
            --              SET is_valid = FALSE,
            --                  dq_errors = ARRAY_APPEND(dq_errors, 
            --                      OBJECT_CONSTRUCT(
            --                          ''check_id'', ' || v_dq_config_id || ',
            --                          ''check_name'', ''' || v_validation_name || ''',
            --                          ''type'', ''' || v_validation_type || ''',
            --                          ''column'', ''' || v_validation_column || ''',
            --                          ''message'', ''' || v_validation_error_msg || ''',
            --                          ''severity'', ''' || v_severity || '''
            --                      ))
            --              WHERE NOT (' || v_validation_query || ') AND is_valid = TRUE';
            --     EXECUTE IMMEDIATE v_sql;
                
            -- WHEN 'COMPLETENESS' THEN
            --     -- Completeness check using validation_query
            --     v_sql := 'UPDATE ' || p_stage_table || '
            --              SET is_valid = FALSE,
            --                  dq_errors = ARRAY_APPEND(dq_errors, 
            --                      OBJECT_CONSTRUCT(
            --                          ''check_id'', ' || v_dq_config_id || ',
            --                          ''check_name'', ''' || v_validation_name || ''',
            --                          ''type'', ''' || v_validation_type || ''',
            --                          ''column'', ''' || COALESCE(v_validation_column, 'Multiple') || ''',
            --                          ''message'', ''' || v_validation_error_msg || ''',
            --                          ''severity'', ''' || v_severity || '''
            --                      ))
            --              WHERE NOT (' || v_validation_query || ') AND is_valid = TRUE';
            --     EXECUTE IMMEDIATE v_sql;
                
            -- WHEN 'CONSISTENCY' THEN
            --     -- Consistency check using validation_query
            --     v_sql := 'UPDATE ' || p_stage_table || '
            --              SET is_valid = FALSE,
            --                  dq_errors = ARRAY_APPEND(dq_errors, 
            --                      OBJECT_CONSTRUCT(
            --                          ''check_id'', ' || v_dq_config_id || ',
            --                          ''check_name'', ''' || v_validation_name || ''',
            --                          ''type'', ''' || v_validation_type || ''',
            --                          ''column'', ''' || v_validation_column || ''',
            --                          ''message'', ''' || v_validation_error_msg || ''',
            --                          ''severity'', ''' || v_severity || '''
            --                      ))
            --              WHERE NOT (' || v_validation_query || ') AND is_valid = TRUE';
            --     EXECUTE IMMEDIATE v_sql;
                
            -- WHEN 'FRESHNESS' THEN
            --     -- Freshness check using validation_query
            --     v_sql := 'UPDATE ' || p_stage_table || '
            --              SET is_valid = FALSE,
            --                  dq_errors = ARRAY_APPEND(dq_errors, 
            --                      OBJECT_CONSTRUCT(
            --                          ''check_id'', ' || v_dq_config_id || ',
            --                          ''check_name'', ''' || v_validation_name || ''',
            --                          ''type'', ''' || v_validation_type || ''',
            --                          ''column'', ''' || v_validation_column || ''',
            --                          ''message'', ''' || v_validation_error_msg || ''',
            --                          ''severity'', ''' || v_severity || ''',
            --                          ''threshold_days'', ''' || v_threshold_value || '''
            --                      ))
            --              WHERE NOT (' || v_validation_query || ') AND is_valid = TRUE';
            --     EXECUTE IMMEDIATE v_sql;
                
            -- WHEN 'VOLUME' THEN
            --     -- Volume check - table level validation
            --     v_validation_query := 'SELECT CASE WHEN ' || v_validation_query || ' THEN 1 ELSE 0 END FROM ' || p_stage_table || ' LIMIT 1';
            --     EXECUTE IMMEDIATE v_validation_query INTO v_count;
                
            --     IF v_count = 0 THEN
            --         -- Log volume check failure (not row-level)
            --         INSERT INTO common.load_error (
            --             ingest_run_id, source_table, error_type, error_column,
            --             error_description, error_severity
            --         ) VALUES (
            --             p_ingest_run_id, p_bronze_table, v_validation_type, 'TABLE_LEVEL',
            --             v_validation_error_msg || ' (Threshold: ' || v_threshold_value || ')', v_severity
            --         );
            --     END IF;
                
            -- WHEN 'STATISTICAL' THEN
            --     -- Statistical outlier detection using validation_query
            --     v_sql := 'UPDATE ' || p_stage_table || '
            --              SET is_valid = FALSE,
            --                  dq_errors = ARRAY_APPEND(dq_errors, 
            --                      OBJECT_CONSTRUCT(
            --                          ''check_id'', ' || v_dq_config_id || ',
            --                          ''check_name'', ''' || v_validation_name || ''',
            --                          ''type'', ''' || v_validation_type || ''',
            --                          ''column'', ''' || v_validation_column || ''',
            --                          ''message'', ''' || v_validation_error_msg || ''',
            --                          ''severity'', ''' || v_severity || ''',
            --                          ''threshold'', ''' || v_threshold_value || ''',
            --                          ''value'', TO_VARCHAR(' || v_validation_column || ')
            --                      ))
            --              WHERE NOT (' || v_validation_query || ') AND is_valid = TRUE';
            --     EXECUTE IMMEDIATE v_sql;
                
            -- WHEN 'CUSTOM_QUERY' THEN
            --     -- Custom business rule using validation_query
            --     v_sql := 'UPDATE ' || p_stage_table || '
            --              SET is_valid = FALSE,
            --                  dq_errors = ARRAY_APPEND(dq_errors, 
            --                      OBJECT_CONSTRUCT(
            --                          ''check_id'', ' || v_dq_config_id || ',
            --                          ''check_name'', ''' || v_validation_name || ''',
            --                          ''type'', ''' || v_validation_type || ''',
            --                          ''column'', ''' || COALESCE(v_validation_column, 'Multiple') || ''',
            --                          ''message'', ''' || v_validation_error_msg || ''',
            --                          ''severity'', ''' || v_severity || '''
            --                      ))
            --              WHERE NOT (' || v_validation_query || ') AND is_valid = TRUE';
            --     EXECUTE IMMEDIATE v_sql;

