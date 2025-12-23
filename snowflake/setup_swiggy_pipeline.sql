-- ====================================================================================================
-- Create Database, Schema & Common Object
-- ====================================================================================================
-- Create Warehouse
CREATE WAREHOUSE IF NOT EXISTS ADHOC_WH
COMMENT = 'This is the ADHOC-WH'
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_RESUME = TRUE
    AUTO_SUSPEND = 60
    ENABLE_QUERY_ACCELERATION = FALSE
    WAREHOUSE_TYPE = 'STANDARD'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    SCALING_POLICY = 'STANDARD'
    INITIALLY_SUSPENDED = TRUE;

-- ----------------------------------------------------------------------------------------------------
-- Create Database & Schemas
-- ----------------------------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS SWIGGY;
USE DATABASE SWIGGY;
CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;
CREATE SCHEMA IF NOT EXISTS COMMON;

-- ----------------------------------------------------------------------------------------------------
-- Create File Formats for stage files
-- ----------------------------------------------------------------------------------------------------
-- CSV Comma Delimited
CREATE FILE FORMAT IF NOT EXISTS BRONZE.FF_CSV_COMMA
TYPE = 'CSV'
COMPRESSION = 'AUTO'
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
NULL_IF = ('\\N');

-- CSV Pipe Delimited
CREATE OR REPLACE FILE FORMAT COMMON.FF_CSV_PIPE
TYPE = 'CSV'
FIELD_DELIMITER = '|'
SKIP_HEADER = 1
TRIM_SPACE = TRUE
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL', 'null', '')
EMPTY_FIELD_AS_NULL = TRUE
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- JSON
CREATE OR REPLACE FILE FORMAT COMMON.FF_JSON
TYPE = 'JSON'
STRIP_OUTER_ARRAY = TRUE
IGNORE_UTF8_ERRORS = TRUE
NULL_IF = ('NULL', 'null');

-- PARQUET
CREATE OR REPLACE FILE FORMAT COMMON.FF_PARQUET
TYPE = 'PARQUET'
SNAPPY_COMPRESSION = TRUE;

-- ----------------------------------------------------------------------------------------------------
-- Create File Stage
-- ----------------------------------------------------------------------------------------------------
CREATE STAGE BRONZE.CSV_STG
DIRECTORY = ( ENABLE = TRUE )
COMMENT = 'THIS IS THE SNOWFLAKE INTERNAL STAGE';

-- ----------------------------------------------------------------------------------------------------
-- Create Tag
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TAG
COMMON.PII_POLICY_TAG
ALLOWED_VALUES 'PII','PRICE','SENSITIVE','EMAIL'
COMMENT = 'THIS IS PII POLICY TAG OBJECT';

-- ----------------------------------------------------------------------------------------------------
-- Create Masking Policy
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE MASKING POLICY
COMMON.PII_MASKING_POLICY AS (PII_TEXT STRING)
RETURNS STRING ->
TO_VARCHAR('** PII **');
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE MASKING POLICY
COMMON.EMAIL_MASKING_POLICY AS (EMAIL_TEXT STRING)
RETURNS STRING ->
TO_VARCHAR('** EMAIL **');
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE MASKING POLICY
COMMON.PHONE_MASKING_POLICY AS (PHONE STRING)
RETURNS STRING ->
TO_VARCHAR('** PHONE **');
-- ====================================================================================================