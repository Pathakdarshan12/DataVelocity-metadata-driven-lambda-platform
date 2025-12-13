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
-- Create File Format for stage files
-- ----------------------------------------------------------------------------------------------------
CREATE FILE FORMAT IF NOT EXISTS BRONZE.CSV_FILE_FORMAT
TYPE = 'CSV'
COMPRESSION = 'AUTO'
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
NULL_IF = ('\\N');

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
TO_VARCHAR('** EAMIL **');
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE MASKING POLICY
COMMON.PHONE_MASKING_POLICY AS (PHONE STRING)
RETURNS STRING ->
TO_VARCHAR('** PHONE **');
-- ====================================================================================================
