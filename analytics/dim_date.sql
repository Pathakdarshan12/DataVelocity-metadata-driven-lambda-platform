-- ====================================================================================================
-- DIM_DATE
-- ====================================================================================================
-- CHANGE_CONTEXT
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SWIGGY;
USE SCHEMA GOLD;

-- ----------------------------------------------------------------------------------------------------
-- CREATE DIM_DATE
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.DIM_DATE (
    DIM_DATE_HK INTEGER PRIMARY KEY AUTOINCREMENT,   -- SURROGATE KEY FOR DATE DIMENSION
    CALENDAR_DATE DATE UNIQUE,                     -- THE ACTUAL CALENDAR DATE
    YEAR NUMBER,                                   -- YEAR
    QUARTER NUMBER,                                -- QUARTER (1-4)
    MONTH NUMBER,                                  -- MONTH (1-12)
    WEEK NUMBER,                                   -- WEEK OF THE YEAR
    DAY_OF_YEAR NUMBER,                            -- DAY OF THE YEAR (1-365/366)
    DAY_OF_WEEK NUMBER,                            -- DAY OF THE WEEK (1-7)
    DAY_OF_THE_MONTH NUMBER,                       -- DAY OF THE MONTH (1-31)
    DAY_NAME STRING                                -- NAME OF THE DAY (E.G., MONDAY)
)
COMMENT = 'DATE DIMENSION TABLE CREATED USING MIN OF ORDER DATA.';

INSERT INTO GOLD.DIM_DATE
WITH RECURSIVE MY_DIM_DATE_CTE AS
(
    -- ANCHOR CLAUSE
    SELECT
        CURRENT_DATE() AS TODAY,
        YEAR(TODAY) AS YEAR,
        QUARTER(TODAY) AS QUARTER,
        MONTH(TODAY) AS MONTH,
        WEEK(TODAY) AS WEEK,
        DAYOFYEAR(TODAY) AS DAY_OF_YEAR,
        DAYOFWEEK(TODAY) AS DAY_OF_WEEK,
        DAY(TODAY) AS DAY_OF_THE_MONTH,
        DAYNAME(TODAY) AS DAY_NAME

    UNION ALL

     -- RECURSIVE CLAUSE
    SELECT
        DATEADD('DAY', -1, TODAY) AS TODAY_R,
        YEAR(TODAY_R) AS YEAR,
        QUARTER(TODAY_R) AS QUARTER,
        MONTH(TODAY_R) AS MONTH,
        WEEK(TODAY_R) AS WEEK,
        DAYOFYEAR(TODAY_R) AS DAY_OF_YEAR,
        DAYOFWEEK(TODAY_R) AS DAY_OF_WEEK,
        DAY(TODAY_R) AS DAY_OF_THE_MONTH,
        DAYNAME(TODAY_R) AS DAY_NAME
    FROM
        MY_DIM_DATE_CTE
    WHERE
        TODAY_R > (SELECT DATE(MIN(ORDER_DATE)) FROM SILVER.ORDERS_SLV)
)
SELECT
    HASH(SHA1_HEX(TODAY)) AS DATE_DIM_HK,
    TODAY ,                     -- THE ACTUAL CALENDAR DATE
    YEAR,                                   -- YEAR
    QUARTER,                                -- QUARTER (1-4)
    MONTH,                                  -- MONTH (1-12)
    WEEK,                                   -- WEEK OF THE YEAR
    DAY_OF_YEAR,                            -- DAY OF THE YEAR (1-365/366)
    DAY_OF_WEEK,                            -- DAY OF THE WEEK (1-7)
    DAY_OF_THE_MONTH,                       -- DAY OF THE MONTH (1-31)
    DAY_NAME
FROM MY_DIM_DATE_CTE;