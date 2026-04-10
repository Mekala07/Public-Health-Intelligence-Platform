-- ==============================================================================
-- 02 ANALYTICS TRACK SETUP
-- ==============================================================================
-- Track 2: Analytics Track
-- Problem: Analytics Problem 01 — The Invisible Workforce Signal
-- ==============================================================================
--
-- HOW TO LOAD THE CSV DATA (do this BEFORE running the script):
--   Option A — Snowflake UI (easiest):
--     1. Left nav -> Data -> Databases -> HACKATHON -> DATA -> Stages
--     2. Click "+ Stage", name it CSV_STAGE, click Create
--     3. Open CSV_STAGE, click "Upload Files", select hr_attrition.csv
--
--   Option B — SnowSQL CLI:
--     PUT file://C:/Users/mekal/OneDrive/Desktop/SNWFLAKE_HACKATHON/Sample Data/hr_attrition.csv
--         @HACKATHON.DATA.CSV_STAGE AUTO_COMPRESS=FALSE;
--
-- After uploading, run this entire script via Run All.
-- ==============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE HACKATHON;
USE SCHEMA DATA;
USE WAREHOUSE COMPUTE_WH;


-- 1. INTERNAL STAGE (shared for all Analytics Track CSVs)
CREATE STAGE IF NOT EXISTS HACKATHON.DATA.CSV_STAGE
    COMMENT = 'Internal stage for Analytics Track CSV uploads';


-- 2. CSV FILE FORMAT
CREATE OR REPLACE FILE FORMAT HACKATHON.DATA.CSV_FORMAT
    TYPE                      = 'CSV'
    SKIP_HEADER               = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF               = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL       = TRUE
    DATE_FORMAT               = 'YYYY-MM-DD';


-- 3. HR_ATTRITION TABLE (matches hr_attrition.csv schema exactly)
CREATE OR REPLACE TABLE HACKATHON.DATA.HR_ATTRITION (
    EMPLOYEE_ID            VARCHAR(20),
    EMPLOYEE_NAME          VARCHAR(100),
    DEPARTMENT             VARCHAR(50),
    MANAGER_ID             VARCHAR(20),
    TENURE_YEARS           FLOAT,
    PERFORMANCE_RATING     INTEGER,
    SALARY_BAND            VARCHAR(10),
    MONTHS_SINCE_PROMOTION INTEGER,
    ENGAGEMENT_SCORE       FLOAT,
    TEAM_SIZE              INTEGER,
    HIRE_DATE              DATE,
    ATTRITION_RISK_LABEL   VARCHAR(10)
);


-- 4. LOAD DATA FROM STAGE
COPY INTO HACKATHON.DATA.HR_ATTRITION
FROM @HACKATHON.DATA.CSV_STAGE/hr_attrition.csv
FILE_FORMAT = (FORMAT_NAME = 'HACKATHON.DATA.CSV_FORMAT')
ON_ERROR    = 'CONTINUE';


-- 5. VERIFY LOAD + BASIC PROFILE
SELECT
    COUNT(*)                              AS TOTAL_EMPLOYEES,
    COUNT(DISTINCT DEPARTMENT)            AS TOTAL_DEPARTMENTS,
    ROUND(AVG(TENURE_YEARS), 1)          AS AVG_TENURE_YEARS,
    ROUND(AVG(PERFORMANCE_RATING), 2)    AS AVG_PERFORMANCE_RATING,
    ROUND(AVG(MONTHS_SINCE_PROMOTION), 1) AS AVG_MONTHS_SINCE_PROMO
FROM HACKATHON.DATA.HR_ATTRITION;
