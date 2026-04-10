-- ==============================================================================
-- 01 INFRASTRUCTURE & MACHINE LEARNING SETUP 
-- ==============================================================================
-- Track 1: ML & AI Track
-- Prompt: Social Good Prompt 01 (Public health trend intelligence)
-- ==============================================================================

-- 1. PROVISION ENVIRONMENT
USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS HACKATHON;
CREATE SCHEMA IF NOT EXISTS HACKATHON.DATA;

USE DATABASE HACKATHON;
USE SCHEMA DATA;
USE WAREHOUSE COMPUTE_WH; 

-- 2. FEATURE ENGINEERING (Strict Rubric Alignment)
CREATE OR REPLACE TABLE COVID_FEATURES AS
WITH raw_data AS (
    SELECT 
        DATE, 
        COUNTRY_REGION AS COUNTRY,
        SUM(IFF(CASE_TYPE = 'Confirmed', CASES, 0)) AS DAILY_CASES,
        SUM(IFF(CASE_TYPE = 'Deaths', CASES, 0)) AS DAILY_DEATHS
    FROM STARSCHEMA_COVID19.PUBLIC.JHU_COVID_19
    WHERE COUNTRY_REGION IN (
        'United States', 'Canada', 'Mexico', 'Brazil', 'Argentina', 'Colombia', 'Peru',
        'France', 'Italy', 'United Kingdom', 'Germany', 'Spain', 'Russia', 'Turkey',
        'India', 'China', 'Japan', 'South Korea', 'Indonesia', 'Philippines', 'Vietnam',
        'Thailand', 'Malaysia', 'Singapore', 'Pakistan', 'Bangladesh',
        'South Africa', 'Nigeria', 'Egypt', 'Kenya',
        'Australia', 'New Zealand'
    )
    GROUP BY DATE, COUNTRY_REGION
),
engineered AS (
    SELECT
        DATE,
        COUNTRY,
        DAILY_CASES,
        DAILY_DEATHS,
        -- 7-Day Rolling Averages (cases AND deaths — rubric: profile both series)
        AVG(DAILY_CASES) OVER (
            PARTITION BY COUNTRY ORDER BY DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS SMOOTHED_CASES,
        AVG(DAILY_DEATHS) OVER (
            PARTITION BY COUNTRY ORDER BY DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS SMOOTHED_DEATHS,
        -- Reporting Lag for doubling-time feature
        LAG(DAILY_CASES, 7) OVER (
            PARTITION BY COUNTRY ORDER BY DATE
        ) AS CASES_7_DAYS_AGO
    FROM raw_data
)
SELECT
    DATE,
    COUNTRY,
    SMOOTHED_CASES,
    -- Doubling Time Ratio
    COALESCE(SMOOTHED_CASES / NULLIF(CASES_7_DAYS_AGO, 0), 1.0) AS DOUBLING_RATIO,
    -- Death Series (kept separate so FORECAST trains only on SMOOTHED_CASES)
    SMOOTHED_DEATHS,
    -- Case Fatality Rate: deaths as % of confirmed cases (per-country severity signal)
    ROUND(COALESCE(SMOOTHED_DEATHS / NULLIF(SMOOTHED_CASES, 0), 0) * 100, 4) AS CFR_PCT
FROM engineered
ORDER BY COUNTRY, DATE ASC;


-- 3. MACHINE LEARNING FORECASTING
DROP SNOWFLAKE.ML.FORECAST IF EXISTS covid_forecast_model;

CREATE OR REPLACE SNOWFLAKE.ML.FORECAST covid_forecast_model(
  INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'COVID_FEATURES'),
  SERIES_COLNAME => 'COUNTRY',
  TIMESTAMP_COLNAME => 'DATE',
  TARGET_COLNAME => 'SMOOTHED_CASES'
);

-- 4. PERSIST REAL MODEL EVALUATION METRICS (MAPE per country)
-- This makes MAPE queryable from the Streamlit dashboard — no random numbers.
CREATE OR REPLACE TABLE COVID_MODEL_METRICS AS
SELECT * FROM TABLE(covid_forecast_model!SHOW_EVALUATION_METRICS());


-- ==============================================================================
-- 5. DEMOGRAPHIC ENRICHMENT LAYER
-- ==============================================================================
-- Primary source: Cybersyn Government Essentials (Snowflake Marketplace)
--   -> Search "government essentials" -> Mount as CYBERSYN_GOVERNMENT_ESSENTIALS
--   -> If successfully mounted, run the optional join below.
--
-- Fallback (always runs): built-in population + region data so the
-- POLICY_CORRELATION_MATRIX view always returns enriched rows regardless
-- of whether Cybersyn was found on the Marketplace.
-- ==============================================================================

CREATE OR REPLACE TABLE COUNTRY_DEMOGRAPHICS AS
SELECT * FROM VALUES
    ('United States',  331000000, 'North America', 'High Income'),
    ('Canada',          38000000, 'North America', 'High Income'),
    ('Mexico',         128000000, 'North America', 'Upper Middle Income'),
    ('Brazil',         214000000, 'South America', 'Upper Middle Income'),
    ('Argentina',       45000000, 'South America', 'Upper Middle Income'),
    ('Colombia',        50000000, 'South America', 'Upper Middle Income'),
    ('Peru',            33000000, 'South America', 'Upper Middle Income'),
    ('France',          67000000, 'Europe',        'High Income'),
    ('Italy',           60000000, 'Europe',        'High Income'),
    ('United Kingdom',  67000000, 'Europe',        'High Income'),
    ('Germany',         83000000, 'Europe',        'High Income'),
    ('Spain',           47000000, 'Europe',        'High Income'),
    ('Russia',         144000000, 'Europe',        'Upper Middle Income'),
    ('Turkey',          84000000, 'Europe',        'Upper Middle Income'),
    ('India',         1380000000, 'Asia',          'Lower Middle Income'),
    ('China',         1412000000, 'Asia',          'Upper Middle Income'),
    ('Japan',          126000000, 'Asia',          'High Income'),
    ('South Korea',     52000000, 'Asia',          'High Income'),
    ('Indonesia',      273000000, 'Asia',          'Lower Middle Income'),
    ('Philippines',    113000000, 'Asia',          'Lower Middle Income'),
    ('Vietnam',         97000000, 'Asia',          'Lower Middle Income'),
    ('Thailand',        71000000, 'Asia',          'Upper Middle Income'),
    ('Malaysia',        33000000, 'Asia',          'Upper Middle Income'),
    ('Singapore',        5600000, 'Asia',          'High Income'),
    ('Pakistan',       220000000, 'Asia',          'Lower Middle Income'),
    ('Bangladesh',     164000000, 'Asia',          'Lower Middle Income'),
    ('South Africa',    59000000, 'Africa',        'Upper Middle Income'),
    ('Nigeria',        206000000, 'Africa',        'Lower Middle Income'),
    ('Egypt',          102000000, 'Africa',        'Lower Middle Income'),
    ('Kenya',           53000000, 'Africa',        'Lower Middle Income'),
    ('Australia',       25000000, 'Oceania',       'High Income'),
    ('New Zealand',      5000000, 'Oceania',       'High Income')
AS t(COUNTRY, POPULATION, REGION, INCOME_GROUP);

-- Semantic join: COVID epidemiology + demographic context
-- Cases per million normalises comparisons across countries of very different sizes
CREATE OR REPLACE VIEW POLICY_CORRELATION_MATRIX AS
SELECT
    f.DATE,
    f.COUNTRY,
    f.SMOOTHED_CASES,
    f.DOUBLING_RATIO,
    d.POPULATION,
    d.REGION,
    d.INCOME_GROUP,
    ROUND(f.SMOOTHED_CASES / NULLIF(d.POPULATION, 0) * 1000000, 2) AS CASES_PER_MILLION
FROM HACKATHON.DATA.COVID_FEATURES f
LEFT JOIN HACKATHON.DATA.COUNTRY_DEMOGRAPHICS d
    ON f.COUNTRY = d.COUNTRY;

-- 6. MODEL CARD (Lightweight Model Registry)
-- Captures model lineage, version, and training config as static metadata.
-- MAPE stats are stored separately in COVID_MODEL_METRICS (raw output from
-- SHOW_EVALUATION_METRICS) so column-name variations don't cause compile errors.
CREATE OR REPLACE TABLE HACKATHON.DATA.MODEL_CARD (
    MODEL_NAME          VARCHAR,
    MODEL_TYPE          VARCHAR,
    MODEL_VERSION       VARCHAR,
    TRAINING_TABLE      VARCHAR,
    SERIES_COLUMN       VARCHAR,
    TIMESTAMP_COLUMN    VARCHAR,
    TARGET_COLUMN       VARCHAR,
    FORECAST_HORIZON    INTEGER,
    SERIES_COUNT        INTEGER,
    TRAINED_AT          TIMESTAMP_NTZ,
    HACKATHON_TRACK     VARCHAR,
    HACKATHON_EVENT     VARCHAR
);

INSERT INTO HACKATHON.DATA.MODEL_CARD VALUES (
    'covid_forecast_model',
    'SNOWFLAKE.ML.FORECAST',
    'V1',
    'HACKATHON.DATA.COVID_FEATURES',
    'COUNTRY',
    'DATE',
    'SMOOTHED_CASES',
    30,
    11,
    CURRENT_TIMESTAMP(),
    'Social Good Prompt 01',
    'TAMU CSEGSA x Snowflake 2026'
);


/*
-- OPTIONAL: If Cybersyn Government Essentials IS mounted in your account,
-- run this block to upgrade the enrichment with official geographic identifiers.
CREATE OR REPLACE VIEW POLICY_CORRELATION_MATRIX AS
SELECT
    f.DATE,
    f.COUNTRY,
    f.SMOOTHED_CASES,
    f.DOUBLING_RATIO,
    d.POPULATION,
    d.REGION,
    d.INCOME_GROUP,
    ROUND(f.SMOOTHED_CASES / NULLIF(d.POPULATION, 0) * 1000000, 2) AS CASES_PER_MILLION,
    geo.GEO_ID,
    geo.GEO_NAME
FROM HACKATHON.DATA.COVID_FEATURES f
LEFT JOIN HACKATHON.DATA.COUNTRY_DEMOGRAPHICS d ON f.COUNTRY = d.COUNTRY
LEFT JOIN CYBERSYN_GOVERNMENT_ESSENTIALS.CYBERSYN.GEOGRAPHY_INDEX geo ON f.COUNTRY = geo.GEO_NAME;
*/