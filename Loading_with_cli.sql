-- Use SnowSQL CLI
-- snowsql -a <account_identifier> -u <username>
-- password required
-- Create a database called healthcare_db
CREATE DATABASE IF NOT EXISTS healthcare_db;
USE DATABASE healthcare_db;

-- Create a table called HEALTHCARE
CREATE OR REPLACE TABLE HEALTHCARE(
    average_covered_charges NUMBER(38,2),
    average_total_payments NUMBER(38,2),
    total_discharges  NUMBER(38,2),
    degree  NUMBER(38,2),
    hs_or_higher  NUMBER(38,2),
    total_payments  VARCHAR(255),
    reimbursement  VARCHAR(255),
    total_covered_charges  VARCHAR(255),
    region_provider_name VARCHAR(255),
    reimbursement_percentage  NUMBER(38,2),
    description  VARCHAR(255),
    referral_region VARCHAR(255),
    income_per_capita  NUMBER(38,2),
    median_earnings_bach NUMBER(38,2),
    median_earnings_graduate NUMBER(38,2),
    median_earnings_hs_grad  NUMBER(38,2),
    median_earnings_hs  NUMBER(38,2),
    median_family_income  NUMBER(38,2),
    number_of_records  NUMBER(38,2),
    population_over_25  NUMBER(38,2),
    provider_city VARCHAR(255),
    provider_id  INT,
    provider_name  VARCHAR(255),
    provider_state  VARCHAR(255),
    provider_address VARCHAR(255),
    provider_zipcode  INT,
    npi  INT
);

-- Create a file format
CREATE OR REPLACE FILE FORMAT csv_fileformat
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1
EMPTY_FIELD_AS_NULL = TRUE;    

-- Create an internal stage
CREATE STAGE TBSF_INTERNAL_STAGE
COMMENT = 'Snowflake Managed Named Internal Stage';

PUT file:///Users/barry/snowflake-labs/datasets_used/healthcare.csv @TBSF_INTERNAL_STAGE;

-- Use Copy command to load the file
COPY INTO HEALTHCARE FROM @TBSF_INTERNAL_STAGE ON_ERROR = 'CONTINUE';

-- Validate the data
SELECT COUNT(*) FROM HEALTHCARE;



