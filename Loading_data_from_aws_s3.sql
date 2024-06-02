-- Create a database called healthcare_db
CREATE DATABASE IF NOT EXISTS healthcare_db;
USE DATABASE healthcare_db;

-- Create 4 schemas
CREATE OR REPLACE SCHEMA land; -- source stage
CREATE OR REPLACE SCHEMA raw;
CREATE OR REPLACE SCHEMA clean; -- curation and de-duplication
CREATE OR REPLACE SCHEMA consumption; --dimension tables, transactional fact, periodic fact

-- Create storage integration object
CREATE OR REPLACE  STORAGE integration s3_int_ht
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE 
STORAGE_AWS_ROLE_ARN    = 'arn:aws:iam::<=======>'
STORAGE_ALLOWED_LOCATIONS = ('s3://tb-snowflake/raw/') -- bucket on aws s3
COMMENT = 'Integration with aws s3 buckets';
 
--Describe integration object to fetch external_id and to be used in s3
DESC integration s3_int_ht;

-- Create schema for file format
CREATE SCHEMA IF NOT EXISTS healthcare_db.file_formats;
CREATE SCHEMA IF NOT EXISTS healthcare_db.external_stages;

-- Create file format object
CREATE OR REPLACE FILE FORMAT healthcare_db.file_formats.csv_fileformat
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1
EMPTY_FIELD_AS_NULL = TRUE;    
    
-- Create stage object with integration object & file format object
CREATE OR REPLACE STAGE healthcare_db.external_stages.aws_s3_csv
URL = 's3://tb-snowflake/raw/'
STORAGE_INTEGRATION = s3_int_ht
FILE_FORMAT = healthcare_db.file_formats.csv_fileformat ;
    
-- Listing files under your AWS bucket
LIST @healthcare_db.external_stages.aws_s3_csv;

-- Create a table called HEALTHCARE
CREATE OR REPLACE TABLE healthcare_db.land.HEALTHCARE(
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

-- Use Copy command to load the file
COPY INTO healthcare_db.land.HEALTHCARE
FROM @healthcare_db.external_stages.aws_s3_csv
PATTERN = '.*healthcare.*'
ON_ERROR = 'CONTINUE';    
 
-- Validate the data
SELECT COUNT(*) FROM healthcare_db.land.HEALTHCARE;

SELECT * FROM healthcare_db.land.HEALTHCARE
LIMIT 10;

-- Unloading data into an S3 bucket
CREATE OR REPLACE STAGE my_ext_unload_stage URL='s3://tb-snowflake/raw/'
STORAGE_INTEGRATION = s3_int_ht
FILE_FORMAT = healthcare_db.file_formats.csv_fileformat;

COPY INTO @my_ext_unload_stage/healthcare_saved 
FROM healthcare_db.land.HEALTHCARE;

