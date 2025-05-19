--=========================================
-- 1ST - Loading data using Web Interface
--=========================================


-- Creating a testing database
CREATE DATABASE TEST_DB;
USE DATABASE TEST_DB;

-- customer table
CREATE TABLE CUSTOMER_DETAILS (
    first_name STRING,
    last_name STRING,
    address STRING,
    city STRING,
    state STRING
);

-- table should be empty
SELECT * FROM CUSTOMER_DETAILS;

-- Now Load data into CUSTOMER_DETAILS using UI

--===================================
-- 2ND - Loading data using SnowSQL
--===================================

snow connection add myconn \
  --account <--> \
  --user <--> \
  --authenticator <--> \
  --role SYSADMIN \
  --database MY_DB \
  --schema PUBLIC \
  --warehouse COMPUTE_WH

-- login snowsql
snowsql

-- Create file format first
CREATE OR REPLACE FILE FORMAT PIPE_FORMAT_CLI
	type = 'CSV'
	field_delimiter = '|'
	skip_header = 1;
	
-- Then Create a stage 
CREATE OR REPLACE STAGE PIP_CLI_STAGE
	file_format = PIPE_FORMAT_CLI;

-- put data into stage
put
file://C:\<-->
@PIP_CLI_STAGE auto_compress=true;

-- list stage to see how many files are there
list @PIP_CLI_STAGE;

-- Resume warehouse, in case the auto-resume feature is OFF
ALTER WAREHOUSE <name> RESUME;

-- copy data from stage to table
COPY INTO CUSTOMER_DETAILS
	FROM @PIP_CLI_STAGE
	file_format = (format_name = PIPE_FORMAT_CLI)
	on_error = 'skip_file';
	
-- We can also give a COPY command with the pattern  if  your stage contains multiple  files
COPY INTO mycsvtable
	FROM @mycsvstage
	file_format = (format_name = PIP_FORMAT_CLI)
	pattern = '*.contain[1-5].csv.gz'
	on_error = 'skip_file';


--=========================================
-- 3RD - Loading data using Cloud Provider
--=========================================

-- tesla table
CREATE OR REPLACE TABLE TESLA_STOCKS(
    date DATE,
    open_value DOUBLE,
    high_vlaue DOUBLE,
    low_value DOUBLE,
    close_vlaue DOUBLE,
    adj_close_value DOUBLE,
    volume BIGINT
);

-- should be empty
SELECT * FROM TESLA_STOCKS;

-- external stage creation
CREATE OR REPLACE STAGE BULK_COPY_TESLA_STOCKS
URL = "<-->" --s3 bucket url
CREDENTIALS = (AWS_KEY_ID='<-->', -- IAM USER Credentials
AWS_SECRET_KEY='<-->');

-- list stage
LIST @BULK_COPY_TESLA_STOCKS;

-- FILE FORMAT

CREATE OR REPLACE FILE FORMAT CSV_FILE_FORMAT
	type = 'CSV'
	field_delimiter = ','
	skip_header = 1;

-- copy data from stage to table
COPY INTO TESLA_STOCKS
	FROM @BULK_COPY_TESLA_STOCKS
	file_format = (format_name = CSV_FILE_FORMAT)
    on_error = 'skip_file';

-- data should be there
SELECT * FROM TESLA_STOCKS;



-----------------------------
-- 4TH - Storage Integration
-----------------------------

-- giving privileges
USE ROLE ACCOUNTADMIN;
GRANT CREATE INTEGRATION ON ACCOUNT TO SYSADMIN;
USE ROLE SYSADMIN;

-- storage integration
CREATE OR REPLACE STORAGE INTEGRATION S3_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = '<-->'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('<-->');

-- giving privileges (not required if using ACCOUNTADMIN)
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON INTEGRATION S3_INTEGRATION TO ROLE SYSADMIN;
USE ROLE SYSADMIN;

-- valdating integration
DESC INTEGRATION S3_INTEGRATION;

-- creating stage
CREATE OR REPLACE STAGE S3_INTEGRATEION_BULK_COPY_TESLA_STOCKS
  STORAGE_INTEGRATION = S3_INTEGRATION
  URL = '<-->'
  FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER=',', SKIP_HEADER=1);

-- validating integration
LIST @S3_INTEGRATEION_BULK_COPY_TESLA_STOCKS;

-- Need to give the snowflake ARN & ID

-- Making sure the table is empty
TRUNCATE TABLE TESLA_STOCKS;

-- to confirm the table has no data
SELECT * FROM TESLA_STOCKS;

-- Copy data using integration
COPY INTO TESLA_STOCKS FROM @S3_INTEGRATEION_BULK_COPY_TESLA_STOCKS;

-- data should be there
SELECT * FROM TESLA_STOCKS;


--=====================================
-- 5th - Loading data using Snow Pipe
--=====================================

-- 1. Stage the data
-- 2. Test the copy command
-- 3. Create pipe
-- 4. Configure cloud event / call snow pipe rest API

-- truncating data again
TRUNCATE TABLE TESLA_STOCKS;

-- dropping previously create integration & stage
DROP STORAGE INTEGRATION S3_INTEGRATION;
DROP STAGE S3_INTEGRATEION_BULK_COPY_TESLA_STOCKS;

-- HELP: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration
-- Step 1: Configure access permissions (policy) for the S3 bucket
-- Step 2: Create the IAM Role in AWS and attach above policy you created.

-- Step 3: Create a Cloud Storage Integration in Snowflake
CREATE OR REPLACE STORAGE INTEGRATION S3_TESLA_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = '<-->'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('<-->');

-- Step 4: Retrieve the AWS IAM User for your Snowflake Account
DESC INTEGRATION S3_TESLA_INTEGRATION;

-- Step 5: Grant the IAM User Permissions to Access Bucket Objects
-- STORAGE_AWS_ROLE_ARN 
-- STORAGE_AWS_EXTERNAL_ID

-- Step 6: Create file format for external stage
CREATE OR REPLACE FILE FORMAT S3_TESLA_STAGE_FORMAT
    TYPE= 'CSV'
    FIELD_DELIMITER=','
    SKIP_HEADER=1;

-- Step 6: Create an external stage using file format createbavove
CREATE STAGE S3_TESLA_STAGE
  STORAGE_INTEGRATION = S3_TESLA_INTEGRATION
  URL = '<-->'
  FILE_FORMAT = S3_TESLA_STAGE_FORMAT;

-- Step 7: Create a COPY Into Command
-- HELP: https://docs.snowflake.com/en/user-guide/data-load-s3-copy

COPY INTO TESLA_STOCKS FROM @S3_TESLA_STAGE;

-- validating & dropping again for pip
SELECT * FROM TESLA_STOCKS;

TRUNCATE TABLE TESLA_STOCKS;

--  Creating Pipe 
CREATE OR REPLACE PIPE S3_TESLA_PIPE AUTO_INGEST=TRUE AS
COPY INTO TESLA_STOCKS FROM @S3_TESLA_STAGE;

-- Configure cloud event / call snow pipe rest API (S3_TESLA_EVENT_NOTICTATION)
SHOW PIPES;

-- Data should be there auotmatically
SELECT * FROM TESLA_STOCKS;

-- DROPPING PIPE
DROP PIPE S3_TESLA_PIPE;