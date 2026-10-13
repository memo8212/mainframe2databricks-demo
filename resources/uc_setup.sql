-- Unity Catalog setup for the mainframe migration POC.
-- Run once (needs CREATE CATALOG) before deploying the bundle.
-- Mirrors the legacy libraries/files: ACCTDB (DB2) + VSAM ACCTMAST.

CREATE CATALOG IF NOT EXISTS mf_poc;
CREATE SCHEMA IF NOT EXISTS mf_poc.accounting COMMENT 'migrated accounting tables';
CREATE SCHEMA IF NOT EXISTS mf_poc.landing    COMMENT 'VSAM/DB2 unload (parquet bridge)';

-- Landing volume holds the unloaded extracts (accounts/transactions parquet,
-- ACCTMAST.parquet) written by the legacy unload step.
CREATE VOLUME IF NOT EXISTS mf_poc.landing.bridge
  COMMENT 'VSAM/DB2 Parquet bridge';

-- Target tables (managed Delta). Bronze -> Silver -> Gold.
CREATE TABLE IF NOT EXISTS mf_poc.accounting.bronze_account
  (ACCT_NO STRING NOT NULL, ACCT_NAME STRING, ACCT_TYPE STRING,
   ACCT_BALANCE DECIMAL(13,2), ACCT_STATUS STRING, ACCT_LAST_DATE STRING,
   _ingested_at TIMESTAMP, _source STRING);

CREATE TABLE IF NOT EXISTS mf_poc.accounting.bronze_transaction
  (TRAN_ID BIGINT, ACCT_NO STRING, TRAN_TYPE STRING,
   TRAN_AMOUNT DECIMAL(11,2), TRAN_DATE STRING,
   _ingested_at TIMESTAMP, _source STRING);

CREATE TABLE IF NOT EXISTS mf_poc.accounting.silver_account
  (ACCT_NO STRING NOT NULL, ACCT_NAME STRING, ACCT_TYPE STRING,
   ACCT_BALANCE DECIMAL(13,2), ACCT_STATUS STRING, ACCT_LAST_DATE STRING,
   CONSTRAINT pk_silver_account PRIMARY KEY (ACCT_NO))
  COMMENT 'VSAM ACCTMAST (KSDS) -> keyed Delta table';

-- Consumers read Gold only.
-- GRANT USAGE  ON CATALOG mf_poc TO `analysts`;
-- GRANT SELECT ON TABLE   mf_poc.accounting.gold_account_summary TO `analysts`;

SELECT 'mf_poc setup complete' AS status;
