-- ============================================================================
-- DB2 DDL for the transaction subsystem (side-by-side with the VSAM files).
-- Migrated to Delta tables in the target Databricks schema.
-- ============================================================================

CREATE TABLE ACCTDB.ACCOUNT (
    ACCT_NO        CHAR(10)       NOT NULL,
    ACCT_NAME      VARCHAR(30),
    ACCT_TYPE      CHAR(2),
    ACCT_BALANCE   DECIMAL(13,2),
    ACCT_STATUS    CHAR(1),
    ACCT_LAST_DATE CHAR(8),
    PRIMARY KEY (ACCT_NO)
) IN DATABASE ACCTDB;

CREATE TABLE ACCTDB.TRANSACTION (
    TRAN_ID        INTEGER        NOT NULL,
    ACCT_NO        CHAR(10)       NOT NULL,
    TRAN_TYPE      CHAR(2),
    TRAN_AMOUNT    DECIMAL(11,2),
    TRAN_DATE      CHAR(8),
    PRIMARY KEY (TRAN_ID),
    FOREIGN KEY (ACCT_NO) REFERENCES ACCTDB.ACCOUNT (ACCT_NO)
) IN DATABASE ACCTDB;

-- Online balance inquiry used by the CICS transaction INQTRAN.
CREATE VIEW ACCTDB.V_ACCT_BALANCE AS
    SELECT ACCT_NO, ACCT_BALANCE, ACCT_STATUS
    FROM   ACCTDB.ACCOUNT;
