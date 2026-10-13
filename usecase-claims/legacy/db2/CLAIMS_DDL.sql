-- ============================================================================
-- DB2 DDL for the claims subsystem (relational copy of the VSAM/sequential
-- files). Migrated to Delta tables in the target Databricks schema.
-- ============================================================================

CREATE TABLE HLTHDB.MEMBER (
    MEMBER_ID       CHAR(8)        NOT NULL,
    MEMBER_NAME     VARCHAR(30),
    PLAN_TYPE       CHAR(6),
    DEDUCTIBLE_REM  DECIMAL(9,2),
    PLAN_YEAR       CHAR(4),
    PRIMARY KEY (MEMBER_ID)
) IN DATABASE HLTHDB;

CREATE TABLE HLTHDB.CLAIM (
    CLAIM_ID        CHAR(10)       NOT NULL,
    MEMBER_ID       CHAR(8)        NOT NULL,
    SERVICE_CODE    CHAR(3),
    CLAIM_AMOUNT    DECIMAL(11,2),
    SERVICE_DATE    CHAR(8),
    PRIMARY KEY (CLAIM_ID),
    FOREIGN KEY (MEMBER_ID) REFERENCES HLTHDB.MEMBER (MEMBER_ID)
) IN DATABASE HLTHDB;

CREATE TABLE HLTHDB.CLAIM_ADJUDICATED (
    CLAIM_ID        CHAR(10)       NOT NULL,
    MEMBER_ID       CHAR(8),
    STATUS          CHAR(8),
    PAID_AMOUNT     DECIMAL(11,2),
    PRIMARY KEY (CLAIM_ID)
) IN DATABASE HLTHDB;

-- Reference data used by adjudication (coverage + coinsurance).
CREATE TABLE HLTHDB.SERVICE_CODE (
    SERVICE_CODE    CHAR(3)        NOT NULL,
    DESCRIPTION     VARCHAR(40),
    COVERED         CHAR(1),
    PRIMARY KEY (SERVICE_CODE)
) IN DATABASE HLTHDB;
