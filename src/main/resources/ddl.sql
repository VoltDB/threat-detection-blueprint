-- VoltDB DDL Schema for Threat Detection
-- Combines fraud transaction detection with CIDR subnet-based threat detection.

-- ============================================
-- Accounts (partitioned on ACCOUNT_ID)
-- ============================================
CREATE TABLE ACCOUNTS (
    ACCOUNT_ID bigint NOT NULL,
    ENABLED tinyint DEFAULT 1 NOT NULL,
    BALANCE decimal DEFAULT 0,
    DAILY_LIMIT decimal DEFAULT 5000,
    NAME varchar(50),
    EMAIL varchar(50),
    PRIMARY KEY (ACCOUNT_ID)
);
PARTITION TABLE ACCOUNTS ON COLUMN ACCOUNT_ID;

-- ============================================
-- Transactions (partitioned on ACCOUNT_ID, co-located with ACCOUNTS)
-- Tracks the IP of the requestor via SOURCE_IP.
-- ============================================
CREATE TABLE TRANSACTIONS (
    TXN_ID bigint NOT NULL,
    ACCOUNT_ID bigint NOT NULL,
    TXN_TIME timestamp NOT NULL,
    MERCHANT_ID integer,
    AMOUNT decimal,
    DEVICE_ID varchar(32),
    SOURCE_IP varchar(45),
    ACCEPTED tinyint,
    REASON varchar(100),
    RULE_NAME varchar(30),
    PRIMARY KEY (TXN_ID, ACCOUNT_ID)
);
PARTITION TABLE TRANSACTIONS ON COLUMN ACCOUNT_ID;
CREATE INDEX TXN_TIME_IDX ON TRANSACTIONS (ACCOUNT_ID, TXN_TIME);

-- ============================================
-- Merchants (replicated — small reference table)
-- ============================================
CREATE TABLE MERCHANTS (
    MERCHANT_ID integer NOT NULL,
    NAME varchar(50),
    CATEGORY varchar(20),
    PRIMARY KEY (MERCHANT_ID)
);

-- ============================================
-- Subnet request tracking (partitioned on SUBNET)
-- Used to count requests from the same /24 CIDR subnet.
-- Counts are recorded outside the transaction; the rule check is inside.
-- ============================================
CREATE TABLE SUBNET_REQUESTS (
    REQUEST_ID bigint NOT NULL,
    SUBNET varchar(39) NOT NULL,
    SOURCE_IP varchar(45) NOT NULL,
    REQUEST_TIME timestamp NOT NULL,
    PRIMARY KEY (REQUEST_ID, SUBNET)
);
PARTITION TABLE SUBNET_REQUESTS ON COLUMN SUBNET;

-- ============================================
-- Materialized views for fraud detection rules
-- ============================================

-- TXN_SUMMARY_30SEC: reject if >5 transactions in 30 seconds
CREATE VIEW TXN_SUMMARY_30SEC (
    ACCOUNT_ID,
    WINDOW_30SEC,
    TXN_COUNT,
    TOTAL_SPENT
) AS
    SELECT ACCOUNT_ID,
           TIME_WINDOW(SECOND, 30, TXN_TIME) AS WINDOW_30SEC,
           COUNT(*) AS TXN_COUNT,
           SUM(AMOUNT) AS TOTAL_SPENT
    FROM TRANSACTIONS
    GROUP BY ACCOUNT_ID, TIME_WINDOW(SECOND, 30, TXN_TIME);

-- TXN_SUMMARY_1MIN: reject if >$5,000 spent in 1 minute
CREATE VIEW TXN_SUMMARY_1MIN (
    ACCOUNT_ID,
    WINDOW_1MIN,
    TXN_COUNT,
    TOTAL_SPENT
) AS
    SELECT ACCOUNT_ID,
           TIME_WINDOW(SECOND, 60, TXN_TIME) AS WINDOW_1MIN,
           COUNT(*) AS TXN_COUNT,
           SUM(AMOUNT) AS TOTAL_SPENT
    FROM TRANSACTIONS
    GROUP BY ACCOUNT_ID, TIME_WINDOW(SECOND, 60, TXN_TIME);

-- REQUESTS_PER_SUBNET: count requests per CIDR subnet in 5-second window
CREATE VIEW REQUESTS_PER_SUBNET (
    SUBNET,
    WINDOW_5SEC,
    REQUEST_COUNT
) AS
    SELECT SUBNET,
           TIME_WINDOW(SECOND, 5, REQUEST_TIME) AS WINDOW_5SEC,
           COUNT(*) AS REQUEST_COUNT
    FROM SUBNET_REQUESTS
    GROUP BY SUBNET, TIME_WINDOW(SECOND, 5, REQUEST_TIME);

-- ============================================
-- DDL-defined procedures
-- ============================================

DROP PROCEDURE UpsertAccount IF EXISTS;
CREATE PROCEDURE UpsertAccount
    PARTITION ON TABLE ACCOUNTS COLUMN ACCOUNT_ID
    AS UPSERT INTO ACCOUNTS (ACCOUNT_ID, ENABLED, BALANCE, DAILY_LIMIT, NAME, EMAIL)
       VALUES (?, ?, ?, ?, ?, ?);

DROP PROCEDURE GetAccount IF EXISTS;
CREATE PROCEDURE GetAccount
    PARTITION ON TABLE ACCOUNTS COLUMN ACCOUNT_ID
    AS SELECT * FROM ACCOUNTS WHERE ACCOUNT_ID = ?;

DROP PROCEDURE UpsertMerchant IF EXISTS;
CREATE PROCEDURE UpsertMerchant
    AS UPSERT INTO MERCHANTS (MERCHANT_ID, NAME, CATEGORY) VALUES (?, ?, ?);

DROP PROCEDURE GetTransactionsByAccount IF EXISTS;
CREATE PROCEDURE GetTransactionsByAccount
    PARTITION ON TABLE TRANSACTIONS COLUMN ACCOUNT_ID
    AS SELECT * FROM TRANSACTIONS WHERE ACCOUNT_ID = ? ORDER BY TXN_TIME DESC;

DROP PROCEDURE SearchBlockedByRule IF EXISTS;
CREATE PROCEDURE SearchBlockedByRule
    AS SELECT * FROM TRANSACTIONS WHERE ACCEPTED = 0 AND RULE_NAME = ? ORDER BY TXN_TIME DESC;

DROP PROCEDURE SearchBlockedByIp IF EXISTS;
CREATE PROCEDURE SearchBlockedByIp
    AS SELECT * FROM TRANSACTIONS WHERE ACCEPTED = 0 AND SOURCE_IP = ? ORDER BY TXN_TIME DESC;

-- ============================================
-- Java class procedures
-- ============================================

DROP PROCEDURE com.example.voltdb.procedures.ProcessTransaction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE TRANSACTIONS COLUMN ACCOUNT_ID
    FROM CLASS com.example.voltdb.procedures.ProcessTransaction;

DROP PROCEDURE com.example.voltdb.procedures.RecordSubnetRequest IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE SUBNET_REQUESTS COLUMN SUBNET
    FROM CLASS com.example.voltdb.procedures.RecordSubnetRequest;