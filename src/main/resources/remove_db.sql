-- VoltDB Remove Schema -- drops all objects in dependency order

-- Step 1: Drop Java class procedures
DROP PROCEDURE com.example.voltdb.procedures.ProcessTransaction IF EXISTS;
DROP PROCEDURE com.example.voltdb.procedures.RecordSubnetRequest IF EXISTS;

-- Step 2: Drop DDL-defined procedures
DROP PROCEDURE UpsertAccount IF EXISTS;
DROP PROCEDURE GetAccount IF EXISTS;
DROP PROCEDURE UpsertMerchant IF EXISTS;
DROP PROCEDURE GetTransactionsByAccount IF EXISTS;
DROP PROCEDURE SearchBlockedByRule IF EXISTS;
DROP PROCEDURE SearchBlockedByIp IF EXISTS;

-- Step 3: Drop views (depend on TRANSACTIONS and SUBNET_REQUESTS)
DROP VIEW TXN_SUMMARY_30SEC IF EXISTS;
DROP VIEW TXN_SUMMARY_1MIN IF EXISTS;
DROP VIEW REQUESTS_PER_SUBNET IF EXISTS;

-- Step 4: Drop co-located tables
DROP TABLE TRANSACTIONS IF EXISTS;

-- Step 5: Drop subnet tracking
DROP TABLE SUBNET_REQUESTS IF EXISTS;

-- Step 6: Drop replicated tables
DROP TABLE MERCHANTS IF EXISTS;

-- Step 7: Drop primary table last
DROP TABLE ACCOUNTS IF EXISTS;
