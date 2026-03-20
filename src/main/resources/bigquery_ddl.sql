-- BigQuery DDL for threat detection analytics
-- Dataset: marina_test

DROP TABLE `marina_test.threat_transactions`;

CREATE TABLE IF NOT EXISTS `marina_test.threat_transactions` (
  txn_id INT64 NOT NULL,
  account_id INT64 NOT NULL,
  account_name STRING,
  txn_time TIMESTAMP NOT NULL,
  merchant_id INT64,
  merchant_name STRING,
  merchant_category STRING,
  amount FLOAT64,
  device_id STRING,
  source_ip STRING,
  accepted INT64,
  reason STRING,
  rule_name STRING
);

-- Iceberg table on GCS (BigLake managed, Parquet format)
-- Prerequisites:
--   1. Create a Cloud Resource connection in BigQuery (e.g., us-east1.threat-to-iceberg)
--   2. Grant the connection's service account Storage Object Admin on the GCS bucket
--   3. Update the connection name and storage_uri below

CREATE TABLE IF NOT EXISTS `marina_test.threat_transactions_iceberg` (
  txn_id INT64 NOT NULL,
  account_id INT64 NOT NULL,
  account_name STRING,
  txn_time TIMESTAMP NOT NULL,
  merchant_id INT64,
  merchant_name STRING,
  merchant_category STRING,
  amount FLOAT64,
  device_id STRING,
  source_ip STRING,
  accepted INT64,
  reason STRING,
  rule_name STRING
)
WITH CONNECTION `us-east1.threat-tx-to-iceberg`
OPTIONS (
  file_format = 'PARQUET',
  table_format = 'ICEBERG',
  storage_uri = 'gs://mpopova_volt/threat_transactions/'
);
