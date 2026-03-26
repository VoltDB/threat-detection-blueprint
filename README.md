# threat-detection-blueprint - VoltDB Partitioned Client

This is the VoltDB component of the threat and fraud detection Blueprint solution presented in this article: TBD, where the full use case as well as the GCP-based architecture are described and a working proof-of-concept solution is demonstrated.

This GIT repo contains a VoltDB client application that demonstrates **real-time threat detection** combining fraud transaction detection with CIDR subnet-based threat detection, using partitioned tables, co-located stored procedures, and TIME_WINDOW materialized views.

## Overview

The application assesses two categories of threats:
1. **Fraud detection** — per-account velocity and spending rules on financial transactions
2. **Malicious actor detection** — identifies requestors issuing transactions from the same CIDR subnet at high rates (configurable prefix length, default /24)

Every transaction tracks the **source IP** of the requestor and is recorded in the `TRANSACTIONS` table for export to BigQuery where further analytics are done. Blocked transactions are recorded with `ACCEPTED=0`, `REASON`, and `RULE_NAME`.

## Partitioning Strategy

The schema uses two partition keys for different concerns:

| Table | Partition Column | Strategy |
|-------|------------------|----------|
| ACCOUNTS | ACCOUNT_ID | Primary entity |
| TRANSACTIONS | ACCOUNT_ID | Co-located with ACCOUNTS |
| MERCHANTS | _(replicated)_ | Small reference table |
| SUBNET_REQUESTS | SUBNET | Separate partition space for CIDR subnet tracking |

### Why two partition keys?
- **ACCOUNT_ID** — fraud rules are per-account (transaction velocity, spending). Co-locating ACCOUNTS and TRANSACTIONS enables single-partition atomic fraud detection.
- **SUBNET** — subnet rate detection counts requests across all accounts from the same CIDR block (e.g., /24, /28). The CIDR prefix length is configurable via `-Dcidr.prefix`. Partitioning by the computed network address enables single-partition counting.

### Two-Step Transaction Flow

Fraud rules and subnet counting live on different partitions (ACCOUNT_ID vs SUBNET), so each incoming request is processed in two single-partition calls:

1. **`RecordSubnetRequest`** (partitioned on SUBNET) — inserts the request into `SUBNET_REQUESTS` and returns the current count from the `REQUESTS_PER_SUBNET` materialized view. This call is **not** part of the fraud transaction — it only tracks subnet activity.
2. **`ProcessTransaction`** (partitioned on ACCOUNT_ID) — a single ACID transaction that validates the account and merchant, inserts the transaction as accepted (so materialized views include it), checks all three threat rules (including the subnet count passed from step 1), and if any rule triggers, updates the transaction to `ACCEPTED=0` with the reason. If all rules pass, the account balance is updated. All rule evaluation and the accept/reject decision happen atomically inside this transaction.

### Materialized Views (TIME_WINDOW)

| View | Window | Purpose |
|------|--------|---------|
| TXN_SUMMARY_30SEC | 30 seconds | Detect transaction velocity fraud (>5 txns) |
| TXN_SUMMARY_1MIN | 1 minute | Detect spending spikes (>$5,000) |
| REQUESTS_PER_SUBNET | 5 seconds | Detect subnet-based attack patterns (>100 requests) |

## Threat Detection Rules

| Rule | Name | Condition | Action |
|------|------|-----------|--------|
| Transaction velocity | `TXN_SUMMARY_30SEC` | >5 transactions from same account in 30 seconds | Reject |
| Spending spike | `TXN_SUMMARY_1MIN` | >$5,000 total spending from same account in 1 minute | Reject |
| Subnet rate | `SUBNET_RATE` | >100 requests from same CIDR subnet in 5 seconds | Reject |

All rules are evaluated inside `ProcessTransaction`. Rejected transactions are recorded in `TRANSACTIONS` with `ACCEPTED=0`, along with the triggering `RULE_NAME` and `REASON`.

## Procedures

| Procedure | Type | Description |
|-----------|------|-------------|
| ProcessTransaction | Single-partition (Java, multi-step) | Atomic fraud + subnet threat detection |
| RecordSubnetRequest | Single-partition (Java) | Track CIDR subnet request count |
| UpsertAccount | Single-partition (DDL) | Add/update account |
| GetAccount | Single-partition (DDL) | Get account by ID |
| UpsertMerchant | Multi-partition (DDL) | Add/update merchant |
| GetTransactionsByAccount | Single-partition (DDL) | Transaction history for an account |
| SearchBlockedByRule | Multi-partition (DDL) | Search blocked transactions by rule name |
| SearchBlockedByIp | Multi-partition (DDL) | Search blocked transactions by source IP |

## GCP Integration — PubSub and BigQuery

Transaction events are published to **Google Cloud PubSub** in real time for downstream analytics in BigQuery.

| Component | File | Purpose |
|-----------|------|---------|
| Publisher | `TransactionPublisher.java` | Publishes each transaction as JSON to PubSub with denormalized account/merchant names |
| Config | `application.properties` | GCP project, PubSub topic, `sendToPubSub` toggle |
| Event schema | `avro/transaction.avsc` | Avro schema defining the transaction event format |
| BigQuery DDL | `bigquery_ddl.sql` | BigQuery table definitions (standard and Iceberg/BigLake) |
| Seed data runner | `PubSubPublishRunner.java` | Publishes ~467 seed transactions covering all attack scenarios |

To run the seed data publisher against a live GCP project:
```bash
mvn failsafe:integration-test -Dit.test=PubSubPublishRunner
```

## Prerequisites

- Java 17+
- Maven 3.6+
- Docker (running)
- VoltDB Enterprise license (optional — tests default to the free Developer Edition)

## Build and Test

Tests default to **VoltDB Developer Edition** (`voltactivedata/volt-developer-edition`), which does not require a license. To use Enterprise Edition instead, edit `src/test/resources/test.properties`:
```properties
voltdb.image.name=voltdb/voltdb-enterprise
voltdb.image.version=14.3.1
```
and set the license path:
```bash
export VOLTDB_LICENSE=~/voltdb-license.xml
```

```bash
# 1. Ensure Docker is running
docker info

# 2. Build
mvn clean package -DskipTests

# 3. Run tests
mvn verify
```

## Running the Application

```bash
# Run against a VoltDB instance (default: localhost:21211)
java -cp "target/threat-detection-blueprint-1.0.jar:target/lib/*" com.example.voltdb.ThreatDetectionApp

# Specify host and port
java -cp "target/threat-detection-blueprint-1.0.jar:target/lib/*" com.example.voltdb.ThreatDetectionApp <host> <port>
```

The application will:
1. Connect to VoltDB and deploy schema if needed
2. Load account and merchant reference data from CSV
3. Process a sample transaction through the two-step threat detection flow
4. Query transaction history and search blocked transactions
5. Clean up and disconnect

## Project Structure

```
threat-detection-blueprint/
├── pom.xml
├── README.md
├── src/main/java/com/example/voltdb/
│   ├── ThreatDetectionApp.java        # Main client app (two-step flow)
│   ├── TransactionPublisher.java      # PubSub JSON publisher with caching
│   ├── CidrUtils.java                # CIDR subnet extraction utility
│   ├── VoltDBSetup.java              # Idempotent schema deployment
│   ├── CsvDataLoader.java            # CSV data loading utility
│   └── procedures/
│       ├── ProcessTransaction.java    # Atomic fraud + subnet detection
│       └── RecordSubnetRequest.java   # Subnet request counting
├── src/main/resources/
│   ├── ddl.sql                        # Tables, views, partitions, procedures
│   ├── remove_db.sql                  # Drop everything (dependency order)
│   ├── application.properties         # Runtime config (GCP, VoltDB)
│   ├── avro/
│   │   └── transaction.avsc           # Avro schema for transaction events
│   ├── bigquery_ddl.sql               # BigQuery table definitions
│   └── data/
│       ├── accounts.csv               # Account reference data
│       └── merchants.csv              # Merchant reference data
├── src/test/java/com/example/voltdb/
│   ├── IntegrationTestBase.java       # Test infrastructure (testcontainer/external)
│   ├── ThreatDetectionIT.java         # Integration tests
│   └── PubSubPublishRunner.java       # Seed data publisher for GCP
└── src/test/resources/
    ├── test.properties                # Test config (Docker image, test mode)
    └── data/
        └── seed_transactions.csv      # ~467 seed transactions with attack scenarios
```