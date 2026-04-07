# threat-detection-blueprint - VoltDB Partitioned Client

This is the VoltDB component of the threat and fraud detection Blueprint solution presented in this article on Medium:
https://medium.com/@ppine7all/real-time-fraud-and-geographic-threat-detection-on-google-cloud-platform-c2091ff4daaa, where the full use case as well as the GCP-based architecture are described and a working proof-of-concept solution is demonstrated.

This GIT repo contains a VoltDB client application that demonstrates **real-time threat detection** combining fraud transaction detection with CIDR subnet-based threat detection, using partitioned tables, co-located stored procedures, and TIME_WINDOW materialized views.

## Overview

The application assesses two categories of threats:
1. **Fraud detection** — per-account velocity and spending rules on financial transactions
2. **Malicious actor detection** — identifies requestors issuing transactions from the same CIDR subnet at high rates (configurable prefix length, default /24)

Every transaction tracks the **source IP** of the requestor and is recorded in the `TRANSACTIONS` table for export to BigQuery where further analytics are done. 
Blocked transactions are recorded with `ACCEPTED=0`, `REASON`, and `RULE_NAME`.
Accepted transactions are recorded with `ACCEPTED=1`.

## VoltDB Schema

The schema is defined in [`src/main/resources/voltdb-ddl.sql`](src/main/resources/voltdb-ddl.sql) and loaded automatically at startup by `VoltDBSetup.initSchemaIfNeeded()` — if the schema is already deployed, only stored procedures are redeployed.

**Tables:**

| Table | Purpose |
|-------|---------|
| `ACCOUNTS` | Account reference data (balance, daily limit, enabled flag) |
| `TRANSACTIONS` | Every processed transaction with accept/reject outcome, rule name, and source IP |
| `MERCHANTS` | Small replicated merchant lookup table |
| `SUBNET_REQUESTS` | Tracks requests per CIDR subnet for rate-based threat detection |

Materialized views, threat detection rules, and stored procedures are described in the sections below.

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
| Dataform scripts | `dataform/` | Dataform transformations for IP geolocation enrichment in BigQuery |
| Seed data runner | `PubSubPublishRunner.java` | Publishes ~467 seed transactions covering all attack scenarios |

To run the seed data publisher against a live GCP project:
```bash
mvn failsafe:integration-test -Dit.test=PubSubPublishRunner
```

## Dataform — BigQuery Transformations

The `src/main/resources/dataform/` directory contains [Dataform](https://cloud.google.com/dataform) scripts that transform raw transaction data in BigQuery by enriching it with IP geolocation from MaxMind GeoIP2 City data.

| File | Type | Description |
|------|------|-------------|
| `sources.js` | Source declarations | Declares external BigQuery tables (`geo_ip_blocks_staging`, `geo_locations`, `threat_transactions`) as Dataform sources |
| `geo_ip_blocks.sqlx` | Table | Converts MaxMind CIDR network ranges into INT64 start/end pairs for efficient IP range lookups |
| `threat_transactions_geo.sqlx` | View | Joins transactions with geo IP blocks and geo locations to produce transactions enriched with country, region, city, coordinates, and proxy/anycast flags |

These scripts use the `<your-dataset>` placeholder for the schema name. Run `./configure.sh` to substitute it with your actual BigQuery dataset (see [Configure Project Properties](#configure-project-properties)).

## Prerequisites

- Java 17+
- Maven 3.6+
- Docker (running)
- VoltDB license — request a free Developer Edition license at [voltactivedata.com/build-with-volt](https://www.voltactivedata.com/build-with-volt/)

## Build and Test

Tests default to **VoltDB Developer Edition** (`voltactivedata/volt-developer-edition`). A free Developer Edition license can be requested at [voltactivedata.com/build-with-volt](https://www.voltactivedata.com/build-with-volt/). To use Enterprise Edition instead, edit `src/test/resources/test.properties`:
```properties
voltdb.image.name=voltdb/voltdb-enterprise
voltdb.image.version=14.3.1
```
Set the license path (either Developer or Enterprise):
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

## Deployment on GCP

This section describes how to deploy the threat detection app as a containerized application on Google Cloud, connecting to a VoltDB cluster running on GKE.

### Architecture

```
Cloud Run (threat-detection-app)
    │
    │  VPC Connector (private network)
    │
    ├──► VoltDB on GKE (internal LB, port 21211)
    │
    └──► Google Cloud PubSub (threat-transactions topic)
              │
              └──► BigQuery (<your-dataset>.threat_transactions)
```

The app runs on **Cloud Run** and reaches VoltDB through a **VPC connector** and an **internal load balancer** — no public VoltDB endpoint is exposed.

### Prerequisites

- `gcloud` CLI installed and authenticated (`gcloud auth login`)
- A GKE cluster with VoltDB running in the `default` VPC (see https://docs.voltactivedata.com/KubernetesAdmin/IntroChap.php)
- PubSub topic `threat-transactions` exists in the project
- Java 17+ and Maven 3.6+ (for local builds only)

### Configure Project Properties

Project files use `<your-gcp-project>` and `<your-dataset>` placeholders. Before building or deploying, replace them with your actual values:

1. Copy the template and fill in your values:
```bash
cp src/main/resources/application.properties.template \
   src/main/resources/application.properties
```

2. Edit `src/main/resources/application.properties` with your GCP project and BigQuery dataset:
```properties
gcp.project=my-gcp-project
bq.dataset=my_dataset
gcs.bucket=my-gcs-bucket
```

3. Run the configure script to substitute placeholders across all project files (`Dockerfile`, `deploy-cloudrun.sh`, `bigquery_ddl.sql`, `dataform/`):
```bash
./configure.sh
```

> **Note:** `application.properties` is git-ignored — it contains your environment-specific values and should not be committed. The checked-in `application.properties.template` serves as the reference.

### One-Time Setup

These steps only need to be done once per project.

#### 1. Create an Artifact Registry repository

This repository stores container images for all blueprint apps.

```bash
gcloud artifacts repositories create volt-blueprint-apps \
  --project=<your-gcp-project> \
  --location=us-west3 \
  --repository-format=docker \
  --description="VoltDB blueprint application images"
```

Verify:
```bash
gcloud artifacts repositories describe volt-blueprint-apps \
  --project=<your-gcp-project> --location=us-west3
```

#### 2. Expose VoltDB with an internal load balancer

Cloud Run cannot reach Kubernetes ClusterIP services directly. Create an internal load balancer for VoltDB's client port:

```bash
kubectl expose service db1-voltdb-cluster-client \
  --name=db1-voltdb-internal-lb \
  --type=LoadBalancer \
  --port=21211 \
  --target-port=21211 \
  -n default

kubectl annotate svc db1-voltdb-internal-lb \
  networking.gke.io/load-balancer-type=Internal -n default
```

Wait for the internal IP to be assigned:
```bash
kubectl get svc db1-voltdb-internal-lb -n default -w
```

The `EXTERNAL-IP` column will show an internal VPC IP (e.g., `10.180.0.39`). Note this IP — you'll use it as the `VOLTDB_HOST` parameter.

Verify connectivity from within the cluster:
```bash
kubectl run test-conn --rm -it --image=busybox --restart=Never -- \
  sh -c "nc -zv 10.180.0.39 21211"
```

#### 3. Create a VPC connector

The VPC connector allows Cloud Run to route traffic into the GKE VPC.

```bash
gcloud compute networks vpc-access connectors create threat-detect-vpc \
  --project=<your-gcp-project> \
  --region=us-west3 \
  --network=default \
  --range=10.9.0.0/28
```

Verify:
```bash
gcloud compute networks vpc-access connectors describe threat-detect-vpc \
  --project=<your-gcp-project> --region=us-west3 \
  --format="value(state)"
# Expected: READY
```

### Build and Deploy

#### Using the deploy script

The `deploy-cloudrun.sh` script builds the container image via Cloud Build and deploys to Cloud Run:

```bash
./deploy-cloudrun.sh <VOLTDB_HOST> [VOLTDB_PORT]

# Example:
./deploy-cloudrun.sh 10.180.0.39
./deploy-cloudrun.sh 10.180.0.39 21211
```

The script will:
1. Create the VPC connector if it doesn't exist
2. Build the Docker image using Cloud Build and push to Artifact Registry
3. Deploy to Cloud Run with VPC connectivity and environment variables

#### Manual step-by-step

**Build and push the image:**
```bash
gcloud builds submit \
  --project=<your-gcp-project> \
  --region=us-west3 \
  --tag=us-west3-docker.pkg.dev/<your-gcp-project>/volt-blueprint-apps/threat-detection-app:latest \
  .
```

Verify the image was pushed:
```bash
gcloud artifacts docker images list \
  us-west3-docker.pkg.dev/<your-gcp-project>/volt-blueprint-apps \
  --format="table(package,version)"
```

**Deploy as a Cloud Run Job** (run-to-completion):
```bash
gcloud run jobs create threat-detection-job \
  --project=<your-gcp-project> \
  --region=us-west3 \
  --image=us-west3-docker.pkg.dev/<your-gcp-project>/volt-blueprint-apps/threat-detection-app:latest \
  --memory=1Gi \
  --cpu=1 \
  --max-retries=0 \
  --task-timeout=300s \
  --vpc-connector=threat-detect-vpc \
  --vpc-egress=private-ranges-only \
  --set-env-vars="VOLTDB_HOST=10.180.0.39,VOLTDB_PORT=21211,GCP_PROJECT=<your-gcp-project>,PUBSUB_TOPIC=threat-transactions,SEND_TO_PUBSUB=true,CIDR_PREFIX=24"
```

**Execute the job:**
```bash
gcloud run jobs execute threat-detection-job \
  --project=<your-gcp-project> --region=us-west3
```

### Verify the Deployment

**Check job execution status:**
```bash
gcloud run jobs executions list \
  --project=<your-gcp-project> --region=us-west3 \
  --job=threat-detection-job \
  --format="table(name,status.conditions[0].type,status.conditions[0].status)"
```

**View job logs:**
```bash
gcloud logging read \
  'resource.type="cloud_run_job" AND resource.labels.job_name="threat-detection-job"' \
  --project=<your-gcp-project> \
  --limit=50 --freshness=15m \
  --format="value(textPayload)"
```

**Verify PubSub messages were published** (if `SEND_TO_PUBSUB=true`):
```bash
# Create a temporary subscription to check messages
gcloud pubsub subscriptions create threat-verify \
  --topic=threat-transactions --project=<your-gcp-project>

gcloud pubsub subscriptions pull threat-verify \
  --project=<your-gcp-project> --limit=5 --auto-ack

# Clean up
gcloud pubsub subscriptions delete threat-verify --project=<your-gcp-project>
```

**Verify data in VoltDB** (from a pod in the cluster):
```bash
kubectl exec -it db1-voltdb-cluster-0 -n default -- \
  sqlcmd --query="SELECT COUNT(*) FROM TRANSACTIONS; SELECT COUNT(*) FROM TRANSACTIONS WHERE ACCEPTED=0;"
```

### Configuration Reference

All configuration is passed via environment variables in the Cloud Run deployment:

| Variable | Default | Description |
|----------|---------|-------------|
| `VOLTDB_HOST` | `localhost` | VoltDB internal LB IP address |
| `VOLTDB_PORT` | `21211` | VoltDB client port |
| `GCP_PROJECT` | `<your-gcp-project>` | GCP project ID for PubSub |
| `PUBSUB_TOPIC` | `threat-transactions` | PubSub topic name |
| `SEND_TO_PUBSUB` | `true` | Enable/disable PubSub publishing |
| `CIDR_PREFIX` | `24` | CIDR prefix length for subnet grouping (0-32) |

To update configuration on an existing job:
```bash
gcloud run jobs update threat-detection-job \
  --project=<your-gcp-project> --region=us-west3 \
  --set-env-vars="VOLTDB_HOST=<new-ip>,VOLTDB_PORT=21211,..."
```

### Cleanup

```bash
# Delete the Cloud Run job
gcloud run jobs delete threat-detection-job \
  --project=<your-gcp-project> --region=us-west3 --quiet

# Delete the VPC connector
gcloud compute networks vpc-access connectors delete threat-detect-vpc \
  --project=<your-gcp-project> --region=us-west3 --quiet

# Delete the internal load balancer
kubectl delete svc db1-voltdb-internal-lb -n default

# Delete the container image
gcloud artifacts docker images delete \
  us-west3-docker.pkg.dev/<your-gcp-project>/volt-blueprint-apps/threat-detection-app \
  --project=<your-gcp-project> --quiet
```

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
│   ├── voltdb-ddl.sql                 # Tables, views, partitions, procedures
│   ├── remove_db.sql                  # Drop everything (dependency order)
│   ├── application.properties         # Runtime config (GCP, VoltDB)
│   ├── avro/
│   │   └── transaction.avsc           # Avro schema for transaction events
│   ├── bigquery_ddl.sql               # BigQuery table definitions
│   ├── dataform/
│   │   ├── sources.js                 # Dataform source declarations
│   │   ├── geo_ip_blocks.sqlx         # CIDR-to-INT64 range table
│   │   └── threat_transactions_geo.sqlx # Geo-enriched transactions view
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