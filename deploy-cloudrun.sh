#!/bin/bash
# Deploy threat-detection-app to Google Cloud Run with VPC connector
#
# Prerequisites:
#   - gcloud CLI authenticated (gcloud auth login)
#   - GKE cluster and VoltDB running in the same VPC
#   - PubSub topic "threat-transactions" exists in the project
#   - Artifact Registry repo "volt-blueprint-apps" exists in us-west3
#     (create once: gcloud artifacts repositories create volt-blueprint-apps \
#       --project=<your-gcp-project> --location=us-west3 --repository-format=docker)
#
# Usage:
#   ./deploy-cloudrun.sh <VOLTDB_HOST> [VOLTDB_PORT]
#
# Example:
#   ./deploy-cloudrun.sh 10.0.81.152
#   ./deploy-cloudrun.sh 10.0.81.152 21211

set -euo pipefail

# --- Configuration ---
PROJECT_ID="<your-gcp-project>"
REGION="us-west3"
NETWORK="default"
SERVICE_NAME="threat-detection-app"
REPO_NAME="volt-blueprint-apps"
IMAGE_NAME="threat-detection-app"
IMAGE_TAG="latest"
VPC_CONNECTOR="threat-detect-vpc"
VPC_CONNECTOR_RANGE="10.9.0.0/28"

VOLTDB_HOST="${1:?Usage: $0 <VOLTDB_HOST> [VOLTDB_PORT]}"
VOLTDB_PORT="${2:-21211}"

IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${IMAGE_NAME}:${IMAGE_TAG}"

# --- Step 1: Create VPC connector (if it doesn't exist) ---
echo "==> Ensuring VPC connector exists..."
if ! gcloud compute networks vpc-access connectors describe "${VPC_CONNECTOR}" \
  --project="${PROJECT_ID}" --region="${REGION}" >/dev/null 2>&1; then
  echo "    Creating VPC connector ${VPC_CONNECTOR}..."
  gcloud compute networks vpc-access connectors create "${VPC_CONNECTOR}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --network="${NETWORK}" \
    --range="${VPC_CONNECTOR_RANGE}"
  echo "    Waiting for connector to become ready..."
  gcloud compute networks vpc-access connectors describe "${VPC_CONNECTOR}" \
    --project="${PROJECT_ID}" --region="${REGION}" \
    --format="value(state)"
else
  echo "    VPC connector already exists."
fi

# --- Step 2: Build and push image using Cloud Build ---
echo "==> Building and pushing image with Cloud Build..."
gcloud builds submit \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --tag="${IMAGE_URI}" \
  .

# --- Step 3: Deploy to Cloud Run with VPC connector ---
echo "==> Deploying to Cloud Run..."
gcloud run deploy "${SERVICE_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --image="${IMAGE_URI}" \
  --platform=managed \
  --no-allow-unauthenticated \
  --memory=1Gi \
  --cpu=1 \
  --vpc-connector="${VPC_CONNECTOR}" \
  --vpc-egress=private-ranges-only \
  --set-env-vars="VOLTDB_HOST=${VOLTDB_HOST},VOLTDB_PORT=${VOLTDB_PORT},GCP_PROJECT=${PROJECT_ID},PUBSUB_TOPIC=threat-transactions,SEND_TO_PUBSUB=true,CIDR_PREFIX=24"

echo ""
echo "==> Deployed! Service URL:"
gcloud run services describe "${SERVICE_NAME}" \
  --project="${PROJECT_ID}" --region="${REGION}" \
  --format="value(status.url)"
