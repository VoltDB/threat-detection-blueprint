#!/bin/bash
# Reads src/main/resources/application.properties and substitutes all
# <placeholder> tokens in project files with the real values.
#
# Usage:
#   ./configure.sh
#
# Prerequisites:
#   Copy application.properties.template to application.properties and
#   fill in your own values:
#     cp src/main/resources/application.properties.template \
#        src/main/resources/application.properties

set -euo pipefail

PROPS_FILE="src/main/resources/application.properties"

if [ ! -f "$PROPS_FILE" ]; then
  echo "ERROR: $PROPS_FILE not found."
  echo "Copy the template first:"
  echo "  cp src/main/resources/application.properties.template $PROPS_FILE"
  exit 1
fi

# --- Read properties into variables ---
get_prop() {
  grep "^$1=" "$PROPS_FILE" | cut -d'=' -f2-
}

GCP_PROJECT=$(get_prop "gcp.project")
BQ_DATASET=$(get_prop "bq.dataset")
GCS_BUCKET=$(get_prop "gcs.bucket")

if [ -z "$GCP_PROJECT" ] || [ -z "$BQ_DATASET" ] || [ -z "$GCS_BUCKET" ]; then
  echo "ERROR: gcp.project, bq.dataset, and gcs.bucket must be set in $PROPS_FILE"
  exit 1
fi

# --- Files to configure ---
FILES=(
  "Dockerfile"
  "deploy-cloudrun.sh"
  "src/main/resources/bigquery_ddl.sql"
  "src/main/resources/dataform/sources.js"
  "src/main/resources/dataform/geo_ip_blocks.sqlx"
  "src/main/resources/dataform/threat_transactions_geo.sqlx"
)

echo "Configuring project with:"
echo "  gcp.project = $GCP_PROJECT"
echo "  bq.dataset  = $BQ_DATASET"
echo "  gcs.bucket  = $GCS_BUCKET"
echo ""

for f in "${FILES[@]}"; do
  if [ ! -f "$f" ]; then
    echo "  SKIP  $f (not found)"
    continue
  fi
  if grep -q '<your-' "$f" 2>/dev/null; then
    sed -i.bak \
      -e "s|<your-gcp-project>|${GCP_PROJECT}|g" \
      -e "s|<your-dataset>|${BQ_DATASET}|g" \
      -e "s|<your-gcs-bucket>|${GCS_BUCKET}|g" \
      "$f"
    rm -f "${f}.bak"
    echo "  OK    $f"
  else
    echo "  ---   $f (no placeholders found)"
  fi
done

echo ""
echo "Done. The project is ready to build and deploy."