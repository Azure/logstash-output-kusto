#!/usr/bin/env bash
# Creates the Kusto table and mappings for manual testing.
# Two mappings are created with a constant `ingestion_mode` field to distinguish
# which mode (buffered vs file) produced each row.
#
# Requires: az cli logged in, jq installed.
# Usage: ./manual-test/setup-table.sh

ENGINE_URL="https://<your-cluster-name>.dev.kusto.windows.net"
DATABASE="<your-database-name>"
TABLE="ManualTestLogs"

# Run a management command via Kusto REST API.
# Uses jq to safely build the JSON body, handling nested quotes in KQL commands.
run_mgmt() {
  local cmd="$1"
  echo "  Running: $cmd"
  local body
  body=$(jq -n --arg db "$DATABASE" --arg csl "$cmd" '{db: $db, csl: $csl}')
  az rest --method post \
    --url "${ENGINE_URL}/v1/rest/mgmt" \
    --resource "$ENGINE_URL" \
    --body "$body" \
    --headers "Content-Type=application/json" \
    -o none 2>&1 | grep -v "^$"
}

echo "=== Setting up Kusto table: $TABLE in $DATABASE ==="

run_mgmt ".drop table $TABLE ifexists"

run_mgmt ".create table $TABLE (timestamp: datetime, ip: string, method: string, url: string, status: int, size: int, raw: string, ingestion_mode: string)"

# Mapping for buffered mode — ConstValue stamps the ingestion source
BUFFERED_MAPPING='[{"column":"timestamp","path":"$.timestamp","datatype":"datetime"},{"column":"ip","path":"$.ip","datatype":"string"},{"column":"method","path":"$.method","datatype":"string"},{"column":"url","path":"$.url","datatype":"string"},{"column":"status","path":"$.status","datatype":"int"},{"column":"size","path":"$.size","datatype":"int"},{"column":"raw","path":"$.raw","datatype":"string"},{"column":"ingestion_mode","properties":{"ConstValue":"buffered"},"datatype":"string"}]'

# Mapping for file mode — ConstValue stamps the ingestion source
FILE_MAPPING='[{"column":"timestamp","path":"$.timestamp","datatype":"datetime"},{"column":"ip","path":"$.ip","datatype":"string"},{"column":"method","path":"$.method","datatype":"string"},{"column":"url","path":"$.url","datatype":"string"},{"column":"status","path":"$.status","datatype":"int"},{"column":"size","path":"$.size","datatype":"int"},{"column":"raw","path":"$.raw","datatype":"string"},{"column":"ingestion_mode","properties":{"ConstValue":"file"},"datatype":"string"}]'

run_mgmt ".create table $TABLE ingestion json mapping 'buffered_mapping' '${BUFFERED_MAPPING}'"
run_mgmt ".create table $TABLE ingestion json mapping 'file_mapping' '${FILE_MAPPING}'"

run_mgmt ".alter table $TABLE policy ingestionbatching @'{\"MaximumBatchingTimeSpan\":\"00:00:30\", \"MaximumNumberOfItems\": 10, \"MaximumRawDataSizeMB\": 100}'"

echo ""
echo "=== Done. Table '$TABLE' is ready with two mappings. ==="
echo "Query: $TABLE | summarize count() by ingestion_mode"
