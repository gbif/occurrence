#!/bin/zsh
#
# Generate an SQL download query using all available occurrence table columns.
#

select_columns=$(curl -Ss --fail https://api.gbif-uat.org/v1/occurrence/download/describe/sql | jq -r '.fields[].name' | \
sed 's/^/ \\"/; s/$/\\"/' | tr '\n' , | sed 's/,$//')

cat > test-download-query-sql-all-columns.json <<EOF
{
  "sendNotification": true,
  "creator": "MattBlissett",
  "notification_address": ["mblissett@gbif.org"],
  "format": "SQL_TSV_ZIP",
  "sql": "SELECT $select_columns FROM occurrence LIMIT 10000"
}
EOF
