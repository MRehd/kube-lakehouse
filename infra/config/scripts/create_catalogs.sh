#!/bin/sh
# Polaris catalog creation script

POLARIS_URL='{{POLARIS_URL}}'

# Acquire OAuth token with retry
for i in $(seq 1 30); do
  TOKEN=$(curl -sf -X POST "$POLARIS_URL/api/catalog/v1/oauth/tokens" \
    -d 'grant_type=client_credentials&client_id={{CLIENT_ID}}&client_secret={{CLIENT_SECRET}}&scope=PRINCIPAL_ROLE:ALL' \
    | sed 's/.*"access_token":"\([^"]*\)".*/\1/')
  [ -n "$TOKEN" ] && break
  sleep 2
done
[ -z "$TOKEN" ] && echo "Failed to get token" && exit 1

# Create a catalog via REST API
# Args: name bucket endpoint access_key secret_key region path_style base_location
create_catalog() {
  local name="$1"
  local bucket="$2"
  local endpoint="$3"
  local access_key="$4"
  local secret_key="$5"
  local region="$6"
  local path_style="$7"
  local base_location="$8"

  payload=$(cat <<EOF
{
  "name": "$name",
  "type": "INTERNAL",
  "properties": {"default-base-location": "$base_location"},
  "storageConfigInfo": {
    "storageType": "S3",
    "allowedLocations": ["s3://$bucket/"],
    "endpoint": "$endpoint",
    "region": "$region",
    "pathStyleAccess": $path_style,
    "stsUnavailable": true
  }
}
EOF
)

  http_code=$(curl -s -o /tmp/catalog_resp.json -w "%{http_code}" -X POST "$POLARIS_URL/api/management/v1/catalogs" \
    -H "Authorization: Bearer $TOKEN" \
    -H 'Content-Type: application/json' \
    -d "$payload")

  if [ "$http_code" = "201" ] || [ "$http_code" = "200" ]; then
    echo "Catalog $name created successfully"
  elif [ "$http_code" = "409" ]; then
    echo "Catalog $name already exists, updating storageConfigInfo..."
    http_code2=$(curl -s -o /tmp/catalog_upd_resp.json -w "%{http_code}" -X PUT "$POLARIS_URL/api/management/v1/catalogs/$name" \
      -H "Authorization: Bearer $TOKEN" \
      -H 'Content-Type: application/json' \
      -d "$payload")
    if [ "$http_code2" = "200" ] || [ "$http_code2" = "204" ]; then
      echo "Catalog $name updated successfully"
    else
      echo "WARNING: Update returned HTTP $http_code2 (may not support PUT), skipping"
      cat /tmp/catalog_upd_resp.json
    fi
  else
    echo "ERROR: Failed to create catalog $name (HTTP $http_code)"
    cat /tmp/catalog_resp.json
    exit 1
  fi
}

{{CATALOG_CALLS}}

echo "Done"
