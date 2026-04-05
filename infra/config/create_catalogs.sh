#!/bin/sh
# Polaris catalog creation script
# Placeholders: {{POLARIS_URL}}, {{CLIENT_ID}}, {{CLIENT_SECRET}}, {{CATALOG_CALLS}}

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
    "s3.endpoint": "$endpoint",
    "s3.access-key-id": "$access_key",
    "s3.secret-access-key": "$secret_key",
    "s3.region": "$region",
    "s3.path-style-access": "$path_style"
  }
}
EOF
)

  curl -sf -X POST "$POLARIS_URL/api/management/v1/catalogs" \
    -H "Authorization: Bearer $TOKEN" \
    -H 'Content-Type: application/json' \
    -d "$payload" || echo "Catalog $name may already exist"
}

{{CATALOG_CALLS}}

echo "Done"
