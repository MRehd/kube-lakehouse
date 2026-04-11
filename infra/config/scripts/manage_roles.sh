#!/bin/sh
# Polaris principal role (RBAC) management script
# Placeholders: {{POLARIS_URL}}, {{CLIENT_ID}}, {{CLIENT_SECRET}}, {{ROLE_CALLS}}

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

# Create a principal role if it doesn't exist
# Args: role_name
create_role() {
  local name="$1"
  
  # Check if role exists
  exists=$(curl -sf -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $TOKEN" \
    "$POLARIS_URL/api/management/v1/principal-roles/$name")
  
  if [ "$exists" = "200" ]; then
    echo "Principal role $name already exists"
  else
    http_code=$(curl -s -o /tmp/role_resp.json -w "%{http_code}" -X POST \
      "$POLARIS_URL/api/management/v1/principal-roles" \
      -H "Authorization: Bearer $TOKEN" \
      -H 'Content-Type: application/json' \
      -d "{\"principalRole\": {\"name\": \"$name\"}}")
    if [ "$http_code" = "201" ] || [ "$http_code" = "200" ]; then
      echo "Created principal role $name"
    else
      echo "ERROR: Failed to create principal role $name (HTTP $http_code)"
      cat /tmp/role_resp.json
      exit 1
    fi
  fi
}

# Grant a catalog role to a principal role if not already granted
# Args: principal_role catalog_name catalog_role
grant_catalog_role() {
  local principal_role="$1"
  local catalog="$2"
  local catalog_role="$3"
  
  # Check if already granted
  granted=$(curl -sf -H "Authorization: Bearer $TOKEN" \
    "$POLARIS_URL/api/management/v1/principal-roles/$principal_role/catalog-roles/$catalog" \
    | grep -q "\"name\":\"$catalog_role\"" && echo "yes" || echo "no")
  
  if [ "$granted" = "yes" ]; then
    echo "Catalog role $catalog_role already granted to $principal_role on $catalog"
  else
    http_code=$(curl -s -o /tmp/grant_resp.json -w "%{http_code}" -X PUT \
      "$POLARIS_URL/api/management/v1/principal-roles/$principal_role/catalog-roles/$catalog" \
      -H "Authorization: Bearer $TOKEN" \
      -H 'Content-Type: application/json' \
      -d "{\"catalogRole\": {\"name\": \"$catalog_role\"}}")
    if [ "$http_code" = "201" ] || [ "$http_code" = "200" ]; then
      echo "Granted catalog role $catalog_role to $principal_role on $catalog"
    else
      echo "ERROR: Failed to grant $catalog_role to $principal_role on $catalog (HTTP $http_code)"
      cat /tmp/grant_resp.json
      exit 1
    fi
  fi
}

{{ROLE_CALLS}}

echo "Done"
