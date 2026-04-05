#!/bin/sh
# Polaris principal and RBAC management script
# Placeholders: {{POLARIS_URL}}, {{CLIENT_ID}}, {{CLIENT_SECRET}}, {{PRINCIPAL_CALLS}}

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

# Create a principal if it doesn't exist
# Args: name
create_principal() {
  local name="$1"
  
  # Check if principal exists
  exists=$(curl -sf -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $TOKEN" \
    "$POLARIS_URL/api/management/v1/principals/$name")
  
  if [ "$exists" = "200" ]; then
    echo "Principal $name already exists"
  else
    curl -sf -X POST "$POLARIS_URL/api/management/v1/principals" \
      -H "Authorization: Bearer $TOKEN" \
      -H 'Content-Type: application/json' \
      -d "{\"name\": \"$name\", \"type\": \"SERVICE\"}" \
      && echo "Created principal $name" \
      || echo "Failed to create principal $name"
  fi
}

# Assign a principal role to a principal if not already assigned
# Args: principal_name role_name
assign_role() {
  local principal="$1"
  local role="$2"
  
  # Check if already assigned by listing principal's roles
  assigned=$(curl -sf -H "Authorization: Bearer $TOKEN" \
    "$POLARIS_URL/api/management/v1/principals/$principal/principal-roles" \
    | grep -q "\"name\":\"$role\"" && echo "yes" || echo "no")
  
  if [ "$assigned" = "yes" ]; then
    echo "Role $role already assigned to $principal"
  else
    curl -sf -X PUT "$POLARIS_URL/api/management/v1/principals/$principal/principal-roles" \
      -H "Authorization: Bearer $TOKEN" \
      -H 'Content-Type: application/json' \
      -d "{\"principalRole\": {\"name\": \"$role\"}}" \
      && echo "Assigned role $role to $principal" \
      || echo "Failed to assign role $role to $principal"
  fi
}

{{PRINCIPAL_CALLS}}

echo "Done"
