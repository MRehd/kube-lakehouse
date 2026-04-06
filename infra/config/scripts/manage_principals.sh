#!/bin/sh
# Polaris principal and RBAC management script

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

# Create a principal and store its initial credentials in a K8s Secret.
# Polaris only returns the clientSecret once — at creation time.
# Idempotent: skips entirely if the K8s Secret already exists.
# If the principal exists but the secret does not, deletes and recreates the
# principal to obtain fresh credentials.
# Args: principal_name k8s_secret_name
create_principal_with_secret() {
  local name="$1"
  local secret_name="$2"

  local k8s_token namespace k8s_ca k8s_api
  k8s_token=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
  namespace=$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace)
  k8s_ca=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt
  k8s_api="https://kubernetes.default.svc/api/v1/namespaces/$namespace/secrets"

  # Skip if secret already exists
  status=$(curl -sf -o /dev/null -w "%{http_code}" \
    --cacert "$k8s_ca" \
    -H "Authorization: Bearer $k8s_token" \
    "$k8s_api/$secret_name" 2>/dev/null || echo "000")

  if [ "$status" = "200" ]; then
    echo "Credentials secret $secret_name already exists, skipping"
    return
  fi

  # If principal already exists without a secret, delete it so we can recreate and capture creds
  exists=$(curl -sf -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $TOKEN" \
    "$POLARIS_URL/api/management/v1/principals/$name")

  if [ "$exists" = "200" ]; then
    echo "Principal $name exists but secret is missing — recreating to capture credentials"
    curl -sf -X DELETE -H "Authorization: Bearer $TOKEN" \
      "$POLARIS_URL/api/management/v1/principals/$name" \
      && echo "Deleted principal $name" \
      || { echo "Failed to delete principal $name"; exit 1; }
  fi

  # Create principal — response includes one-time clientId + clientSecret
  response=$(curl -sf -X POST "$POLARIS_URL/api/management/v1/principals" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"principal\":{\"name\":\"$name\",\"type\":\"SERVICE\"},\"credentialRotationRequired\":false}")

  client_id=$(echo "$response" | sed 's/.*"clientId":"\([^"]*\)".*/\1/')
  client_secret=$(echo "$response" | sed 's/.*"clientSecret":"\([^"]*\)".*/\1/')

  if [ -z "$client_id" ] || [ -z "$client_secret" ]; then
    echo "Failed to create principal $name or parse credentials from response"
    exit 1
  fi

  b64_id=$(printf '%s' "$client_id" | base64 | tr -d '\n')
  b64_secret=$(printf '%s' "$client_secret" | base64 | tr -d '\n')

  curl -sf -X POST \
    --cacert "$k8s_ca" \
    -H "Authorization: Bearer $k8s_token" \
    -H "Content-Type: application/json" \
    "$k8s_api" \
    -d "{
      \"apiVersion\": \"v1\",
      \"kind\": \"Secret\",
      \"metadata\": {\"name\": \"$secret_name\", \"namespace\": \"$namespace\"},
      \"data\": {\"CLIENT_ID\": \"$b64_id\", \"CLIENT_SECRET\": \"$b64_secret\"}
    }" \
    && echo "Stored credentials for $name in secret $secret_name" \
    || { echo "Failed to store credentials for $name"; exit 1; }
}

# Assign a principal role to a principal if not already assigned
# Args: principal_name role_name
assign_role() {
  local principal="$1"
  local role="$2"

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
