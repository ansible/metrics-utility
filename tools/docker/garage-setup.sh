#!/bin/sh
set -e

echo "Installing curl and jq..."
apk add --no-cache curl jq

echo "Waiting for Garage to be ready..."
until curl -s http://garage:3900/health >/dev/null 2>&1; do
  sleep 2
done
sleep 3

echo "Garage is ready! Setting up cluster..."

ADMIN_TOKEN="admin-token-secret"
ADMIN_URL="http://garage:3900"

# Helper function to call Garage admin API
garage_api() {
  METHOD="$1"
  ENDPOINT="$2"
  DATA="$3"
  
  if [ -n "$DATA" ]; then
    curl -s -X "$METHOD" \
      -H "Authorization: Bearer $ADMIN_TOKEN" \
      -H "Content-Type: application/json" \
      -d "$DATA" \
      "$ADMIN_URL$ENDPOINT"
  else
    curl -s -X "$METHOD" \
      -H "Authorization: Bearer $ADMIN_TOKEN" \
      "$ADMIN_URL$ENDPOINT"
  fi
}

# Get node ID and setup layout
echo "Configuring node layout..."
STATUS_RESPONSE=$(garage_api GET /v1/status)
echo "Status: $STATUS_RESPONSE"

NODE_ID=$(echo "$STATUS_RESPONSE" | jq -r '.nodes[0].id' 2>/dev/null || echo "")

if [ -n "$NODE_ID" ] && [ "$NODE_ID" != "null" ] && [ "$NODE_ID" != "0" ]; then
  echo "Found node: $NODE_ID"
  
  # Check current layout
  LAYOUT_RESPONSE=$(garage_api GET /v1/layout)
  echo "Current layout: $LAYOUT_RESPONSE"
  
  # Update layout - assign capacity to the node (stage the change)
  echo "Assigning capacity to node..."
  LAYOUT_UPDATE="[{\"id\": \"$NODE_ID\", \"zone\": \"dc1\", \"capacity\": 1073741824, \"tags\": []}]"
  LAYOUT_PATCH_RESPONSE=$(garage_api POST "/v1/layout" "$LAYOUT_UPDATE")
  echo "Layout update response: $LAYOUT_PATCH_RESPONSE"
  
  sleep 2
  
  # Apply layout with incremented version number (version after staging)
  echo "Applying layout..."
  # Get the current version from the staged layout response
  STAGED_VERSION=$(echo "$LAYOUT_PATCH_RESPONSE" | jq -r '.version' 2>/dev/null || echo "0")
  # Garage expects the next version number when applying
  APPLY_VERSION=$((STAGED_VERSION + 1))
  echo "Applying layout version: $APPLY_VERSION"
  
  APPLY_RESPONSE=$(garage_api POST /v1/layout/apply "{\"version\": $APPLY_VERSION}")
  echo "Layout apply response: $APPLY_RESPONSE"
  
  sleep 3
  
  # Verify layout is applied
  FINAL_LAYOUT=$(garage_api GET /v1/layout)
  echo "Final layout: $FINAL_LAYOUT"
fi

# Create a new key (Garage will generate the key ID and secret)
echo "Creating access key..."
KEY_CREATE='{"name": "metrics-utility-key"}'
KEY_RESPONSE=$(garage_api POST /v1/key "$KEY_CREATE")
echo "Key creation API response: $KEY_RESPONSE"

KEY_ID=$(echo "$KEY_RESPONSE" | jq -r '.accessKeyId' 2>/dev/null || echo "")
KEY_SECRET=$(echo "$KEY_RESPONSE" | jq -r '.secretAccessKey' 2>/dev/null || echo "")

echo "Generated Key ID: $KEY_ID"
echo "Generated Key Secret: $KEY_SECRET"

# Update the environment variables file for reference
if [ -n "$KEY_ID" ] && [ "$KEY_ID" != "null" ] && [ -n "$KEY_SECRET" ] && [ "$KEY_SECRET" != "null" ]; then
  echo ""
  echo "=== UPDATE YOUR CONFIGURATION ==="
  echo "Use these credentials instead of myuseraccesskey/myusersecretkey:"
  echo "METRICS_UTILITY_BUCKET_ACCESS_KEY=$KEY_ID"
  echo "METRICS_UTILITY_BUCKET_SECRET_KEY=$KEY_SECRET"
  echo "================================="
  echo ""
fi

# Create bucket
echo "Creating bucket..."
BUCKET_CREATE=$(cat <<'EOF'
{
  "globalAlias": "metricsutilitys3"
}
EOF
)
BUCKET_RESPONSE=$(garage_api POST /v1/bucket "$BUCKET_CREATE" 2>/dev/null || echo "")
BUCKET_ID=$(echo "$BUCKET_RESPONSE" | jq -r '.id' 2>/dev/null || echo "")

# If we couldn't create it, try to find existing bucket
if [ -z "$BUCKET_ID" ] || [ "$BUCKET_ID" = "null" ]; then
  BUCKET_ID=$(garage_api GET /v1/bucket?globalAlias=metricsutilitys3 | jq -r '.[0].id' 2>/dev/null || echo "")
fi

# Allow key to access bucket using Garage CLI (API endpoint for this is not well-documented)
if [ -n "$KEY_ID" ] && [ "$KEY_ID" != "null" ]; then
  echo "Granting bucket permissions to key..."
  # Download garage binary if not already done
  if [ ! -f "/usr/local/bin/garage" ]; then
    wget -q -O /usr/local/bin/garage https://garagehq.deuxfleurs.fr/_releases/v1.0.0/aarch64-unknown-linux-musl/garage
    chmod +x /usr/local/bin/garage
  fi
  
  # Grant permissions using CLI
  /usr/local/bin/garage bucket allow --read --write metricsutilitys3 --key "$KEY_ID" || true
fi

echo "Garage setup complete!"

