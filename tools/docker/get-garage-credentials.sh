#!/bin/sh
# Helper script to get Garage S3 credentials

ADMIN_TOKEN="admin-token-secret"
ADMIN_URL="http://localhost:3900"

echo "Fetching Garage credentials..."
echo ""

# List all keys
KEYS=$(curl -s -H "Authorization: Bearer $ADMIN_TOKEN" "$ADMIN_URL/v1/key")

# Get the first key
KEY_ID=$(echo "$KEYS" | jq -r '.[0].accessKeyId' 2>/dev/null)
KEY_NAME=$(echo "$KEYS" | jq -r '.[0].name' 2>/dev/null)

if [ -n "$KEY_ID" ] && [ "$KEY_ID" != "null" ]; then
  echo "Found key: $KEY_NAME"
  echo ""
  echo "Add these to your environment or docker-compose.yaml:"
  echo "METRICS_UTILITY_BUCKET_ACCESS_KEY=$KEY_ID"
  echo ""
  echo "Note: The secret key is only shown once during creation."
  echo "Check the garage-setup container logs for the secret:"
  echo "  docker logs docker-garage-setup-1 | grep 'Generated Key Secret'"
else
  echo "No keys found. Run docker-compose up to initialize Garage."
fi

