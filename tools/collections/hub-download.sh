#!/bin/bash
set -e
cd "`dirname "$0"`"

SSO='https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token'
CLIENT_ID=${CLIENT_ID:?Missing CLIENT_ID}
CLIENT_SECRET=${CLIENT_SECRET:?Missing CLIENT_SECRET}

BEARER=`curl -s -d "client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET&grant_type=client_credentials" -H "Content-Type: application/x-www-form-urlencoded" -X POST "$SSO" | jq -r .access_token`

API='https://console.redhat.com/api/automation-hub/v3/plugin/ansible/search/collection-versions/'
FILTERS='?is_deprecated=false&is_highest=true&order_by=name&repository_label=!hide_from_search'

COUNT=`curl -H "Authorization: Bearer $BEARER" -s "$API""$FILTERS""&offset=0&limit=1" | jq -r .meta.count`
echo COUNT=$COUNT

OFFSET=0
LIMIT=100
while [ "$OFFSET" -lt "$COUNT" ]; do
  sleep $(( RANDOM % 8 + 8 ))
  echo GET $OFFSET - $((OFFSET + LIMIT - 1))
  curl -H "Authorization: Bearer $BEARER" -s "$API""$FILTERS""&offset=$OFFSET&limit=$LIMIT" | jq > hub."$OFFSET".json
  OFFSET=$((OFFSET + LIMIT))
done
