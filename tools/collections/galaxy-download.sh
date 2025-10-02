#!/bin/bash
set -e
cd "`dirname "$0"`"

API='https://galaxy.ansible.com/api/v3/plugin/ansible/search/collection-versions/'
FILTERS='?is_deprecated=false&is_highest=true&order_by=name&repository_label=!hide_from_search'

COUNT=`curl -s "$API""$FILTERS""&offset=0&limit=1" | jq -r .meta.count`
echo COUNT=$COUNT

OFFSET=0
LIMIT=100
while [ "$OFFSET" -lt "$COUNT" ]; do
  sleep $(( RANDOM % 8 + 8 ))
  echo GET $OFFSET - $((OFFSET + LIMIT - 1))
  curl -s "$API""$FILTERS""&offset=$OFFSET&limit=$LIMIT" | jq > galaxy."$OFFSET".json
  OFFSET=$((OFFSET + LIMIT))
done
