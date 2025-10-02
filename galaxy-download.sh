#!/bin/bash
set -e

API='https://galaxy.ansible.com/api/v3/plugin/ansible/search/collection-versions/'
FILTERS='?repository_name='published'&is_deprecated=false&is_highest=true&order_by=name'

COUNT=`curl -s "$API""$FILTERS""&offset=0&limit=1" | jq -r .meta.count`
echo COUNT=$COUNT

OFFSET=0
LIMIT=100
while [ "$OFFSET" -lt "$COUNT" ]; do
  sleep $(( RANDOM % 8 + 8 ))
  echo GET $OFFSET - $((OFFSET + LIMIT - 1))
  curl -s "$API""$FILTERS""&offset=$OFFSET&limit=$LIMIT" | jq > galaxy."$OFFSET"
  OFFSET=$((OFFSET + LIMIT))
done
