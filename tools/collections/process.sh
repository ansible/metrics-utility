#!/bin/sh
set -e
cd "`dirname "$0"`"

(
for f in galaxy.[0-9]*.json; do
  cat "$f" | jq '.data | map(.collection_version) | map( .namespace + "." + .name )' | sed -e 's/\[//g' -e 's/\]//g' -e 's/"//g' -e 's/,$//' -e 's/\s*//g' -e '/^$/d'
done
for f in hub.[0-9]*.json; do
  cat "$f" | jq '.data | map(.collection_version) | map( .namespace + "." + .name )' | sed -e 's/\[//g' -e 's/\]//g' -e 's/"//g' -e 's/,$//' -e 's/\s*//g' -e '/^$/d'
done
) | sort -u | sed -e 's/^/  "/' -e 's/$/",/' -e '1s/^/[/' -e '$s/,$/]/' | jq > collections.json
