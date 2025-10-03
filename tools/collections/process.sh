#!/bin/sh
set -e
cd "`dirname "$0"`"

(
for f in galaxy.[0-9]*.json; do
  jq -S '.data | map( { key: (.collection_version.namespace + "." + .collection_version.name), value: "community" } ) | from_entries' "$f"
done
for f in hub.[0-9]*.json; do
  jq -S '.data | map( { key: (.collection_version.namespace + "." + .collection_version.name), value: (if .repository.name == "published" then "certified" else "validated" end) } ) | from_entries' "$f"
done
) | jq -S -s add > collections.json
