#!/bin/sh
set -e

rm -vf galaxy.all || true

for f in galaxy.[0-9]*; do
  cat "$f" | jq '.data | map(.collection_version) | map( .namespace + "." + .name )' | sed -e 's/\[//g' -e 's/\]//g' -e 's/"//g' -e 's/,$//' -e 's/\s*//g' -e '/^$/d' >> galaxy.all
done

sort -u galaxy.all | sponge galaxy.all
wc -l galaxy.all
