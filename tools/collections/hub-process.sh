#!/bin/sh
set -e
cd "`dirname "$0"`"

rm -vf hub.all || true

for f in hub.[0-9]*; do
  cat "$f" | jq '.data | map(.collection_version) | map( .namespace + "." + .name )' | sed -e 's/\[//g' -e 's/\]//g' -e 's/"//g' -e 's/,$//' -e 's/\s*//g' -e '/^$/d' #>> hub.all
done

#sort -u hub.all | sponge hub.all
#wc -l hub.all
