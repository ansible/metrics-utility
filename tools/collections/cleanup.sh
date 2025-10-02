#!/bin/bash
set -e
cd "`dirname "$0"`"

rm -v ./{galaxy,hub}.[0-9]*.json || true
