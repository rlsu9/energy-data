#!/bin/bash

if [ $# -lt 2 ]; then
    echo >&2 "Usage: $0 dbname dumpfile"
    exit 1
fi

dbname="$1"
dumpfile="$2"

set -x
cat "$dumpfile" | gunzip | psql --set ON_ERROR_STOP=on $dbname
