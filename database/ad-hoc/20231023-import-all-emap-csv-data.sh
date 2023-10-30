#!/bin/bash

set -e

find ./emap-data/ -type f -name "*.csv" -print0 | while read -d $'\0' file
do
    echo "Importing $file ..."
    psql -d electricity-data -q -v ON_ERROR_STOP=1 -v file="$file" < 20231023-import-emap-csv-data.sql
done
