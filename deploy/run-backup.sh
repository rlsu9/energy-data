#!/bin/zsh

cd "$(dirname "$0")"/..

set -e
./scripts/sql/postgres.backup-database.sh electricity-data /c3lab-migration/prod/backups/$(date +%Y%m%d)-postgres.backup.electricity-data.gz
