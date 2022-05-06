#!/bin/zsh

cd "$(dirname "$0")"/..

set -e
# set -x

ROOTDIR=/c3lab-migration/prod/electricity-data-crawler
echo "Deploying cralwer to: \"$ROOTDIR\""


echo "Copying files to deployment folder  ..."
rsync -auhvzP --delete \
    --exclude '__pycache__' \
    --filter='+ /crawler/' \
    --filter='+ /deploy/' \
    --filter='+ /scripts/' \
    --filter='+ /scripts/sql/' \
    --filter='- /scripts/*' \
    --filter='- /*' \
    ./ $ROOTDIR
git describe --all --long --dirty > $ROOTDIR/git-version.txt


echo "Installing cron schedule ..."
CRON_SCHEDULE=(
    # m h  dom mon dow   command
    # Run crawler every minute
    "* * * * * $ROOTDIR/deploy/run-crawler.sh"
    # Backup every day at 22:59
    "59 22 * * * $ROOTDIR/deploy/run-backup.sh"
)

# This avoid duplicate lines when re-running
# Sort in reverse to keep environment on top, e.g. SHELL=/bin/zsh
(crontab -l ; printf '%s\n' "${CRON_SCHEDULE[@]}") | sort -u -r - | crontab -
