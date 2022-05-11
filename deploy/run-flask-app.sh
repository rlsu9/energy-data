#!/bin/zsh

cd "$(dirname "$0")/.."

set -e
source $HOME/anaconda3/bin/activate
conda activate flask
gunicorn --workers=4 \
    --log-level info \
    --access-logfile - \
    --access-logformat '%({X-Real-IP}i)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"' \
    flask_app:app
