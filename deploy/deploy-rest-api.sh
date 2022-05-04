#!/bin/zsh

cd "$(dirname "$0")"/..

set -e
# set -x

PROD_DIR=/c3lab-migration/prod/electricity-data-rest-api
echo "Deploying Flask REST API to: \"$PROD_DIR\""


echo "Copying files to deployment folder  ..."
rsync -auhvzP --delete \
    --exclude '__pycache__' \
    --filter='+ /external/' \
    --filter='+ /external/watttime/' \
    --filter='- /external/watttime/data' \
    --filter='- /external/*' \
    --filter='+ /resources/' \
    --filter='+ /__init__.py' \
    --filter='- /*' \
    ./api/ "$PROD_DIR/flask_app"
rsync -auhvzP \
    --exclude '__pycache__' \
    --filter='+ /deploy/' \
    --filter='+ /deploy/run-flask-app.sh' \
    --filter='- /deploy/*' \
    --filter='- /*' \
    ./ "$PROD_DIR"


echo "Reloading flask app via supervisor ..."
sudo supervisorctl reload
echo "Done"

echo "You can run this to monitor supervisor stderr logs:"
echo "tail -f /c3lab-migration/prod/electricity-data-rest-api/flask_app.err"
