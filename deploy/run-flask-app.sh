#!/bin/zsh

cd "$(dirname "$0")/.."

set -e
source $HOME/anaconda3/bin/activate
conda activate flask
gunicorn --workers=4 flask_app:app