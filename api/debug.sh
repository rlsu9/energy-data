#!/bin/zsh

cd "$(dirname "$0")/.."

source $HOME/anaconda3/bin/activate
conda activate flask

set -x
gunicorn --bind localhost:8082 -w 1 --log-level=debug --reload api:app
