#!/bin/zsh

cd "$(dirname "$0")"

set -e
# set -x

source "$HOME/anaconda3/bin/activate"

# crawler env
conda create -n crawler python
conda activate crawler
conda install numpy pandas arrow psycopg2 requests beautifulsoup4
conda install -c conda-forge demjson3
conda deactivate

# flask env
conda create -n flask python
conda activate flask
conda install ipython numpy flask arrow psycopg2 requests gunicorn pytest
conda install -c conda-forge flask-restful pyyaml tzwhere webargs marshmallow "marshmallow-dataclass[enum,union]"
conda deactivate
