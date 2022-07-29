#!/bin/zsh

cd "$(dirname "$0")"

set -e
# set -x

source $HOME/anaconda3/bin/activate

# py39 env
conda create -n py39 python=3.9
conda activate py39
conda install ipython numpy arrow psycopg2 requests pandas
conda deactivate

# flask env
conda create -n flask python
conda activate flask
conda install ipython numpy flask arrow psycopg2 requests gunicorn pytest
conda install -c conda-forge flask-restful pyyaml tzwhere webargs marshmallow "marshmallow-dataclass[enum,union]"
conda deactivate
