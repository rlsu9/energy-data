#!/bin/zsh

cd "$(dirname "$0")"

set -e
# set -x

source $HOME/anaconda3/bin/activate

for conda_env in py39 flask; do
    conda create --name $conda_env --file ./conf/conda/conda.env.$conda_env.txt
done
