#!/bin/zsh

cd "$(dirname "$0")"

conda_envs=(
    py39
    flask
)

set -e
# set -x

source $HOME/anaconda3/bin/activate

for conda_env in $conda_envs
do
    conda activate $conda_env
    conda list -e > ./conf/conda/conda.env.$conda_env.txt
    pip list --format=freeze > ./conf/pip/$conda_env/requirements.txt
done

conda deactivate
