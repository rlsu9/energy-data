#!/bin/zsh

cd "$(dirname "$0")"/..

set -e
conda activate py39
./crawler/crawl.py >> ./logs/crawler.log
