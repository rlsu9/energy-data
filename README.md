# energy-data
This repo holds scripts to collect energy data, e.g. electricity sources, carbon intensity, ...

We're starting with US electricity data from various ISOs, e.g. CAISO, MISO, PJM, etc.

## crawler
[Crawler](./crawler) holds the script to pull data from various sources.
- [crawl.py](./crawler/crawl.py) runs every minute via `crontab`, invokes individual parser for each source and store the result in a postgre database.
- Individual [parsers](./crawler/parsers) are copied/derived from electricityMap's [sources](https://github.com/electricitymap/electricitymap-contrib/tree/master/parsers) (MIT licensed).

## data sources:
We are starting with US ISOs, which currently include:
- MISO, which only has current data and is updated every five minutes.
- (TODO) CAISO, which has both current day and historic data.
- (TODO) PJM, which only has current day data publicly available.
