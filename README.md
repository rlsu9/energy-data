# energy-data
This repo holds scripts to collect energy data, e.g. electricity sources, carbon intensity, ...

We're starting with US electricity data from various ISOs, e.g. CAISO, MISO, PJM, etc.

## crawler
[Crawler](./crawler) holds the script to pull data from various sources.
- [crawl.py](./crawler/crawl.py) runs every minute via `crontab`, invokes individual parser for each source and store the result in a postgre database. The crawling frequency for each source is defined near top of this file.
- Individual [parsers](./crawler/parsers) are copied/derived from electricityMap's [sources](https://github.com/electricitymap/electricitymap-contrib/tree/master/parsers) (MIT licensed).

## data sources:
We are starting with US ISOs, which currently include:
- [MISO](./crawler/parsers/US_MISO.py), which only has current data and is updated every five minutes.
- [CAISO](./crawler/parsers/US_CA.py), which has both current day and historic data, but we pull last day's full data daily.
- [PJM](./crawler/parsers/US_PJM.py), which only has current day data publicly available on their website, updated every hour.
