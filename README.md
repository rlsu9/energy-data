# energy-data
This repo holds scripts to collect energy data, e.g. electricity sources, carbon intensity, ...

We're starting with US electricity data from various ISOs, e.g. CAISO, MISO, PJM, etc.

## Crawler
[Crawler](./crawler) holds the script to pull data from various sources.
- [crawl.py](./crawler/crawl.py) runs every minute via `crontab`, invokes individual parser for each source and store the result in a postgre database. The crawling frequency for each source is defined near top of this file.
- Individual [parsers](./crawler/parsers) are copied/derived from electricityMap's [sources](https://github.com/electricitymap/electricitymap-contrib/tree/master/parsers) (MIT licensed).

### Data sources
We are starting with US ISOs, which currently include:
- [MISO](./crawler/parsers/US_MISO.py), which only has current data and is updated every five minutes.
- [CAISO](./crawler/parsers/US_CA.py), which has both current day and historic data, but we pull last day's full data daily.
- [PJM](./crawler/parsers/US_PJM.py), which only has current day data publicly available on their website, updated every hour.

## Database
- Database is currently hosted on development machine and only locally accessible (or via SSH tunnel).
- Table definitions are in [database/tables](./database/tables).
- I used Jetbrains DataGrip for quick access to the database and have included the IDE settings.
TODOs:
- External (read-only) data access.
- Data visualization.

## Deployment
Deployment scripts are in [deploy](./deploy).
The crawler deployment script ([deploy-crawler.sh](./deploy/deploy-crawler.sh)) copies the entire source tree to a "production" folder and installs the `run-*.sh` files with appropriate schedules via `crontab`.
Currently, we run:
- [Database backup](./deploy/run-backup.sh) once per day.
- [Main crawler](./deploy/run-crawler.sh) once every minute.
