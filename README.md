# energy-data
This repo holds scripts to collect energy data from various electricity sources (mostly US ISOs, e.g. CAISO, MISO, PJM, etc.) and the api to provide carbon data and related complex query like carbon-aware scheduling.

## Data sources

### `c3lab`, or directly collected data

We're starting with US electricity data from various ISOs

#### Crawler
[crawler](./crawler) holds the script to pull data from various sources.
- [crawl.py](./crawler/crawl.py) runs every minute via `crontab`, invokes individual parser for each source and store the result in a postgre database. The crawling frequency for each source is defined near top of this file.
- Individual [parsers](./crawler/parsers) are copied/derived from electricityMap's [sources](https://github.com/electricitymap/electricitymap-contrib/tree/master/parsers) (MIT licensed).

#### Covered regions
We are starting with US ISOs, which currently include:
- [MISO](./crawler/parsers/US_MISO.py), which only has current data and is updated every five minutes.
- [CAISO](./crawler/parsers/US_CAISO.py), [NEISO](./crawler/parsers/US_NEISO.py) and [NY](./crawler/parsers/US_NY.py), which has data for past few days, so we pull last day's full data daily.
- [PJM](./crawler/parsers/US_PJM.py), which only has current day data publicly available on their website, updated every hour.
- [BPA](./crawler/parsers/US_BPA.py), which has the data for past two days, so we pull daily.
- [SPP](./crawler/parsers/US_SPP.py), which only has current data for the past two hours, so we pull every hour.
- [PR](./crawler/parsers/US_PREPA.py) (**disabled**), which only has current data, but is stale and always shows 03/24/2022, so it's disabled for now.
- [HI](./crawler/parsers/US_HI.py) (**disabled**), which has daily historic data, but stopped after 04/13/2022, so it's disabled for now.
- `ERCOT` (~~[US_ERCOT.py](./crawler/parsers/US_ERCOT.py)~~) and `PACW` which uses the new data source from EIA, and has historic data. ~~We plan to migrate other sources to EIA as well to standardize the data sources.~~ (EIA data sources had some temporary issue since June 2022 and hasn't been fixed in July 2022.)

You can find the exact list at the top of the main crawler file [crawl.py](./crawler/crawl.py).

### `electricity-map`, or `EMap` for short

This covers the free trial data from [electricity map](https://www.electricitymaps.com/).

## Database
- Database is currently hosted on development machine and only locally accessible (or via SSH tunnel).
- Table definitions are in [database/tables](./database/tables).
- I used Jetbrains DataGrip for quick access to the database and have included the IDE settings.

TODOs:
- External (read-only) data access.
- Data visualization.

## REST API
This is work in progress.

We implement a REST API using [Flask](https://flask.palletsprojects.com/) and [Flask-RESTful](https://flask-restful.readthedocs.io/).
The code is located in [api](./api) and calls to external APIs are implemented in [api/external](./api/external).
The Flask app is deployed using `nginx` + `gunicorn`, which are detailed in the deployment script below. You can also run locally using `gunicorn` directly by executing `gunicorn api:create_app()` in repo root, or via [VSCode launch script](./.vscode/launch.json).

Currently, we support:
- [Look up balancing authority](./api/routes/balancing_authority.py) based on GPS coordinates (via WattTime API).
- [Look up carbon intensity](./api/routes/carbon_intensity.py) based on GPS coordinates and time range.
- [Carbon-aware multi-region scheduler](./api/routes/carbon_aware_scheduler.py) that assigns workload based on its [profile](./api/models/workload.py) and an [optimization algorithm](./api/models/optimization_engine.py).

The full list is defined in [api module](./api/__init__.py).

## Deployment
Deployment scripts are in [deploy](./deploy).

### Crawler
The crawler deployment script ([deploy-crawler.sh](./deploy/deploy-crawler.sh)) copies the crawler code and relevant scripts to a "production" folder and installs the `run-*.sh` files with appropriate schedules via `crontab`.
Currently, we run:
- [Database backup](./deploy/run-backup.sh) once per day.
- [Main crawler](./deploy/run-crawler.sh) once every minute.

### REST API
The REST API deployment script ([deploy-rest-api.sh](./deploy/deploy-rest-api.sh)) copies the api code to a "production" folder and reloads `supervisor`, which has been set up to monitor and control the `flask` app via `gunicorn`. `nginx` acts as a reverse proxy to `gunicorn`. The entire setup process is documented in [scripts/setup/install-flask-runtime.sh](./scripts/setup/install-flask-runtime.sh).
