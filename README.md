# energy-data
This repo holds scripts to collect energy data, e.g. electricity sources, carbon intensity, ...

We're starting with US electricity data from various ISOs, e.g. CAISO, MISO, PJM, etc.

## Crawler
[crawler](./crawler) holds the script to pull data from various sources.
- [crawl.py](./crawler/crawl.py) runs every minute via `crontab`, invokes individual parser for each source and store the result in a postgre database. The crawling frequency for each source is defined near top of this file.
- Individual [parsers](./crawler/parsers) are copied/derived from electricityMap's [sources](https://github.com/electricitymap/electricitymap-contrib/tree/master/parsers) (MIT licensed).

### Data sources
We are starting with US ISOs, which currently include:
- [MISO](./crawler/parsers/US_MISO.py), which only has current data and is updated every five minutes.
- [CAISO](./crawler/parsers/US_CISO.py), [NEISO](./crawler/parsers/US_NEISO.py) and [NY](./crawler/parsers/US_NY.py), which has data for past few days, so we pull last day's full data daily.
- [PJM](./crawler/parsers/US_PJM.py), which only has current day data publicly available on their website, updated every hour.
- [BPA](./crawler/parsers/US_BPA.py), which has the data for past two days, so we pull daily.
- [SPP](./crawler/parsers/US_SPP.py), which only has current data for the past two hours, so we pull every hour.
- [PR](./crawler/parsers/US_PREPA.py) (**disabled**), which only has current data, but is stale and always shows 03/24/2022, so it's disabled for now.
- [HI](./crawler/parsers/US_HI.py) (**disabled**), which has daily historic data, but stopped after 04/13/2022, so it's disabled for now.
- [ERCOT](./crawler/parsers/US_ERCOT.py), which uses the new data source from EIA, and has historic data. We plan to migrate other sources to EIA as well to standardize the data sources.

You can find the exact list at the top of the main crawler file [crawl.py](./crawler/crawl.py).

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
The code is located in [api](./api/) and calls to external APIs are implemented in [api/external](./api/external/).
The Flask app is deployed using NGINX + gunicorn, which are detailed in the deployment script below. You can also run locally using `gunicorn` directly by executing `gunicorn api:app` in repo root.

Currently we support:
- Look up balancing authority based on GPS coordinates (via WattTime API).

## Deployment
Deployment scripts are in [deploy](./deploy).

### Crawler
The crawler deployment script ([deploy-crawler.sh](./deploy/deploy-crawler.sh)) copies the crawler code and relevant scripts to a "production" folder and installs the `run-*.sh` files with appropriate schedules via `crontab`.
Currently, we run:
- [Database backup](./deploy/run-backup.sh) once per day.
- [Main crawler](./deploy/run-crawler.sh) once every minute.

### REST API
The REST API deployment script ([deploy-rest-api.sh](./deploy/deploy-rest-api.sh)) copies the api code to a "production" folder and reloads supervisor, which has been set up to monitor and control the flask app via gunicorn. NGINX acts as a proxy to gunicorn. The entire setup process is documented in [scripts/setup/install-flask-runtime.sh](./scripts/setup/install-flask-runtime.sh).
