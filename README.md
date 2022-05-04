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
