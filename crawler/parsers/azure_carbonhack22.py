#!/usr/bin/env python3

# Run like this:
# region=XXX && cd /c3lab-migration/energy-data/crawler/parsers && python -u azure_carbonhack22.py -R $region --fetch-prediction 1> >(tee azure.prediction.$region.log >&1) 2> >(tee azure.prediction.$region.err >&2)

from dateutil import tz
import requests
import sys
import traceback
import argparse
import arrow
import psycopg2, psycopg2.extras
import time
import random
from datetime import datetime, timedelta

WINDOW_SIZE_IN_DAYS = 30

azure_regions = [
    "eastus",
    "southcentralus",
    "westus",
    "westus2",
    "westus3",
    "westcentralus",
    "australiaeast",
    "australiasoutheast",
    "northeurope",
    "swedencentral",
    "uksouth",
    "westeurope",
    "centralus",
    "canadaeast",
    "canadacentral",
    "francecentral",
    "germanywestcentral",
    "norwayeast",
    # "eastus2",    # same as "eastus"
    # "eastus2euap",    # same as "eastus"
    # "northcentralus", # same as "eastus"
    # "centraluseuap", # same as "centralus"
    # "australiacentral",   # same as "australiacentral"
    # "australiacentral2",  # same as "australiacentral"
    # "francesouth",    # same as "francecentral"
    # "germanynorth",   # same as "germanywestcentral"
    # "norwaywest",     # same as "norwayeast"
    # "ukwest",         # same as "uksouth"
    # "southeastasia",  400
    # "southafricanorth",   400
    # "centralindia",   400
    # "eastasia",   400
    # "japaneast",  400
    # "koreacentral",   400
    # "brazilsouth",    400
    # "jioindiawest",   400
    # "switzerlandnorth",   400
    # "uaenorth",   400
    # "southafricawest",    400
    # "japanwest",  400
    # "jioindiacentral",    400
    # "koreasouth", 400
    # "southindia", 400
    # "westindia",  400
    # "switzerlandwest",    400
    # "uaecentral", 400
    # "brazilsoutheast",    400
    # "asia", # 500
    # "asiapacific", # 500
    # "australia", # 500
    # "brazil", # 500
    # "canada", # 500
    # "europe", # 500
    # "france", # 500
    # "germany", # 500
    # "global", # 500
    # "india", # 500
    # "japan", # 500
    # "korea", # 500
    # "norway", # 500
    # "southafrica", # 500
    # "switzerland", # 500
    # "uae", # 500
    # "uk", # 500
    # "unitedstates", # 500
    # "unitedstateseuap", # 500
]

def get_db_connection(host='/var/run/postgresql/', database="electricity-data"):
    try:
        conn = psycopg2.connect(host=host, database=database, user="crawler_rw")
        return conn
    except Exception as e:
        print("Failed to connect to database.")
        raise e


# emissions part


def fetch_emissions(region: str, target_datetime: datetime) -> list:
    url_get_carbon_intensity = 'https://carbon-aware-api.azurewebsites.net/emissions/bylocations'
    response = requests.get(url_get_carbon_intensity, params={
        'location': [region],
        'time': arrow.get(target_datetime).shift(days=-WINDOW_SIZE_IN_DAYS),
        'toTime': arrow.get(target_datetime).shift(minutes=-1),
    })
    assert response.ok, "GSF carbon intensity lookup failed (%d): %s" % (response.status_code, response.text)
    if response.status_code == 204:
        return []
    try:
        response_json = response.json()
    except (ValueError, TypeError) as e:
        raise ValueError(f'Failed to read JSON: "{e}", url: "{response.request.path_url}", text: "{response.text}"')

    rows = []
    if len(response_json) == 0:
        return []
    min_timestamp = arrow.get(datetime.max, tz.UTC).datetime
    max_timestamp = arrow.get(datetime.min, tz.UTC).datetime
    for entry in response_json:
        iso = entry['location']
        timestamp = arrow.get(entry['time']).datetime
        rating = float(entry['rating'])
        duration = entry['duration']
        row = (region, iso, timestamp, rating, duration)
        rows.append(row)
        min_timestamp = min(min_timestamp, timestamp)
        max_timestamp = max(max_timestamp, timestamp)
    return rows, min_timestamp, max_timestamp

def upload_emissions_data(conn, rows):
    with conn, conn.cursor() as cur:
        try:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO AzureCarbonEmissions (region, iso, time, rating, duration)
                    VALUES %s
                    ON CONFLICT DO NOTHING;""",
                rows
            )
        except psycopg2.Error as e:
            raise ValueError(f'Failed to upload new data: {e}')

def get_emissions_data_count(conn, region: str, not_before: datetime, not_after: datetime) -> int:
    with conn, conn.cursor() as cur:
        try:
            cur.execute("""SELECT COUNT(*) FROM AzureCarbonEmissions
                            WHERE region = %s AND
                                %s <= time AND time <= %s;""",
                            [region, not_before, not_after])
            result = cur.fetchone()
        except psycopg2.Error as e:
            raise ValueError(f'Failed to get emissions data count: {e}')
    return result[0] if result is not None else datetime.min

def crawl_emissions_data_at(conn, region: str, target_datetime: datetime) -> int:
    print(f'region: {region}, date: {target_datetime.strftime("%Y/%m/%d")}')

    try:
        result = fetch_emissions(region, target_datetime)
        emissions_data = result[0]
        print(f'Retrieved {len(emissions_data)} rows.')
        if len(emissions_data) == 0:
            print('[ERROR] no data received!', file=sys.stderr)
            return 0

        upload_emissions_data(conn, emissions_data)
        count_inserted = get_emissions_data_count(conn, region, result[1], result[2])
        if len(emissions_data) != count_inserted:
            raise ValueError(f'[ERROR] Only inserted {count_inserted} rows.')
        print()
        return len(emissions_data)
    except Exception as e:
        print(datetime.now().isoformat(),
                f"Exception occurred while crawling region {region}: {e}",
                file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        return 0

def crawl_emissions_data(region: str, start_time: datetime):
    print(f'Crawling emissions data for region {region} ...')
    conn = get_db_connection()
    total_count = 0
    noresult_count = 0
    date = start_time
    while True:
        if noresult_count > 5:
            break
        count_records = crawl_emissions_data_at(conn, region, date.datetime)
        total_count += count_records
        date = date.shift(days=-WINDOW_SIZE_IN_DAYS)
        if count_records == 0:
            noresult_count += 1
    print(f'region: {region}, total count: {total_count}')
    return


# prediction part


fetch_prediction_count = 0


def fetch_prediction(region: str, target_datetime: datetime) -> dict:
    time.sleep(random.randint(1, 10))
    global fetch_prediction_count
    fetch_prediction_count += 1
    if fetch_prediction_count % 288 == 0:
        time.sleep(60)

    url_get_carbon_intensity = 'https://carbon-aware-api.azurewebsites.net/emissions/forecasts/batch'
    response = requests.post(url_get_carbon_intensity, json=[{
        'location': region,
        'requestedAt': arrow.get(target_datetime).for_json(),
        'windowSize': 5,
    }])
    try:
        assert response.ok
    except AssertionError:
        raise ValueError("GSF carbon forecast lookup failed (%d): %s" % (response.status_code, response.text))
    if response.status_code == 204:
        return [], None
    try:
        response_json = response.json()
    except (ValueError, TypeError) as e:
        raise ValueError(f'Failed to read JSON: "{e}", url: "{response.request.path_url}", text: "{response.text}"')

    rows = []
    if len(response_json) == 0:
        return [], None
    for response_element in response_json:
        generatedAt = response_element['generatedAt']
        for entry in response_element['forecastData']:
            iso = entry['location']
            timestamp = arrow.get(entry['timestamp']).datetime
            rating = float(entry['value'])
            duration = timedelta(minutes=entry['duration'])
            row = (region, iso, generatedAt, timestamp, rating, duration)
            rows.append(row)
    return rows, generatedAt

def upload_prediction_data(conn, rows):
    with conn, conn.cursor() as cur:
        try:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO AzureCarbonEmissionsForecast (region, iso, generatedAt, time, rating, duration)
                    VALUES %s
                    ON CONFLICT DO NOTHING;""",
                rows
            )
        except psycopg2.Error as e:
            raise ValueError(f'Failed to upload new data: {e}')

def get_prediction_data_count(conn, region: str, generatedAt: datetime) -> int:
    with conn, conn.cursor() as cur:
        try:
            cur.execute("""SELECT COUNT(*) FROM AzureCarbonEmissionsForecast
                            WHERE region = %s AND generatedAt = %s;""",
                            [region, generatedAt])
            result = cur.fetchone()
        except psycopg2.Error as e:
            raise ValueError(f'Failed to get emissions forecast data count: {e}')
    return result[0] if result is not None else datetime.min

def crawl_prediction_data_at(conn, region: str, target_datetime: datetime) -> int:
    print(f'region: {region}, date: {target_datetime.strftime("%Y/%m/%d %H:%M:%S")}')

    try:
        count_inserted = get_prediction_data_count(conn, region, target_datetime)
        if count_inserted > 0:
            print(f'Prediction data already exists. Skipping...')
            return count_inserted

        prediction_data, generatedAt = fetch_prediction(region, target_datetime)
        print(f'Retrieved {len(prediction_data)} rows.')
        if len(prediction_data) == 0:
            print('[ERROR] no data received!', file=sys.stderr)
            return 0

        upload_prediction_data(conn, prediction_data)
        count_inserted = get_prediction_data_count(conn, region, generatedAt)
        if len(prediction_data) != count_inserted:
            raise ValueError(f'[ERROR] Only inserted {count_inserted} rows.')
        print()
        return len(prediction_data)
    except Exception as e:
        print(datetime.now().isoformat(),
                f"Exception occurred while crawling region {region}: {e}",
                file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        return 0

def crawl_prediction_data(region: str, start_time: datetime):
    print(f'Crawling prediction data for region {region} ...')
    conn = get_db_connection()
    total_count = 0
    noresult_count = 0
    date = start_time
    while True:
        print(date.datetime)
        count_records = crawl_prediction_data_at(conn, region, date.datetime)
        total_count += count_records
        date = date.shift(hours=-1)
        if count_records == 0:
            noresult_count += 1
        if noresult_count > 5:
            break
    print(f'region: {region}, total count: {total_count}')
    return

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-R', '--regions', nargs='+', choices=azure_regions, help='Select a subset of regions')
    parser.add_argument('--start-time', help='The start time')
    parser.add_argument('--fetch-emissions', action='store_true', help='Fetch emissions data')
    parser.add_argument('--fetch-prediction', action='store_true', help='Fetch prediction data')
    args = parser.parse_args()

    regions_to_crawl = args.regions if args.regions else azure_regions
    print(f'Regions to crawl: {regions_to_crawl}')
    start_time = arrow.get(arrow.now().date())
    if args.start_time:
        start_time = arrow.get(args.start_time)
        print(f'Start time: {args.start_time}')
    for region in regions_to_crawl:
        if args.fetch_emissions:
            crawl_emissions_data(region, start_time)
        if args.fetch_prediction:
            crawl_prediction_data(region, start_time)

if __name__ == "__main__":
    main()
