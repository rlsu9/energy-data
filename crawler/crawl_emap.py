import requests
import configparser
from typing import Callable
import psycopg2
import psycopg2.extras
from crawl import get_db_connection
import datetime
from flask import current_app
import random
from time import sleep
import sys
CONFIG_FILE = "../api/external/electricitymap/electricitymap.ini"

def exponential_backoff(max_retries: int = 3,
                        base_delay_s: int = 1,
                        should_retry: Callable[[Exception], bool] = lambda _: True):
    def decorator(func):
        def wrapper(*args, **kwargs):
            retries = 0
            while True:
                try:
                    result_func = func(*args, **kwargs)
                    return result_func
                except Exception as ex:
                    current_app.logger.debug(f"Attempt {retries + 1} failed: {ex}")
                    if retries < max_retries and should_retry(ex):
                        delay = (base_delay_s * 2 ** retries + random.uniform(0, 1))
                        current_app.logger.debug(f"Retrying in {delay:.2f} seconds...")
                        sleep(delay)
                        retries += 1
                    else:
                        raise
        return wrapper
    return decorator

def get_auth_token():
    try:
        parser = configparser.ConfigParser()
        config_filepath = CONFIG_FILE
        parser.read(config_filepath)
        return parser['auth']['token']
    except Exception as ex:
        raise ValueError(
            f'Failed to retrieve watttime credentials: {ex}') from ex


def prepare_insert_query(data):
    carbon_intensity = data.get('carbonIntensity', 0.0)
    if carbon_intensity is None:
        carbon_intensity = 0.0

    low_carbon_percentage = 0.00
    renewable_percentage = 0.00

    datetime = data['datetime']

    zone_id = data['zone']

    query = """
    INSERT INTO EMapCarbonIntensity(DateTime, ZoneId, CarbonIntensity, LowCarbonPercentage, RenewablePercentage)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (DateTime, ZoneId) DO UPDATE SET
    CarbonIntensity = EXCLUDED.CarbonIntensity,
    LowCarbonPercentage = EXCLUDED.LowCarbonPercentage,
    RenewablePercentage = EXCLUDED.RenewablePercentage;
    """

    return query, (datetime, zone_id, carbon_intensity, low_carbon_percentage, renewable_percentage)

@exponential_backoff(should_retry=lambda ex: ex.response.status_code == 429)
def fetch(zone):
    url = f"https://api-access.electricitymaps.com/free-tier/carbon-intensity/history?zone={zone}"
    headers = {
        "auth-token": get_auth_token()
    }

    history_response = requests.get(url, headers=headers)

    history_response_json = history_response.json()
    try:
        assert history_response.ok, f"Error in EMap request for getting history according to zone ({history_response.status_code}): {history_response.text}"
        # Rest of your code goes here, which will execute if the assertion passes
    except AssertionError as error:
        print(str(error), file=sys.stderr)

    return history_response_json


def update(conn, history_response_json):
    if 'history' in history_response_json:
        for history in history_response_json['history']:
            query, data = prepare_insert_query(history)
            with conn, conn.cursor() as cur:
                try:
                    cur.execute(query, data)
                except psycopg2.Error as ex:
                    print(datetime.now().isoformat(),
                          f"Failed to execute set_last_updated query.: {ex}",
                          file=sys.stderr)


def fetch_and_update(conn, zone):
    history_response_json = fetch(zone)

    update(conn, history_response_json)


def get_all_electricity_zones():
    url = "https://api-access.electricitymaps.com/free-tier/zones"
    headers = {
        "auth-token": get_auth_token()
    }

    zone_response = requests.get(url, headers=headers)

    zone_response_json = zone_response.json()

    try:
        assert zone_response.ok, f"Error in EMap request for getting all zones in electricity map ({zone_response.status_code}): {zone_response.text}"
    except AssertionError as error:
        print(str(error), file=sys.stderr)

    return zone_response_json.keys()


if __name__ == '__main__':
    conn = get_db_connection()
    for item in get_all_electricity_zones():
        fetch_and_update(conn, item)
    conn.close()
