import requests
import configparser
import os
import psycopg2
import psycopg2.extras
from crawl import get_db_connection
CONFIG_FILE = "../api/external/electricitymap/electricitymap.ini"


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

    query = f"""
    INSERT INTO EMapCarbonIntensity(DateTime, ZoneId, CarbonIntensity, LowCarbonPercentage, RenewablePercentage)
    VALUES ('{datetime}', '{zone_id}', {carbon_intensity}, {low_carbon_percentage}, {renewable_percentage})
	ON CONFLICT (DateTime, ZoneId) DO UPDATE SET
    CarbonIntensity = EXCLUDED.CarbonIntensity,
    LowCarbonPercentage = EXCLUDED.LowCarbonPercentage,
    RenewablePercentage = EXCLUDED.RenewablePercentage;
    """
    return query


def get_history_data(conn, zone):
    url = f"https://api-access.electricitymaps.com/free-tier/carbon-intensity/history?zone={zone}"
    headers = {
        "auth-token": get_auth_token()
    }

    response = requests.get(url, headers=headers)

    parsed_data = response.json()

    # some data crawled from electricity-map history api may not contain the 'history' in their feedback
    if 'history' in parsed_data:
        for history in parsed_data['history']:
            query = prepare_insert_query(history)
            with conn, conn.cursor() as cur:
                try:
                    cur.execute(query)
                except psycopg2.Error as ex:
                    raise ValueError(
                        "Failed to execute set_last_updated query.") from ex


def get_all_electricity_zones():
    url = "https://api-access.electricitymaps.com/free-tier/zones"
    headers = {
        "auth-token": get_auth_token()
    }

    response = requests.get(url, headers=headers)

    parsed_data = response.json()

    return parsed_data.keys()


if __name__ == '__main__':
    conn = get_db_connection()
    for item in get_all_electricity_zones():
        get_history_data(conn, item)
