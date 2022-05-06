#!/usr/bin/env python3

# Copyright (c) 2022 C3-Lab (c3lab.net)

from dateutil import parser, tz
from logging import getLogger
import requests
import re, sys, arrow
from datetime import datetime

# Source: https://www.eia.gov/electricity/gridmonitor/dashboard/electric_overview/balancing_authority/ERCO
#   electricity generation by energy sources
# This has about 1 week worth of data, in hour granularity
EIA_JSON_URL = "https://www.eia.gov/electricity/930-api/export/data/json"

GENERATION_MAPPING = {
    'Wind': 'wind',
    'Hydro': 'hydro',
    'Fossil/Biomass': 'biomass',
    'Nuclear': 'nuclear',
    'VER': 'wind/solar',
}

def get_data_json(url, session=None):
    s = session or requests.Session()
    response = s.post(url)
    assert response.ok, "Failed to retrieve data from %s: %s" % (url, response.text)
    return response.json()

def convert_timestamp_str(timestamp_str: str):
    """Convert raw timestmap string from EIA API to proper timestamp object."""
    # timestamp_str: e.g. '4/28/2022 12 a.m. CDT'
    '''
    regex = re.compile(r'(\d+)/(\d+)/(\d+) (\d+) (a.m.|p.m.) CDT')
    m = regex.match(timestamp_str)
    assert m, "Failed to parse timestamp string \"%s\"" % timestamp_str
    month = int(m.group(1))
    day = int(m.group(2))
    year = int(m.group(3))
    hour = int(m.group(4))
    am_or_pm = m.group(5)
    if hour == 12:
        hour = 0
    if am_or_pm == 'p.m.':
        hour += 12
    return arrow.get(datetime(year, month, day, hour, tzinfo=tz.gettz('America/Chicago')))
    '''
    assert timestamp_str.endswith('CDT') or timestamp_str.endswith('CST'), \
        "Timezone has changed, refusing to parse %s" % timestamp_str
    if timestamp_str.endswith('CDT'):
        timestamp_str = timestamp_str.removesuffix('CDT')
    if timestamp_str.endswith('CST'):
        timestamp_str = timestamp_str.removesuffix('CST')
    timestamp_str = timestamp_str.replace('a.m.', 'am').replace('p.m.', 'pm').strip()
    dt = datetime.strptime(timestamp_str, '%m/%d/%Y %I %p')
    return arrow.get(dt, tzinfo=tz.gettz('America/Chicago'))

def transform_data(raw_data):
    """Change raw JSON to a list of (timestamp, {fuel-source: power_in_mw}) entries."""
    data_by_timestamp = {}
    # raw_data['title']: 'Electric Reliability Council of Texas, Inc. (ERCO) electricity generation by energy source 4/28/2022 – 5/5/2022, Central Time'
    for series in raw_data['series']:
        fuel_source_raw = series['name']
        if fuel_source_raw in GENERATION_MAPPING:
            fuel_source = GENERATION_MAPPING[fuel_source_raw]
        else:
            print("Unknow fuel source %s" % fuel_source_raw, file=sys.stderr)
            fuel_source = 'unknown'
        for data in series['data']:
            # E.g. {'Timestamp (Hour Ending)': '4/28/2022 12 a.m. CDT', 'value': 22103}
            timestamp_str = data['Timestamp (Hour Ending)']
            timestamp = convert_timestamp_str(timestamp_str)
            power_in_mw = data['value']
            if timestamp not in data_by_timestamp:
                data_by_timestamp[timestamp] = {}
            data_by_timestamp[timestamp][fuel_source] = power_in_mw
    return [(timestamp, d_power_by_fuel_source) for timestamp, d_power_by_fuel_source in data_by_timestamp]

def fetch_production(zone_key = 'US-ERCOT', session=None, target_datetime=None, logger=getLogger(__name__)) -> dict:
    """Requests the last known production mix (in MW) of a given zone."""
    if target_datetime is not None:
        raise NotImplementedError('This parser is not yet able to parse past dates')

    # Note: This unfortunately doesn't work and we need to load the data directly from downloaded JSON file.
    raw_data = get_data_json(EIA_JSON_URL)
    print(raw_data)
    processed_data = transform_data(raw_data)
    data = []
    for item in processed_data:
        datapoint = {'zoneKey': zone_key,
                     'datetime': item[0],
                     'production': item[1],
                     'storage': {},
                     'source': 'eia.gov'}

        data.append(datapoint)

if __name__ == "__main__":
    print('fetch_production() ->')
    print(fetch_production())
