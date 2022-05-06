#!/usr/bin/env python3

# Copyright (c) 2022 C3-Lab (c3lab.net)

from dateutil import tz
from logging import getLogger
import requests
import sys, arrow
from datetime import datetime
from .util_eia import get_eia_api_key

EIA_TEXAS_SERIES_BASE_URL = 'https://api.eia.gov/series/?'

EIA_SERIES_ID_MAPPING = {
    'coal': 'EBA.TEX-ALL.NG.COL.HL',
    'hydro': 'EBA.TEX-ALL.NG.WAT.HL',
    'gas': 'EBA.TEX-ALL.NG.NG.HL',
    'nuclear': 'EBA.TEX-ALL.NG.NUC.HL',
    'other': 'EBA.TEX-ALL.NG.OTH.HL',
    'solar': 'EBA.TEX-ALL.NG.SUN.HL',
    'wind': 'EBA.TEX-ALL.NG.WND.HL',
}

def parse_eia_timestamp(timestamp_str):
    # It can be in either hourly format or second-format: e.g. '20220506T00-05' and '2022-05-06T09:49:21-0400'.
    l_transform_parseformat = [
        ('%s00', '%Y%m%dT%H%z'),
        ('%s', '%Y-%m-%dT%H:%M:%S%z'),
    ]
    for (transform, parseformat) in l_transform_parseformat:
        try:
            return arrow.get(datetime.strptime(transform % timestamp_str, parseformat))
        except:
            pass
    raise ValueError('Cannot parse timestamp "%s"' % timestamp_str)

def convert_to_eia_timestamp(timestamp):
    # EIA API requires a two-digit timezone offset after hours, but %z in strftime() return four digits
    return timestamp.strftime('%Y%m%dT%H%z')[:-2]

def get_data_json(series_id, start_date, end_date, session=None):
    s = session or requests.Session()
    params = {
        'api_key': get_eia_api_key(),
        'series_id': series_id,
        'start': convert_to_eia_timestamp(start_date),
        'end': convert_to_eia_timestamp(end_date),
    }
    url = EIA_TEXAS_SERIES_BASE_URL + '&'.join(['%s=%s' % (k, v) for k, v in params.items()])
    response = s.post(url)
    assert response.ok, "Failed to retrieve data from %s: %s" % (url, response.text)
    return response.json()

def fetch_production(zone_key = 'US-ERCOT', session=None, target_datetime=None, logger=getLogger(__name__)) -> dict:
    """Requests the last known production mix (in MW) of a given zone."""
    target_datetime = arrow.get(target_datetime) if target_datetime else arrow.get()
    target_datetime = target_datetime.to('America/Chicago')
    target_date = arrow.get(target_datetime.date(), tzinfo=tz.gettz('America/Chicago'))
    request_start = target_date
    request_end = target_date.shift(days=1)

    production_by_timestamp = {}
    for fuel_source, series_id in EIA_SERIES_ID_MAPPING.items():
        response = get_data_json(series_id, request_start, request_end)
        # print(response, file=sys.stderr)
        assert len(response['series']) == 1, "Incorrect number of series returned"
        response_series = response['series'][0]
        assert response['request']['series_id'] == series_id == response_series['series_id']
        assert response_series['units'] == 'megawatthours', 'Unit has changed from MW!'
        response_start = parse_eia_timestamp(response_series['start'])
        response_end = parse_eia_timestamp(response_series['end'])
        response_updated = parse_eia_timestamp(response_series['updated'])
        if request_start < response_start:
            logger.warning("Requested start time is earlier than what's available")
        if request_end > response_end:
            logger.warning("Requested end time is later than what's available")
        if response_updated < request_end:
            logger.warning("Updated timestamp is less than end date, missing data is possible")
        for (timestamp_str, power_in_mw) in response_series['data']:
            timestamp = parse_eia_timestamp(timestamp_str)
            if timestamp not in production_by_timestamp:
                production_by_timestamp[timestamp] = {}
            production_by_timestamp[timestamp][fuel_source] = power_in_mw
    data = []
    for timestamp, production in production_by_timestamp.items():
        datapoint = {'zoneKey': zone_key,
                     'datetime': timestamp.datetime,
                     'production': production,
                     'storage': {},
                     'source': 'eia.gov'}
        data.append(datapoint)
    return data

if __name__ == "__main__":
    print('fetch_production(target_datetime=arrow.get(tzinfo=tz.gettz("America/Chicago")).shift(days=-1)) ->')
    print(fetch_production(target_datetime=arrow.get(tzinfo=tz.gettz("America/Chicago")).shift(days=-1)))
