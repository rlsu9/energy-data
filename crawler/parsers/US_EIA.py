#!/usr/bin/env python3

# Copyright (c) 2022 C3-Lab (c3lab.net)

import logging
import sys
from dateutil import tz
from logging import getLogger
import requests
import json
import arrow
from datetime import datetime, date, timezone
if __name__ == "__main__":
    from util_eia import get_eia_api_key
else:
    from . util_eia import get_eia_api_key

EIA_V2_API_URL = 'https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/'

EIA_V2_REGION_MAPPING = {
    'US-BPA': 'BPAT',
    'US-CA': 'CAL',
    'US-CAISO': 'CISO',
    'US-ERCOT': 'ERCO',
    'US-MISO': 'MISO',
    'US-NEISO': 'ISNE', # or NE, seem to be the same (same below)
    'US-NY': 'NYIS',  # or NY
    'US-PJM': 'PJM',
    'US-SPP': 'SWPP',
    # '': '',
}

EIA_FUELTYPE_MAPPING = {
    'COL': 'coal',
    'WND': 'wind',
    'NG': 'gas',
    'OTH': 'other',
    'SUN': 'solar',
    'WAT': 'hydro',
    'NUC': 'nuclear',
    'OIL': 'oil'
    # '': '',
}

def get_eia_v2_region(region):
    '''Convert the region to EIA API v2 respondent name.'''
    if region in EIA_V2_REGION_MAPPING:
        return EIA_V2_REGION_MAPPING[region]
    raise ValueError("Region %s not defined in EIA mapping." % region)

def get_data_json(eia_respondents: list[str], start: arrow.arrow.Arrow, end: arrow.arrow.Arrow, session=None):
    logging.info("Request data for respodents [%s] in time range [%s, %s] ..." % (
        ', '.join(eia_respondents), start.isoformat(), end.isoformat()
    ))
    s = session or requests.Session()
    params = {
        'api_key': get_eia_api_key(),
    }
    url = EIA_V2_API_URL + '?' + '&'.join(['%s=%s' % (k, v) for k, v in params.items()])
    x_params = {
        "frequency": "hourly",
        "data": [ "value" ],
        "facets": {
            "respondent": eia_respondents
        },
        "start": start.to('utc').strftime("%Y-%m-%dT%H"),
        "end": end.to('utc').strftime("%Y-%m-%dT%H"),
        # "offset": 0
    }
    logging.debug("EIA API call header: %s" % json.dumps(x_params), file=sys.stderr)
    response = s.get(url, headers={ "X-Params": json.dumps(x_params) })
    assert response.ok, "Failed to retrieve data from %s: %s" % (url, response.text)
    response_json = response.json()
    assert 'response' in response_json, 'No "response" in returned json'
    return response_json['response']

def convert_eia_dateformat_to_strftime_format(eia_dateformat: str) -> str:
    '''Convert EIA dataFormat (e.g. "YYYY-MM-DD\"T\"HH24") to format used by datetime.strftime.'''
    m_subfields = {
        'YYYY': '%Y',
        'MM': '%m',
        'DD': '%d',
        '"T"': 'T',
        'HH24': '%H'
    }
    for orig_field, new_field in m_subfields.items():
        eia_dateformat = eia_dateformat.replace(orig_field, new_field)
    return eia_dateformat

def parse_eia_timestamp(timestamp_str: str, datetime_format: str) -> arrow.arrow.Arrow:
    return arrow.get(datetime.strptime(timestamp_str, datetime_format).replace(tzinfo=timezone.utc))

def parse_fueltype(fueltype: str) -> str:
    if fueltype not in EIA_FUELTYPE_MAPPING:
        logging.warning('Unknown fuel type "%s".' % fueltype)
        return 'unknown'
    return EIA_FUELTYPE_MAPPING[fueltype]

def get_power_in_mwh(value_str: str, value_units: str) -> float:
    """Convert the read value and value units into MWh numbers."""
    m_conversion_factor = {
        'megawatthours': pow(10, 0),
        'kilowatthours': pow(10, -3),
    }
    if value_units not in m_conversion_factor:
        raise ValueError('Power unit %s not recognized.' % value_units)
    return float(value_str) * m_conversion_factor[value_units]

def parse_eia_response(response, eia_respondent: str) -> dict:
    production_by_timestamp = {}
    assert response['total'] == len(response['data'])
    datetime_format = convert_eia_dateformat_to_strftime_format(response['dateFormat'])
    for entry in response['data']:
        assert entry['respondent'] == eia_respondent
        timestamp = parse_eia_timestamp(entry['period'], datetime_format)
        fuel_type = parse_fueltype(entry['fueltype'])
        power_in_mw = get_power_in_mwh(entry['value'], entry['value-units'])
        if timestamp not in production_by_timestamp:
            production_by_timestamp[timestamp] = {}
        production_by_timestamp[timestamp][fuel_type] = power_in_mw
    return production_by_timestamp

def fetch_production(zone_key = 'US-CAISO', target_datetime=None, logger=getLogger(__name__)) -> dict:
    """
        Requests the last known production mix (in MW) of a given zone.
        Note: UTC time is used in this EIA API wrapper, so we convert @target_datetime to utc before invoking EIA API.
    """
    target_datetime = arrow.get(target_datetime) if target_datetime else arrow.utcnow()
    request_start = target_datetime
    request_end = target_datetime.shift(days=1).shift(minutes=-1)

    eia_respondent = get_eia_v2_region(zone_key)
    response = get_data_json([eia_respondent], request_start, request_end)
    production_by_timestamp = parse_eia_response(response, eia_respondent)
    data = []
    for timestamp, production in production_by_timestamp.items():
        datapoint = {
            'datetime': timestamp.datetime,
            'production': production
        }
        data.append(datapoint)
    return data

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    for region in EIA_V2_REGION_MAPPING:
        yesterday = arrow.get(date.today()).shift(days = -1)
        print('fetch_production("%s", target_datetime=%s) -->' % (region, yesterday))
        print(fetch_production(region, target_datetime=yesterday))
