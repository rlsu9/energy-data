#!/usr/bin/env python3


"""Real time parser for the New England ISO (NEISO) area."""
from datetime import datetime, date, timedelta
import arrow
from collections import defaultdict
import logging
import requests
import time

url = 'https://www.iso-ne.com/ws/wsclient'

generation_mapping = {
    'Coal': 'coal',
    'NaturalGas': 'gas',
    'Wind': 'wind',
    'Hydro': 'hydro',
    'Nuclear': 'nuclear',
    'Wood': 'biomass',
    'Oil': 'oil',
    'Refuse': 'biomass',
    'LandfillGas': 'biomass',
    'Solar': 'solar'
}


def timestring_converter(time_string):
    """Converts ISO-8601 time strings in neiso data into aware datetime objects."""

    dt_naive = arrow.get(time_string)
    dt_aware = dt_naive.replace(tzinfo='America/New_York').datetime

    return dt_aware


def get_json_data(target_datetime, params, session=None):
    """Fetches json data for requested params and target_datetime using a post request."""

    epoch_time = str(int(time.time()))

    # when target_datetime is None, arrow.get(None) will return current time
    target_datetime = arrow.get(target_datetime) if target_datetime else arrow.get()
    target_ne = target_datetime.to('America/New_York')
    target_ne_day = target_ne.format('MM/DD/YYYY')
    print(target_ne_day)

    postdata = {
        '_nstmp_formDate': epoch_time,
        '_nstmp_startDate': target_ne_day,
        '_nstmp_endDate': target_ne_day,
        '_nstmp_twodays': 'false',
        '_nstmp_showtwodays': 'false'
    }
    postdata.update(params)

    s = session or requests.Session()

    req = s.post(url, data=postdata)
    json_data = req.json()
    raw_data = json_data[0]['data']

    return raw_data


def production_data_processer(raw_data, logger) -> list:
    """
    Takes raw json data and removes unnecessary keys.
    Separates datetime key and converts to a datetime object.
    """

    other_keys = {'BeginDateMs', 'Renewables', 'BeginDate', 'Other'}
    known_keys = generation_mapping.keys() | other_keys

    unmapped = set()
    clean_data = []
    counter = 0
    for datapoint in raw_data:
        current_keys = datapoint.keys() | set()
        unknown_keys = current_keys - known_keys
        unmapped = unmapped | unknown_keys

        keys_to_remove = {'BeginDateMs', 'Renewables'} | unknown_keys
        for k in keys_to_remove:
            datapoint.pop(k, None)

        time_string = datapoint.pop('BeginDate', None)
        if time_string:
            dt = timestring_converter(time_string)
        else:
            # passing None to arrow.get() will return current time
            counter += 1
            logger.warning('Skipping US-NEISO datapoint missing timestamp.', extra={'key': 'US-NEISO'})
            continue

        # neiso storage flow signs are opposite to EM
        battery_storage = -1*datapoint.pop('Other', 0.0)

        production = defaultdict(lambda: 0.0)
        for k, v in datapoint.items():
            # Need to avoid duplicate keys overwriting.
            production[generation_mapping[k]] += v

        # move small negative values to 0
        for k, v in production.items():
            if -5 < v < 0:
                production[k] = 0

        clean_data.append((dt, dict(production), battery_storage))

    for key in unmapped:
        logger.warning('Key \'{}\' in US-NEISO is not mapped to type.'.format(key), extra={'key': 'US-NEISO'})

    if counter > 0:
        logger.warning('Skipped {} US-NEISO datapoints that were missing timestamps.'.format(counter), extra={'key': 'US-NEISO'})

    return sorted(clean_data)

def fetch_production(zone_key='US-NEISO', session=None, target_datetime=datetime.now() + timedelta(days=-1), logger=logging.getLogger(__name__)) -> list:
    """Requests the last known production mix (in MW) of a given country."""

    postdata = {
        '_nstmp_chartTitle': 'Fuel+Mix+Graph',
        '_nstmp_requestType': 'genfuelmix',
        '_nstmp_fuelType': 'all',
        '_nstmp_height': '250'
    }

    production_json = get_json_data(target_datetime, postdata, session)
    points = production_data_processer(production_json, logger)

    # Hydro pumped storage is included within the general hydro category.
    production_mix = []
    for item in points:
        data = {
            'zoneKey': zone_key,
            'datetime': item[0],
            'production': item[1],
            'storage': {
                'hydro': None,
                'battery': item[2]
            },
            'source': 'iso-ne.com'
        }
        production_mix.append(data)

    return production_mix


if __name__ == '__main__':
    """Main method, never used by the Electricity Map backend, but handy for testing."""

    from pprint import pprint
    print('fetch_production() ->')
    pprint(fetch_production())

    print('fetch_production(target_datetime=arrow.get(date(2017, 12, 31), "America/New_York") ->')
    pprint(fetch_production(target_datetime=arrow.get(date(2017, 12, 31), 'America/New_York')))
