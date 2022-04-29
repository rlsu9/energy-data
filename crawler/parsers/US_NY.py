#!/usr/bin/env python3

# Source: https://github.com/electricitymap/electricitymap-contrib/blob/master/parsers/US_NY.py

"""Real time parser for the state of New York."""
from collections import defaultdict
from datetime import timedelta
from operator import itemgetter
from urllib.error import HTTPError

import arrow
import pandas as pd

# Dual Fuel systems can run either Natural Gas or Oil, they represent
# significantly more capacity in NY State than plants that can only
# burn Natural Gas. When looking up fuel usage for NY in 2016 in
# https://www.eia.gov/electricity/data/state/annual_generation_state.xls
# 100 times more energy came from NG than Oil. That means Oil
# consumption in the Dual Fuel systems is roughly ~1%, and to a first
# approximation it's just Natural Gas.

# Pumped storage is present but is not split into a separate category.
from arrow.parser import ParserError

mapping = {
    'Dual Fuel': 'gas',
    'Natural Gas': 'gas',
    'Nuclear': 'nuclear',
    'Other Fossil Fuels': 'unknown',
    'Other Renewables': 'unknown-renewables',
    'Wind': 'wind',
    'Hydro': 'hydro'
}


def read_csv_data(url):
    """Gets csv data from a url and returns a dataframe."""

    csv_data = pd.read_csv(url)

    return csv_data


def timestamp_converter(timestamp_string):
    """Converts timestamps in nyiso data into aware datetime objects."""
    try:
        dt_naive = arrow.get(timestamp_string, 'MM/DD/YYYY HH:mm:ss')
    except ParserError:
        dt_naive = arrow.get(timestamp_string, 'MM/DD/YYYY HH:mm')
    dt_aware = dt_naive.replace(tzinfo='America/New_York').datetime

    return dt_aware


def data_parser(df) -> list:
    """
    Takes dataframe and loops over rows to form dictionaries consisting of datetime and generation type.
    Merges these dictionaries using datetime key.

    :return: list of tuples containing datetime string and production.
    """

    chunks = []
    for row in df.itertuples():
        piece = {}
        piece['datetime'] = row[1]
        piece[row[3]] = row[4]
        chunks.append(piece)

    # Join dicts on shared 'datetime' keys.
    combine = defaultdict(dict)
    for elem in chunks:
        combine[elem['datetime']].update(elem)

    ordered = sorted(combine.values(), key=itemgetter("datetime"))

    mapped_generation = []
    for item in ordered:
        mapped_types = [(mapping.get(k, k), v) for k, v in item.items()]

        # Need to avoid multiple 'unknown' keys overwriting.
        complete_production = defaultdict(lambda: 0.0)
        for key, val in mapped_types:
            try:
                complete_production[key] += val
            except TypeError:
                # Datetime is a string at this point!
                complete_production[key] = val

        dt = complete_production.pop('datetime')
        final = (dt, dict(complete_production))
        mapped_generation.append(final)

    return mapped_generation

def fetch_production(zone_key='US-NY', session=None, target_datetime=None, logger=None) -> list:
    """Requests the last known production mix (in MW) of a given zone."""
    target_datetime = arrow.get(target_datetime) if target_datetime else arrow.get()
    target_datetime = target_datetime.to('America/New_York')

    if (arrow.now() - target_datetime).days > 9:
        raise NotImplementedError('you can get data older than 9 days at the '
                                  'url http://mis.nyiso.com/public/')

    ny_date = target_datetime.format('YYYYMMDD')
    mix_url = 'http://mis.nyiso.com/public/csv/rtfuelmix/{}rtfuelmix.csv'.format(ny_date)
    try:
        raw_data = read_csv_data(mix_url)
    except HTTPError:
        # this can happen when target_datetime has no data available
        return None

    clean_data = data_parser(raw_data)

    production_mix = []
    for datapoint in clean_data:
        data = {
            'zoneKey': zone_key,
            'datetime': timestamp_converter(datapoint[0]),
            'production': datapoint[1],
            'storage': {},
            'source': 'nyiso.com'
        }

        production_mix.append(data)

    return production_mix


if __name__ == '__main__':
    """Main method, never used by the Electricity Map backend, but handy for testing."""

    from pprint import pprint
    print('fetch_production() ->')
    pprint(fetch_production())

    print('fetch_production(target_datetime=arrow.now("America/New_York").shift(days=-1) ->')
    pprint(fetch_production(target_datetime=arrow.now('America/New_York').shift(days=-1)))
