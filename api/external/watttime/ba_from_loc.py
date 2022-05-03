#!/usr/bin/env python3

# Source: https://www.watttime.org/api-documentation/#determine-grid-region

import requests
import argparse
from util import get_watttime_token

# Get the balancing authority based on GPS location
def get_ba_from_loc(latitude: float, longitude: float):
    region_url = 'https://api2.watttime.org/v2/ba-from-loc'
    headers = {'Authorization': 'Bearer {}'.format(get_watttime_token())}
    params = {'latitude': latitude, 'longitude': longitude}
    response = requests.get(region_url, headers=headers, params=params)
    assert 200 <= response.status_code < 300, "Request failed %d" % response.status_code
    return response.text

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--loc', '-l', type=str, help='GPS coordinate')
    args = parser.parse_args()

    loc = (32.8800604,-117.2362075) # UCSD
    if args.loc:
        loc_array = args.loc.split(',')
        assert len(loc_array) == 2, "Invalid GPS coordinate"
        latitude = float(loc_array[0])
        longitude = float(loc_array[1])
        loc = (latitude, longitude)
    print(get_ba_from_loc(loc[0], loc[1]))
