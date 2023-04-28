#!/usr/bin/env python3

# Source: https://www.watttime.org/api-documentation/#determine-grid-region

import requests
import argparse
from flask_caching import Cache

if __package__:
    from .util import get_watttime_token
else:
    from util import get_watttime_token


ba_from_loc_cache = Cache(config={'CACHE_TYPE': 'SimpleCache'})

# Get the balancing authority based on GPS location
@ba_from_loc_cache.memoize(timeout=0)
def get_ba_from_loc(latitude: float, longitude: float):
    region_url = 'https://api2.watttime.org/v2/ba-from-loc'
    headers = {'Authorization': 'Bearer {}'.format(get_watttime_token())}
    params = {'latitude': latitude, 'longitude': longitude}
    return requests.get(region_url, headers=headers, params=params)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--loc', '-l', type=str, help='GPS coordinate')
    args = parser.parse_args()

    loc = (32.8800604, -117.2362075)  # UCSD
    if args.loc:
        loc_array = args.loc.split(',')
        assert len(loc_array) == 2, "Invalid GPS coordinate"
        latitude = float(loc_array[0])
        longitude = float(loc_array[1])
        loc = (latitude, longitude)
    response = get_ba_from_loc(loc[0], loc[1])
    assert response.ok, "Request failed %d: %s" % (response.status_code, response.text)
    print(response.json())
