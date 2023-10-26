#!/usr/bin/env python3

import argparse
import requests

from api.util import simple_cache
from flask import current_app

if __package__:
    from .util import get_auth_token
else:
    from util import get_auth_token


# Get the balancing authority based on GPS location
@simple_cache.memoize(timeout=0)
def get_ba_from_loc(latitude: float, longitude: float):
    region_url = 'https://api-access.electricitymaps.com/free-tier/home-assistant'
    headers = { "auth-token": get_auth_token() }
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
