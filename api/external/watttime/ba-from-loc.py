#!/usr/bin/env python3

import requests
from requests.auth import HTTPBasicAuth
import argparse
from util import get_watttime_token

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

region_url = 'https://api2.watttime.org/v2/ba-from-loc'
headers = {'Authorization': 'Bearer {}'.format(get_watttime_token())}
params = {'latitude': loc[0], 'longitude': loc[1]}
rsp=requests.get(region_url, headers=headers, params=params)
print(rsp.text)

