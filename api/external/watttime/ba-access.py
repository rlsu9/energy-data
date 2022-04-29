#!/usr/bin/env python3

# Source: https://www.watttime.org/api-documentation/#list-of-grid-regions

import requests
from util import get_watttime_token
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--all-regions', action='store_true', help='Get all regions')
args = parser.parse_args()

list_url = 'https://api2.watttime.org/v2/ba-access'
headers = {'Authorization': 'Bearer {}'.format(get_watttime_token())}
params = {'all': str(args.all_regions).lower()}
rsp=requests.get(list_url, headers=headers, params=params)
print(rsp.text)

