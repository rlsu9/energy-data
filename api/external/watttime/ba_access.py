#!/usr/bin/env python3

# Source: https://www.watttime.org/api-documentation/#list-of-grid-regions

import requests
from util import get_watttime_token
import argparse

def get_accessible_regions(all_regions):
    list_url = 'https://api2.watttime.org/v2/ba-access'
    headers = {'Authorization': 'Bearer {}'.format(get_watttime_token())}
    params = {'all': str(all_regions).lower()}
    response = requests.get(list_url, headers=headers, params=params)
    assert 200 <= response.status_code < 300, "Request failed %d" % response.status_code
    return response.text

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--all-regions', action='store_true', help='Get all regions')
    args = parser.parse_args()

    print(get_accessible_regions(args.all_regions))
