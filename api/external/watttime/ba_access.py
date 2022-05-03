#!/usr/bin/env python3

# Source: https://www.watttime.org/api-documentation/#list-of-grid-regions

import requests
import argparse
if __package__:
    from . util import get_watttime_token
else:
    from util import get_watttime_token

def get_accessible_regions(all_regions: bool):
    list_url = 'https://api2.watttime.org/v2/ba-access'
    headers = {'Authorization': 'Bearer {}'.format(get_watttime_token())}
    params = {'all': str(all_regions).lower()}
    response = requests.get(list_url, headers=headers, params=params)
    return (response.json(), response.status_code)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--all-regions', action='store_true', help='Get all regions')
    args = parser.parse_args()

    (response, status_code) = get_accessible_regions(args.all_regions)
    assert 200 <= status_code < 300, "Request failed %d: %s" % (status_code, response)
    print(response)
