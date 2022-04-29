#!/usr/bin/env python3

# Source: https://www.watttime.org/api-documentation/#list-of-grid-regions

import requests
from util import get_watttime_token

list_url = 'https://api2.watttime.org/v2/ba-access'
headers = {'Authorization': 'Bearer {}'.format(get_watttime_token())}
params = {'all': 'false'}
rsp=requests.get(list_url, headers=headers, params=params)
print(rsp.text)

