#!/usr/bin/env python3

# Source: https://static.electricitymap.org/api/docs/index.html#zones

import requests

list_url = 'https://api.electricitymap.org/v3/zones'
rsp = requests.get(list_url)
print(rsp.text)
