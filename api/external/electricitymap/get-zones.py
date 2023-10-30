#!/usr/bin/env python3

# Source: https://api-portal.electricitymaps.com/

import requests

from util import get_auth_token

list_url = 'https://api-access.electricitymaps.com/free-tier/zones'
headers = {
  "auth-token": get_auth_token(),
}
response = requests.get(list_url, headers=headers)
print(response.text)
