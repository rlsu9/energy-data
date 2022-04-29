#!/usr/bin/env python3

# Source: https://www.watttime.org/api-documentation/#register-new-user

import requests
from util import get_username_password

(username, password) = get_username_password()
print(username, password)

register_url = 'https://api2.watttime.org/v2/register'
params = {
    'username': username,
    'password': password,
    'email': 'c3lab@c3lab.net',
    'org': ''
}
rsp = requests.post(register_url, json=params)
print(rsp.text)

