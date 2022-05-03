#!/usr/bin/env python3

# Source: https://www.watttime.org/api-documentation/#register-new-user

import requests
from util import get_username_password

# Register an account
def register():
    (username, password) = get_username_password()
    # print(username, password)

    register_url = 'https://api2.watttime.org/v2/register'
    params = {
        'username': username,
        'password': password,
        'email': 'c3lab@c3lab.net',
        'org': ''
    }
    response = requests.post(register_url, json=params)
    return (response.json(), response.status_code)

if __name__ == '__main__':
    (response, status_code) = register()
    assert 200 <= status_code < 300, "Request failed %d: %s" % (status_code, response)
    print(response)
