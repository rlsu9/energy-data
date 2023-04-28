#!/usr/bin/env python3

import configparser
import os

import requests
from requests.auth import HTTPBasicAuth

CONFIG_FILE = "watttime.ini"


def get_username_password():
    try:
        parser = configparser.ConfigParser()
        config_filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), CONFIG_FILE)
        parser.read(config_filepath)
        username = parser['auth']['username']
        password = parser['auth']['password']
        return username, password
    except Exception as ex:
        raise ValueError(f'Failed to retrieve watttime credentials: {ex}') from ex


# Source: https://www.watttime.org/api-documentation/#login-amp-obtain-token

def get_watttime_token():
    login_url = 'https://api2.watttime.org/v2/login'
    (username, password) = get_username_password()
    response = requests.get(login_url, auth=HTTPBasicAuth(username, password))
    assert response.ok, "Failed to get token (%d): %s" % (response.status_code, response.text)
    token = response.json()['token']
    return token
