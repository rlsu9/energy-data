#!/usr/bin/env python3

import configparser
import requests
from requests.auth import HTTPBasicAuth

CONFIG_FILE = "watttime.ini"

def get_username_password():
    parser = configparser.ConfigParser()
    parser.read(CONFIG_FILE)
    username = parser['auth']['username']
    password = parser['auth']['password']
    return (username, password)

# Source: https://www.watttime.org/api-documentation/#login-amp-obtain-token

def get_watttime_token():
    login_url = 'https://api2.watttime.org/v2/login'
    (username, password) = get_username_password()
    token = requests.get(login_url, auth=HTTPBasicAuth(username, password)).json()['token']
    return token
