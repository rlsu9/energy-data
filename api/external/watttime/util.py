#!/usr/bin/env python3

import configparser

CONFIG_FILE = "watttime.ini"

def get_username_password():
    parser = configparser.ConfigParser()
    parser.read(CONFIG_FILE)
    username = parser['auth']['username']
    password = parser['auth']['password']
    return (username, password)

