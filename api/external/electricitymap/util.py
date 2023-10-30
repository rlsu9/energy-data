#!/usr/bin/env python3

import configparser
import os

CONFIG_FILE = "electricitymap.ini"


def get_auth_token():
    try:
        parser = configparser.ConfigParser()
        config_filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), CONFIG_FILE)
        parser.read(config_filepath)
        return parser['auth']['token']
    except Exception as ex:
        raise ValueError(f'Failed to retrieve watttime credentials: {ex}') from ex
