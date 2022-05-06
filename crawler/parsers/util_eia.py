#!/usr/bin/env python3

import os
import configparser

CONFIG_FILE = "eia.ini"

def get_eia_api_key():
    parser = configparser.ConfigParser()
    config_filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), CONFIG_FILE)
    parser.read(config_filepath)
    return parser['auth']['api_key']
