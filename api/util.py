#!/usr/bin/env python3

import logging

def getLogger():
    return logging.getLogger('gunicorn.error')