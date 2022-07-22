#!/usr/bin/env python3

import logging

logger = logging.getLogger()

def assert_response_ok(response):
    assert response.status_code < 400, \
        "status_code not ok (%d): %s" % (response.status_code, response.data.decode('utf-8'))
