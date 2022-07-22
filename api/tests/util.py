#!/usr/bin/env python3

import logging
from typing import Tuple
from tzwhere import tzwhere
import pytz
from datetime import datetime

logger = logging.getLogger()
g_tzwhere = tzwhere.tzwhere()

def assert_response_ok(response):
    assert response.status_code < 400, \
        "status_code not ok (%d): %s" % (response.status_code, response.data.decode('utf-8'))

def add_timezone_by_gps(dt: datetime, gps: Tuple[float, float]) -> datetime:
    timezone_str = g_tzwhere.tzNameAt(gps[0], gps[1])
    return pytz.timezone(timezone_str).localize(dt)
