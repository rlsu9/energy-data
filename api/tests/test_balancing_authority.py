#!/usr/bin/env python3

import pytest
from api.tests.util import logger, assert_response_ok

def test_balancing_authority_list(client):
    response = client.get('/balancing-authority/list')
    assert_response_ok(response)

    expected_regions_subset = {
        'US-BPA',
        'US-CAISO',
    }
    actual_regions: list[str] = response.json
    logger.debug("Received %d regions." % len(actual_regions))
    assert expected_regions_subset.issubset(actual_regions)
