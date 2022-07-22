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

class TestBalancingAuthority_Get:
    @pytest.mark.parametrize('coordinate, expected_region, location', [
        ((32.8801, -117.2340), 'US-CAISO', "UCSD"),
        ((43.0389, -87.9065), 'US-MISO', "Milwaukee, WI"),
        ((39.9833, -82.9833), 'US-PJM', "Columbus, OH"),    # (AWS us-east-1)
        ((39.0438, -77.4874), 'US-PJM', "Ashburn, VA"),     # (AWS us-east-2)
        ((45.8399, -119.7006), 'US-BPA', "Boardman, OR"),   # (AWS us-west-1)
        # NOTE: for some reason, WattTime thinks this should be BPA instead of PACW
        ((44.0521, -123.0868), 'US-BPA', "Eugene, OR"),
        ((30.2672, -97.7431), 'US-ERCOT', "Austin, TX"),
    ])
    def test_balancing_authority_get(self, coordinate, expected_region, location, client):
        logger.info(f"Expecting ISO({location}) == {expected_region}")
        assert len(coordinate) == 2
        response = client.get('/balancing-authority/', query_string={
            'latitude': coordinate[0],
            'longitude': coordinate[1]
        })
        assert_response_ok(response)

        response_json: dict[str] = response.json
        assert 'error' not in response_json
        assert 'region' in response_json and response_json['region'] == expected_region
