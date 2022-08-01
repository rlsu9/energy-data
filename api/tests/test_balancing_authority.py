#!/usr/bin/env python3

import pytest
from api.tests.util import logger, assert_response_ok


class TestBalancingAuthority:
    def test_balancing_authority_list(self, client):
        response = client.get('/balancing-authority/list')
        assert_response_ok(response)

        expected_regions_subset = {
            'US-BPA',
            'US-CAISO',
        }
        actual_regions: list[str] = response.json
        logger.debug("Received %d regions." % len(actual_regions))
        assert expected_regions_subset.issubset(actual_regions)

    @pytest.mark.parametrize('location, expected_region', [
        ('UCSD', 'US-CAISO'),
        ('Milwaukee, WI', 'US-MISO'),
        ('Columbus, OH', 'US-PJM'),     # (AWS us-east-1)
        ('Ashburn, VA', 'US-PJM'),      # (AWS us-east-2)
        ('Boardman, OR', 'US-BPA'),     # (AWS us-west-1)
        ('Eugene, OR', 'US-BPA'),       # NOTE: for some reason, WattTime thinks this should be BPA instead of PACW
        ('Austin, TX', 'US-ERCOT'),
    ])
    def test_balancing_authority_get(self, location, expected_region, client, get_gps_coordinate):
        logger.info(f"Expecting ISO({location}) == {expected_region}")
        gps_coordinate = get_gps_coordinate(location)
        assert len(gps_coordinate) == 2
        response = client.get('/balancing-authority/', query_string={
            'latitude': gps_coordinate[0],
            'longitude': gps_coordinate[1]
        })
        assert_response_ok(response)

        response_json: dict[str] = response.json
        assert 'error' not in response_json
        assert 'region' in response_json and response_json['region'] == expected_region
