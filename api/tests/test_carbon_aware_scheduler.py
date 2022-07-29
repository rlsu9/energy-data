#!/usr/bin/env python3

from datetime import timedelta

from api.tests.util import logger, assert_response_ok

class TestCarbonAwareScheduler:
    def test_onetime_no_delay(self, client):
        request_payload = {
            "runtime": timedelta(hours=1).total_seconds(),
            "schedule": {
                "type": "onetime",
                "start_time": "2022-07-28 20:40:52.973550-07:00",
                "interval": None,
                "max_delay": timedelta(hours=0).total_seconds()
            },
            "dataset": {
                "input_size_gb": 256,
                "output_size_gb": 0
            }
        }
        # PACW is the most clean one due to hydro, and currently the other scores are the same across regions
        expected_selected_region = 'AWS:us-west-2'
        expected_cloud_regions_and_isos = [
            ('AWS:us-west-1', 'US-CAISO'),
            ('AWS:us-west-2', 'US-PACW'),
            ('AWS:us-east-1', 'US-PJM'),
            ('AWS:us-east-2', 'US-PJM'),
        ]
        expected_cloud_regions = [cloud_region for cloud_region, _ in expected_cloud_regions_and_isos]

        response = client.get('/carbon-aware-scheduler/', json=request_payload)
        assert_response_ok(response)
        response_json: dict[str] = response.json
        assert 'error' not in response_json
        logger.debug(response_json)

        assert 'iso' in response_json
        actual_iso_mapping = response_json['iso']
        for cloud_region, expected_iso in expected_cloud_regions_and_isos:
            assert actual_iso_mapping[cloud_region] == expected_iso

        assert response_json['selected-region'] == expected_selected_region

        assert 'weighted-scores' in response_json
        assert set(response_json['weighted-scores'].keys()) == set(expected_cloud_regions)

        assert 'warning' not in response_json or not response_json['warning']

        for cloud_region, l_start_delay in response_json['start_delay'].items():
            assert all([start_delay == 0 for start_delay in l_start_delay])
