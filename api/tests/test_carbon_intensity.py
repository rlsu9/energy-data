#!/usr/bin/env python3

from datetime import date, datetime
import pytest
import numpy as np
import warnings
warnings.filterwarnings("ignore", category=np.VisibleDeprecationWarning) 

from api.tests.util import add_timezone_by_gps, logger, assert_response_ok

def localtime(hour=0, minute=0, date=date(2022, 7, 1)):
    """Returns a datetime without tzinfo."""
    return datetime(date.year, date.month, date.day, hour, minute)

TR_NOON = (localtime(10), localtime(14))
TR_EARLYNIGHT = (localtime(20), localtime(22))
TR_ALLDAY = (localtime(0), localtime(23, 59))

class TestCarbonIntensity:
    @pytest.mark.parametrize('location, iso, timerange, expected_carbon_intensity_range', [
        ('UCSD', 'US-CAISO', TR_NOON, (100, 150)),              # Mostly solar during daytime
        ('UCSD', 'US-CAISO', TR_EARLYNIGHT, (250, 350)),        # No solar during nighttime
        ('Milwaukee, WI', 'US-MISO', TR_ALLDAY, (300, None)),   # All-day coal
        ('Columbus, OH', 'US-PJM', TR_ALLDAY, (300, None)),     # All-day coal
        ('Boardman, OR', 'US-BPA', TR_ALLDAY, (None, 60)),      # All-day hydro
        ('Austin, TX', 'US-ERCOT', (localtime(0, 0, date(2022, 6, 1)),
            localtime(23, 59, date(2022, 6, 1))), (300, 400)),  # Relative stable in ERCOT
    ])
    def test_carbon_intensity_in_rough_range(self, location, iso, timerange, expected_carbon_intensity_range, client, get_gps_coordinate):
        (time_start, time_end) = timerange
        (expected_ci_min, expected_ci_max) = expected_carbon_intensity_range
        logger.info(f"Expecting CI({location}, [{time_start}, {time_end}])"
                    " in range [{expected_ci_min}, {expected_ci_max}]")
        gps_coordinate = get_gps_coordinate(location)
        assert len(gps_coordinate) == 2
        response = client.get('/carbon-intensity/', query_string={
            'latitude': gps_coordinate[0],
            'longitude': gps_coordinate[1],
            'start': add_timezone_by_gps(time_start, gps_coordinate),
            'end': add_timezone_by_gps(time_end, gps_coordinate),
        })
        assert_response_ok(response)

        response_json: dict[str] = response.json
        assert 'error' not in response_json
        assert 'carbon_intensities' in response_json
        assert isinstance(response_json['carbon_intensities'], list)
        l_actual_ci = []
        for entry in response_json['carbon_intensities']:
            l_actual_ci.append(entry['carbon_intensity'])
        assert len(l_actual_ci) > 0, 'No carbon intensity data'
        if expected_ci_min is not None:
            assert expected_ci_min <= min(l_actual_ci)
        if expected_ci_max is not None:
            assert max(l_actual_ci) <= expected_ci_max
