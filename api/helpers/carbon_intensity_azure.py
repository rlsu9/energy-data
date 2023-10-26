#!/usr/bin/env python3

from datetime import datetime, timedelta, timezone
from flask import current_app
import requests
import arrow

from api.helpers.balancing_authority import MAPPING_WATTTIME_BA_TO_AZURE_REGION
from api.models.common import ISO_PREFIX_WATTTIME
from api.util import carbon_data_cache, round_down, round_up

M_ISO_TO_AZURE_REGION = MAPPING_WATTTIME_BA_TO_AZURE_REGION

def get_azure_region_from_iso(iso: str) -> str:
    # Transform ISO region to azure region
    if iso.startswith(ISO_PREFIX_WATTTIME):
        iso = iso.removeprefix(ISO_PREFIX_WATTTIME)
        if iso not in M_ISO_TO_AZURE_REGION:
            raise ValueError(f'Unknown azure region for iso {iso}')
        return M_ISO_TO_AZURE_REGION[iso]
    else:
        raise NotImplementedError(f'Unknown azure region for iso {iso}')

@carbon_data_cache.memoize()
def fetch_emissions(region: str, start: datetime, end: datetime) -> tuple[bool, list[dict]|str]:
    """Fetch emission data and return success/failure and either:
        (on success) the time series carbon data, or
        (on failure) any error message."""
    current_app.logger.debug(f'fetch_emissions({region}, {start}, {end})')
    url_get_carbon_intensity = 'https://carbon-aware-api.azurewebsites.net/emissions/bylocations'
    response = requests.get(url_get_carbon_intensity, params={
        'location': [region],
        'time': arrow.get(start).for_json(),
        'toTime': arrow.get(end).shift(minutes=-1).for_json(),
    })
    try:
        assert response.ok
    except AssertionError:
        return False, "GSF carbon intensity lookup failed (%d): %s" % (response.status_code, response.text)
    if response.status_code == 204:
        return True, []
    try:
        response_json = response.json()
    except (ValueError, TypeError) as e:
        return False, f'Failed to read JSON: "{e}", url: "{response.request.path_url}", text: "{response.text}"'

    rows = []
    if len(response_json) == 0:
        return rows

    for entry in response_json:
        iso = entry['location']
        timestamp = arrow.get(entry['time']).datetime
        rating = float(entry['rating'])
        duration = entry['duration']
        rows.append({
            'timestamp': timestamp,
            'carbon_intensity': rating
        })
    return True, rows


@carbon_data_cache.memoize()
def fetch_prediction(region: str, start: datetime, end: datetime) -> tuple[bool, list[dict]|str]:
    """Fetch prediction data and return success/failure and either:
        (on success) the time series carbon data, or
        (on failure) any error message."""
    current_app.logger.debug(f'fetch_prediction({region}, {start}, {end})')
    url_get_carbon_intensity = 'https://carbon-aware-api.azurewebsites.net/emissions/forecasts/current'
    # Calculate bounds based on typical available window of this prediction API
    min_window = timedelta(minutes=5)
    prediction_window_min = round_up(datetime.now(timezone.utc), min_window)
    prediction_window_max = prediction_window_min + timedelta(days=1)
    start = max(prediction_window_min, round_down(start, min_window))
    end = min(prediction_window_max, round_up(end, min_window))
    response = requests.get(url_get_carbon_intensity, params={
        'location': region,
        'dataStartAt': arrow.get(start).for_json(),
        'dataEndAt': arrow.get(end).shift(minutes=5).for_json(),
        'windowSize': 5,
    })
    try:
        assert response.ok
    except AssertionError:
        return False, "GSF carbon forecast lookup failed (%d): %s" % (response.status_code, response.text)
    if response.status_code == 204:
        return True, []
    try:
        response_json = response.json()
    except (ValueError, TypeError) as e:
        return False, f'Failed to read JSON: "{e}", url: "{response.request.path_url}", text: "{response.text}"'

    if len(response_json) == 0:
        return []
    rows = []
    for response_element in response_json:
        generatedAt = arrow.get(response_element['generatedAt']).datetime
        current_app.logger.debug(f'fetch_prediction({region}, {start}, {end}) generatedAt: {generatedAt}')
        for entry in response_element['forecastData']:
            iso = entry['location']
            timestamp = arrow.get(entry['timestamp']).datetime
            rating = float(entry['value'])
            duration = timedelta(minutes=entry['duration'])
            rows.append({
                'timestamp': timestamp,
                'carbon_intensity': rating
            })
        break
    return True, rows

def get_carbon_intensity_list(iso: str, start: datetime, end: datetime,
        use_prediction: bool = False) -> list[dict]:
    """Retrieve the carbon intensity time series data in the given time window.

        Args:
            iso: the ISO region name.
            start: the start time.
            end: the end time.
            use_prediction: whether to use prediction or actual data.

        Returns:
            A list of time series data.
    """
    region = get_azure_region_from_iso(iso)
    if use_prediction:
        success, result_or_error = fetch_prediction(region, start, end)
    else:
        success, result_or_error = fetch_emissions(region, start, end)
    if success:
        return result_or_error
    else:
        raise ValueError('Failed to get carbon intensity: ' + result_or_error)
