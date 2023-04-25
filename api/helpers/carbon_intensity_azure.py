#!/usr/bin/env python3

from datetime import datetime, timedelta
import requests
import arrow
import pandas as pd

from api.helpers.balancing_authority import MAPPING_WATTTIME_BA_TO_AZURE_REGION

M_ISO_TO_AZURE_REGION = MAPPING_WATTTIME_BA_TO_AZURE_REGION

def get_azure_region_from_iso(iso: str) -> str:
    # Transform ISO region to azure region
    if iso not in M_ISO_TO_AZURE_REGION:
        raise ValueError(f'Unknown azure region for iso {iso}')
    return M_ISO_TO_AZURE_REGION[iso]

def fetch_emissions(region: str, start: datetime, end: datetime) -> pd.DataFrame:
    url_get_carbon_intensity = 'https://carbon-aware-api.azurewebsites.net/emissions/bylocations'
    response = requests.get(url_get_carbon_intensity, params={
        'location': [region],
        'time': arrow.get(start).for_json(),
        'toTime': arrow.get(end).shift(minutes=-1).for_json(),
    })
    assert response.ok, "GSF carbon intensity lookup failed (%d): %s" % (response.status_code, response.text)
    if response.status_code == 204:
        return []
    try:
        response_json = response.json()
    except (ValueError, TypeError) as e:
        raise ValueError(f'Failed to read JSON: "{e}", url: "{response.request.path_url}", text: "{response.text}"')

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
    return rows


def fetch_prediction(region: str, start: datetime, end: datetime) -> tuple[pd.DataFrame, datetime]:
    url_get_carbon_intensity = 'https://carbon-aware-api.azurewebsites.net/emissions/forecasts/batch'
    response = requests.post(url_get_carbon_intensity, json=[{
        'location': region,
        'requestedAt': arrow.get().for_json(),
        'windowSize': 5,
        'dataStartAt': arrow.get(start).for_json(),
        'dataEndAt': arrow.get(end).shift(minutes=5).for_json(),
    }])
    try:
        assert response.ok
    except AssertionError:
        raise ValueError("GSF carbon forecast lookup failed (%d): %s" % (response.status_code, response.text))
    if response.status_code == 204:
        return []
    try:
        response_json = response.json()
    except (ValueError, TypeError) as e:
        raise ValueError(f'Failed to read JSON: "{e}", url: "{response.request.path_url}", text: "{response.text}"')

    if len(response_json) == 0:
        return []
    rows = []
    for response_element in response_json:
        generatedAt = arrow.get(response_element['generatedAt']).datetime
        print('generatedAt:', generatedAt)
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
    return rows

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
        return fetch_prediction(region, start, end)
    else:
        return fetch_emissions(region, start, end)
