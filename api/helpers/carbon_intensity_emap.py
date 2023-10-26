#!/usr/bin/env python3

from datetime import datetime, timedelta, timezone
from flask import current_app
import requests
import arrow

from api.models.common import ISO_PREFIX_EMAP
from api.util import carbon_data_cache

def get_emap_region_from_iso(iso: str) -> str:
    """Transform ISO region to electricity map region."""
    if iso.startswith(ISO_PREFIX_EMAP):
        return iso.removeprefix(ISO_PREFIX_EMAP)
    raise NotImplementedError(f'Unknown EMAP region for iso {iso}')

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
    region = get_emap_region_from_iso(iso)
    if use_prediction:
        success, result_or_error = fetch_prediction(region, start, end)
    else:
        success, result_or_error = fetch_emissions(region, start, end)
    if success:
        return result_or_error
    else:
        raise ValueError('Failed to get carbon intensity: ' + result_or_error)

@carbon_data_cache.memoize()
def fetch_prediction(region: str, start: datetime, end: datetime) -> list[dict]:
    current_app.logger.debug(f'fetch_prediction({region}, {start}, {end})')
    raise ValueError('Electricit map carbon data source does not support prediction')

@carbon_data_cache.memoize()
def fetch_emissions(region: str, start: datetime, end: datetime) -> list[dict]:
    current_app.logger.debug(f'fetch_emissions({region}, {start}, {end})')
    # TODO: implement this.
    raise NotImplementedError('To be implemented')
