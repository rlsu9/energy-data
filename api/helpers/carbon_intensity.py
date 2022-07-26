#!/usr/bin/env python3

import os
from typing import Tuple
import numpy as np
import psycopg2
from pathlib import Path
from datetime import datetime, timedelta
from werkzeug.exceptions import NotFound, BadRequest
from bisect import bisect_left

from api.util import loadYamlData, get_psql_connection, psql_execute_list, psql_execute_scalar, round_down


def get_map_carbon_intensity_by_fuel_source(config_path: os.path) -> dict[str, float]:
    '''Load the carbon intensity per fuel source map from config.'''
    # Load ISO-to-WattTime-BA mapping from yaml config
    yaml_data = loadYamlData(config_path)
    carbon_intensity_map_name = 'carbon_intensity_by_fuel_source'
    assert yaml_data is not None and carbon_intensity_map_name in yaml_data, \
        f'Failed to load {carbon_intensity_map_name} from config.'
    return yaml_data[carbon_intensity_map_name]

MAP_CARBON_INTENSITY_BY_FUEL_SOURCE = get_map_carbon_intensity_by_fuel_source(os.path.join(Path(__file__).parent.absolute(), 'carbon_intensity.yaml'))
DEFAULT_CARBON_INTENSITY_FOR_UNKNOWN_SOURCE = 700

def validate_region_exists(conn: psycopg2.extensions.connection, region: str) -> None:
    cursor = conn.cursor()
    region_exists = psql_execute_scalar(cursor,
        "SELECT EXISTS(SELECT 1 FROM EnergyMixture WHERE region = %s)",
        [region])
    if not region_exists:
        raise NotFound(f"Region {region} doesn't exist in database.")

def get_available_time_range(conn: psycopg2.extensions.connection, region: str) -> \
    Tuple[datetime, datetime]:
    """Get the timestamp range for which we have electricity data in given region."""
    cursor = conn.cursor()
    timestamp_min: datetime|None = psql_execute_scalar(cursor,
        "SELECT MIN(DateTime) FROM EnergyMixture WHERE Region = %s;"
        , [region])
    timestamp_max: datetime|None = psql_execute_scalar(cursor,
        "SELECT MAX(DateTime) FROM EnergyMixture WHERE Region = %s;"
        , [region])
    return (timestamp_min, timestamp_max)

def validate_time_range(conn: psycopg2.extensions.connection,
        region: str, start: datetime, end: datetime) -> None:
    """Validate we have electricity data for the given time range."""
    if start > end:
        raise BadRequest("end must be before start")
    (available_start, available_end) = get_available_time_range(conn, region)
    if start > available_end:
        raise BadRequest("Time range is too new. Data not yet available.")
    if end < available_start:
        raise BadRequest("Time range is too old. No data available.")

def get_average_carbon_intensity(conn: psycopg2.extensions.connection,
        region: str, start: datetime, end: datetime)  -> list[dict]:
    cursor = conn.cursor()
    records: list[tuple[datetime, float]] = psql_execute_list(cursor,
        """SELECT datetime, carbonintensity FROM CarbonIntensity
            WHERE region = %s AND %s <= datetime AND datetime <= %s
            ORDER BY datetime;""",
        [region, start, end])
    l_carbon_intensity = []
    for (timestamp, carbon_intensity) in records:
        l_carbon_intensity.append({
            'timestamp': timestamp,
            'carbon_intensity': carbon_intensity,
        })
    return l_carbon_intensity

def get_power_by_timemstamp_and_fuel_source(conn: psycopg2.extensions.connection,
        region: str, start: datetime, end: datetime) -> dict[datetime, dict[str, float]]:
    cursor = conn.cursor()
    records: list[tuple[str, datetime, datetime]] = psql_execute_list(cursor,
        """SELECT datetime, category, power_mw FROM EnergyMixture
            WHERE region = %s AND %s <= datetime AND datetime <= %s
            ORDER BY datetime, category;""",
        [region, start, end])
    d_power_bytimestamp_and_fuel_source: dict[datetime, dict[str, float]] = {}
    for (timestamp, category, power_mw) in records:
        if timestamp not in d_power_bytimestamp_and_fuel_source:
            d_power_bytimestamp_and_fuel_source[timestamp] = {}
        d_power_bytimestamp_and_fuel_source[timestamp][category] = power_mw
    return d_power_bytimestamp_and_fuel_source

def calculate_average_carbon_intensity(
        power_by_timestamp_and_fuel_source: dict[datetime, dict[str, float]]) -> list[dict]:
    l_carbon_intensity_by_timestamp = []
    for timestamp, power_by_fuel_source in power_by_timestamp_and_fuel_source.items():
        l_carbon_intensity = []
        l_weight = []   # aka power in MW
        for fuel_source, power_in_mw in power_by_fuel_source.items():
            if fuel_source not in MAP_CARBON_INTENSITY_BY_FUEL_SOURCE:
                carbon_intensity = DEFAULT_CARBON_INTENSITY_FOR_UNKNOWN_SOURCE
            else:
                carbon_intensity = MAP_CARBON_INTENSITY_BY_FUEL_SOURCE[fuel_source]
            l_carbon_intensity.append(carbon_intensity)
            l_weight.append(power_in_mw)
        average_carbon_intensity = np.average(l_carbon_intensity, weights=l_weight)
        l_carbon_intensity_by_timestamp.append({
            'timestamp': timestamp,
            'carbon_intensity': average_carbon_intensity,
        })
    return l_carbon_intensity_by_timestamp

def get_carbon_intensity_list(region: str, start: datetime, end: datetime) -> list[dict]:
    """Retrieve the carbon intensity time series data in the given time window.

        Args:
            region: the ISO region name.
            start/end: the time window.

        Returns:
            A list of time series data.
    """
    conn = get_psql_connection()
    validate_region_exists(conn, region)
    validate_time_range(conn, region, start, end)
    return get_average_carbon_intensity(conn, region, start, end)
    # power_by_fuel_source = get_power_by_timemstamp_and_fuel_source(conn, region, start, end)
    # return calculate_average_carbon_intensity(power_by_fuel_source)

def convert_carbon_intensity_list_to_dict(l_carbon_intensity: list[dict])-> dict[datetime, float]:
    d_carbon_intensity_by_timestamp: dict[datetime, float] = {}
    for d in l_carbon_intensity:
        timestamp = d['timestamp']
        carbon_intensity = d['carbon_intensity']
        d_carbon_intensity_by_timestamp[timestamp] = carbon_intensity
    return d_carbon_intensity_by_timestamp

def get_carbon_intensity_interval(timestamps: list[datetime]) -> timedelta:
    """Deduce the interval from a series of timestamps returned from the database."""
    if len(timestamps) == 0:
        raise ValueError("Invalid argument: empty list.")
    values, counts = np.unique(timestamps, return_counts=True)
    return values[np.argmax(counts)]

def calculate_total_carbon_emissions(start: datetime, end: datetime, power: float,
        carbon_intensity_by_timestamp: dict[datetime, float]) -> float:
    """Calculate the total carbon emission by multiplying energy with carbon intensity.

        Args:
            start/end: start and end time of a workload.
            power: average power in watt.
            carbon_intensity_by_timestamp: a timeseries carbon intensity data in gCO2/kWh.
        
        Returns:
            Total carbon emissions in kgCO2.
    """
    if (start < end):
        raise BadRequest("start time is less than end time")

    l_timestamps = sorted(carbon_intensity_by_timestamp.keys())
    l_carbon_intensity = [carbon_intensity_by_timestamp[timestamp] for timestamp in l_timestamps]
    average_interval = get_carbon_intensity_interval(l_timestamps)
    # round down start/end as carbon intensity data starts at mostly aligned intervals
    end_rounded = round_down(end, average_interval)

    if start < min(l_timestamps) or end_rounded > max(l_timestamps):
        raise NotFound("Missing carbon intensity data for the given time interval.")

    def _calculate_carbon_emission_in_interval(start: datetime, end: datetime, carbon_intensity: float) -> float:
        """Calculate carbon emission in a small interval with fixed carbon intensity."""
        # This converts s * W * gCO2/kWh to h * kW * kgCO2/kWh (or kgCO2)
        conversion_factor = timedelta(hours=1).total_seconds() * 1000 * 1000
        return (end - start).total_seconds() * power * carbon_intensity / conversion_factor

    index_start = bisect_left(l_timestamps, start)
    index_end = bisect_left(l_timestamps, end)
    if index_start == index_end:
        carbon_intensity = l_carbon_intensity[index_start]
        return _calculate_carbon_emission_in_interval(start, end, carbon_intensity)

    total_carbon_emissions = 0.
    # Partial starting interval
    total_carbon_emissions += _calculate_carbon_emission_in_interval(
        start,
        l_timestamps[index_start + 1],
        l_carbon_intensity[index_start])

    # Whole intervals in the middle
    index_interval = index_start + 1
    while index_interval < index_end:
        total_carbon_emissions += _calculate_carbon_emission_in_interval(
            l_timestamps[index_interval],
            l_timestamps[index_interval + 1],
            l_carbon_intensity[index_interval])
        index_interval += 1

    # Partial ending interval
    total_carbon_emissions += _calculate_carbon_emission_in_interval(
        l_timestamps[index_end],
        end,
        l_carbon_intensity[index_end])

    return total_carbon_emissions
