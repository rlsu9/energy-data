#!/usr/bin/env python3

import os
from typing import Tuple
import numpy as np
import psycopg2
from pathlib import Path
from datetime import datetime, timedelta
from werkzeug.exceptions import NotFound, BadRequest
from bisect import bisect

from api.util import load_yaml_data, get_psql_connection, psql_execute_list, psql_execute_scalar, round_down


def get_map_carbon_intensity_by_fuel_source(config_path: os.path) -> dict[str, float]:
    """Load the carbon intensity per fuel source map from config."""
    # Load ISO-to-WattTime-BA mapping from yaml config
    yaml_data = load_yaml_data(config_path)
    carbon_intensity_map_name = 'carbon_intensity_by_fuel_source'
    assert yaml_data is not None and carbon_intensity_map_name in yaml_data, \
        f'Failed to load {carbon_intensity_map_name} from config.'
    return yaml_data[carbon_intensity_map_name]


MAP_CARBON_INTENSITY_BY_FUEL_SOURCE = get_map_carbon_intensity_by_fuel_source(
    os.path.join(Path(__file__).parent.absolute(), 'carbon_intensity.yaml'))
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
    timestamp_min: datetime | None = psql_execute_scalar(cursor,
                                                         "SELECT MIN(DateTime) FROM EnergyMixture WHERE Region = %s;",
                                                         [region])
    timestamp_max: datetime | None = psql_execute_scalar(cursor,
                                                         "SELECT MAX(DateTime) FROM EnergyMixture WHERE Region = %s;",
                                                         [region])
    return timestamp_min, timestamp_max


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
                                 region: str, start: datetime, end: datetime) -> list[dict]:
    cursor = conn.cursor()
    # in case start/end lie in between two timestamps, find the timestamp <= start and >= end.
    records: list[tuple[datetime, float]] = psql_execute_list(cursor,
                                                              """SELECT datetime, carbonintensity FROM CarbonIntensity
            WHERE region = %(region)s
                AND datetime >= (SELECT COALESCE(
                    (SELECT MAX(datetime) FROM EnergyMixture
                        WHERE datetime <= %(start)s AND region = %(region)s),
                    (SELECT MIN(datetime) FROM EnergyMixture
                        WHERE region = %(region)s)))
                AND datetime <= (SELECT COALESCE(
                    (SELECT MIN(datetime) FROM EnergyMixture
                        WHERE datetime >= %(end)s AND region = %(region)s),
                    (SELECT MAX(datetime) FROM EnergyMixture
                        WHERE region = %(region)s)))
            ORDER BY datetime;""",
                                                              dict(region=region, start=start, end=end))
    l_carbon_intensity = []
    for (timestamp, carbon_intensity) in records:
        l_carbon_intensity.append({
            'timestamp': timestamp,
            'carbon_intensity': carbon_intensity,
        })
    return l_carbon_intensity


def get_power_by_timestamp_and_fuel_source(conn: psycopg2.extensions.connection,
                                           region: str, start: datetime, end: datetime) -> \
        dict[datetime, dict[str, float]]:
    cursor = conn.cursor()
    records: list[tuple[str, datetime, datetime]] = psql_execute_list(
        cursor,
        """SELECT datetime, category, power_mw FROM EnergyMixture
            WHERE region = %s AND %s <= datetime AND datetime <= %s
            ORDER BY datetime, category;""",
        [region, start, end])
    d_power_by_timestamp_and_fuel_source: dict[datetime, dict[str, float]] = {}
    for (timestamp, category, power_mw) in records:
        if timestamp not in d_power_by_timestamp_and_fuel_source:
            d_power_by_timestamp_and_fuel_source[timestamp] = {}
        d_power_by_timestamp_and_fuel_source[timestamp][category] = power_mw
    return d_power_by_timestamp_and_fuel_source


def calculate_average_carbon_intensity(
        power_by_timestamp_and_fuel_source: dict[datetime, dict[str, float]]) -> list[dict]:
    l_carbon_intensity_by_timestamp = []
    for timestamp, power_by_fuel_source in power_by_timestamp_and_fuel_source.items():
        l_carbon_intensity = []
        l_weight = []  # aka power in MW
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
            start: the start time.
            end: the end time.

        Returns:
            A list of time series data.
    """
    conn = get_psql_connection()
    validate_region_exists(conn, region)
    validate_time_range(conn, region, start, end)
    return get_average_carbon_intensity(conn, region, start, end)
    # power_by_fuel_source = get_power_by_timestamp_and_fuel_source(conn, region, start, end)
    # return calculate_average_carbon_intensity(power_by_fuel_source)


def get_power_by_fuel_type(region: str, start: datetime, end: datetime) -> list[dict]:
    """Retrieves the raw power (in MW) broken down by timestamp and fuel type."""
    conn = get_psql_connection()
    validate_region_exists(conn, region)
    validate_time_range(conn, region, start, end)
    d_timestamp_fuel_power = get_power_by_timestamp_and_fuel_source(conn, region, start, end)
    result = []
    for timestamp in d_timestamp_fuel_power:
        l_fuel_mix = []
        for fuel in d_timestamp_fuel_power[timestamp]:
            power_mw = d_timestamp_fuel_power[timestamp][fuel]
            l_fuel_mix.append({
                'type': fuel,
                'power_mw': power_mw
            })
        result.append({
            'timestamp': timestamp,
            'values': l_fuel_mix
        })
    return result


def convert_carbon_intensity_list_to_dict(l_carbon_intensity: list[dict]) -> dict[datetime, float]:
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
    if len(timestamps) == 1:
        return timedelta(hours=1)
    timestamp_deltas = np.diff(timestamps)
    values, counts = np.unique(timestamp_deltas, return_counts=True)
    return values[np.argmax(counts)]


def calculate_total_carbon_emissions(start: datetime, end: datetime, power: float,
                                     carbon_intensity_by_timestamp: dict[datetime, float],
                                     max_delay: timedelta = timedelta()) -> Tuple[float, timedelta]:
    """Calculate the total carbon emission by multiplying energy with carbon intensity.

        Args:
            start: start time of a workload.
            end: end time of a workload.
            power: average power in watt.
            carbon_intensity_by_timestamp: a timeseries carbon intensity data in gCO2/kWh.
            max_delay: the amount of delay that a workload can tolerate.

        Returns:
            Total carbon emissions in kgCO2.
            Optimal delay of start time, if applicable.
    """
    if start > end:
        raise BadRequest("start time is later than end time")

    # Convert dict to lists for easier indexing and searching
    l_timestamps = sorted(carbon_intensity_by_timestamp.keys())
    l_carbon_intensity = [carbon_intensity_by_timestamp[timestamp] for timestamp in l_timestamps]

    # Check if we have carbon intensity data for the requested range
    # round down as carbon intensity data starts at mostly aligned intervals
    end_rounded = round_down(end, get_carbon_intensity_interval(l_timestamps))
    if start < min(l_timestamps) or end_rounded > max(l_timestamps):
        raise NotFound("Missing carbon intensity data for the given time interval.")

    def _calculate_carbon_emission_in_interval(interval: timedelta, carbon_intensity: float) -> float:
        """Calculate carbon emission in a small interval with fixed carbon intensity."""
        # This converts s * W * gCO2/kWh to h * kW * kgCO2/kWh (or kgCO2)
        conversion_factor = timedelta(hours=1).total_seconds() * 1000 * 1000
        return interval.total_seconds() * power * carbon_intensity / conversion_factor

    def _find_timestamp_index(timestamp: datetime) -> int:
        """Find the timestamp index in carbon intensity timestamp list, or the closest one to the left."""
        return bisect(l_timestamps, timestamp) - 1

    index_start = _find_timestamp_index(start)
    index_end = _find_timestamp_index(end)
    if index_start == index_end:  # start and end lie in one interval
        carbon_intensity = l_carbon_intensity[index_start]
        return _calculate_carbon_emission_in_interval(end - start, carbon_intensity), timedelta()

    # Calculate total carbon emissions with unaligned interval
    total_carbon_emissions = 0.

    # Partial starting interval
    total_carbon_emissions += _calculate_carbon_emission_in_interval(
        l_timestamps[index_start + 1] - start,
        l_carbon_intensity[index_start])

    # Whole intervals in the middle
    index_interval = index_start + 1
    while index_interval < index_end:
        total_carbon_emissions += _calculate_carbon_emission_in_interval(
            l_timestamps[index_interval + 1] - l_timestamps[index_interval],
            l_carbon_intensity[index_interval])
        index_interval += 1

    # Partial ending interval
    total_carbon_emissions += _calculate_carbon_emission_in_interval(
        end - l_timestamps[index_end],
        l_carbon_intensity[index_end])

    if max_delay == timedelta():
        return total_carbon_emissions, timedelta()

    # Check if delaying the workload can result in carbon savings
    def _min_distance_to_next_interval(start: datetime, end: datetime):
        """Get the minimum (out of start and end) step size to next carbon intensity interval start."""
        return min(
            l_timestamps[_find_timestamp_index(start) + 1] - start,
            l_timestamps[_find_timestamp_index(end) + 1] - end,
        )

    # Sliding window minimum sum with continuous axis (timestamp)
    #   but fixed value (carbon intensity) within small intervals.
    # Basic idea: slide till either left or right of interval changes carbon intensity value, and apply the diff
    #   during that small step size.
    delay = timedelta()
    l_delay = [delay]
    cumulative_carbon_emission_delta = 0.
    l_cumulative_carbon_emission_delta = [cumulative_carbon_emission_delta]
    while delay < max_delay:
        step_size = _min_distance_to_next_interval(start + delay, end + delay)
        if delay + step_size > max_delay:
            step_size = max_delay - delay
        # Carbon intensity values are to the left of the current timestamp, i.e. last window
        carbon_intensity_left = l_carbon_intensity[_find_timestamp_index(start + l_delay[-1])]
        carbon_intensity_right = l_carbon_intensity[_find_timestamp_index(end + + l_delay[-1])]
        carbon_intensity_diff = carbon_intensity_right - carbon_intensity_left
        # Translate carbon intensity diff to carbon emission diff
        carbon_emission_delta = _calculate_carbon_emission_in_interval(step_size, carbon_intensity_diff)
        # Record data and iterate. Emission diff is applied after the current step; thus adding to delay first.
        delay += step_size
        l_delay.append(delay)
        cumulative_carbon_emission_delta += carbon_emission_delta
        l_cumulative_carbon_emission_delta.append(cumulative_carbon_emission_delta)
    index_min_carbon_emission_delta = np.argmin(l_cumulative_carbon_emission_delta)
    min_carbon_emission_delta = l_cumulative_carbon_emission_delta[index_min_carbon_emission_delta]
    min_carbon_emission = total_carbon_emissions + min_carbon_emission_delta
    optimal_delay = l_delay[index_min_carbon_emission_delta]
    return min_carbon_emission, optimal_delay
