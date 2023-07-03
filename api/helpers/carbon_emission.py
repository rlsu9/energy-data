#!/usr/bin/env python3

from typing import Tuple
from datetime import datetime, timedelta
from werkzeug.exceptions import NotFound, BadRequest
from bisect import bisect
import numpy as np

from api.helpers.carbon_intensity import get_carbon_intensity_interval
from api.util import round_down


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
