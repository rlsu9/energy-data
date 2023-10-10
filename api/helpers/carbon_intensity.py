#!/usr/bin/env python3

from enum import Enum
from itertools import product
from math import ceil
import sys
import numpy as np
import pandas as pd
import staircase as sc
from datetime import datetime, timedelta
from datetime import datetime, timedelta
from werkzeug.exceptions import BadRequest
from flask import current_app

from api.helpers.carbon_intensity_c3lab import get_carbon_intensity_list as get_carbon_intensity_list_c3lab
from api.helpers.carbon_intensity_azure import get_carbon_intensity_list as get_carbon_intensity_list_azure


class CarbonDataSource(str, Enum):
    C3Lab = "c3lab"
    Azure = "azure"


def get_carbon_intensity_list(iso: str, start: datetime, end: datetime,
        carbon_data_source: CarbonDataSource, use_prediction: bool,
        desired_renewable_ratio: float = None) -> list[dict]:
    """Retrieve the carbon intensity time series data in the given time window.

        Args:
            iso: the ISO region name.
            start: the start time.
            end: the end time.
            carbon_data_source: the source of the carbon data.
            use_prediction: whether to use prediction or actual data.

        Returns:
            A list of time series data.
    """
    current_app.logger.info(f'Getting carbon intensity for {iso} in range ({start}, {end})')
    match carbon_data_source:
        case CarbonDataSource.C3Lab:
            return get_carbon_intensity_list_c3lab(iso, start, end, use_prediction, desired_renewable_ratio)
        case CarbonDataSource.Azure:
            if desired_renewable_ratio is not None:
                raise ValueError('Azure carbon data source does not support custom renewable ratio.')
            return get_carbon_intensity_list_azure(iso, start, end, use_prediction)
        case _:
            raise NotImplementedError()

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
        raise ValueError("Invalid argument: empty carbon intensity list.")
    if len(timestamps) == 1:
        return timedelta(hours=1)
    timestamp_deltas = np.diff(timestamps)
    values, counts = np.unique(timestamp_deltas, return_counts=True)
    return values[np.argmax(counts)]


def calculate_total_carbon_emissions(start: datetime, runtime: timedelta,
                                     max_delay: timedelta,
                                     input_transfer_time: timedelta,
                                     output_transfer_time: timedelta,
                                     compute_carbon_emission_rates: pd.Series,
                                     transfer_carbon_intensity_rates: pd.Series,
                                     ) -> tuple[float, timedelta]:
    """Calculate the total carbon emission, including both compute and data transfer emissions.

        Args:
            start: start time of a workload.
            runtime: runtime of a workload.
            max_delay: the amount of delay that a workload can tolerate.
            transfer_input_time: time to transfer input data.
            transfer_output_time: time to transfer output data.
            compute_carbon_intensity_by_timestamp: the compute carbon emission rate in gCO2/s.
            transfer_carbon_intensity_by_timestamp: the aggregated data transfer carbon emission rate in gCO2/s.

        Returns:
            Total carbon emissions in kgCO2.
            Optimal delay of start time, if applicable.
    """
    if runtime <= timedelta():
        raise BadRequest("Runtime must be positive.")

    def _calculate_carbon_emission_across_intervals(start: datetime, end: datetime, carbon_emission_rates: pd.Series) -> float:
        if carbon_emission_rates.empty or start >= end:
            return 0.
        rates_sc = sc.Stairs.from_values(initial_value=0, values=carbon_emission_rates)
        # rates_sc = rates_sc.slice(pd.date_range(start, end))
        rates_sc = rates_sc.clip(start, end)
        return rates_sc.integral() / timedelta(seconds=1)

    def _calculate_carbon_emission_in_interval(interval: timedelta, carbon_emission_rate: float) -> float:
        """Calculate carbon emission in a small interval with fixed carbon emission rate (gCO2/s)."""
        return interval.total_seconds() * carbon_emission_rate

    if input_transfer_time + output_transfer_time > max_delay:
        raise ValueError("Not enough time to finish before deadline.")

    # Sliding window algorithm using timestamp directly
    t_total_wait_limit = max_delay - input_transfer_time - output_transfer_time
    t_wait_times_minutes = [0, 0, 0]    # input wait, compute wait and output wait

    def _calculate_total_emission(breakdown=False) -> float:
        (m_input_wait, m_compute_wait, m_output_wait) = curr_wait_times_minutes.tolist()
        input_transfer_start = start + timedelta(minutes=m_input_wait)
        input_transfer_end = input_transfer_start + input_transfer_time
        compute_start = input_transfer_end + timedelta(minutes=m_compute_wait)
        compute_end = compute_start + runtime
        output_transfer_start = compute_end + timedelta(minutes=m_output_wait)
        output_transfer_end = output_transfer_start + output_transfer_time
        input_transfer_emission = _calculate_carbon_emission_across_intervals(
                input_transfer_start,
                input_transfer_end,
                transfer_carbon_intensity_rates)
        compute_emission = _calculate_carbon_emission_across_intervals(
                compute_start,
                compute_end,
                compute_carbon_emission_rates)
        output_transfer_emission = _calculate_carbon_emission_across_intervals(
                output_transfer_start,
                output_transfer_end,
                transfer_carbon_intensity_rates)
        if breakdown:
            return (compute_emission , input_transfer_emission + output_transfer_emission)
        else:
            return input_transfer_emission + compute_emission + output_transfer_emission

    def _get_marginal_emission_rate_delta_and_step_size(curr_wait_times_minutes, moving_index: int) -> tuple[float, timedelta]:
        def _impl_single_interval(start: datetime, end: datetime, carbon_emission_rates: pd.Series):
            """Calculates the marginal emission rate delta and step size for a single interval."""
            if carbon_emission_rates.empty:
                return 0., timedelta(days=365)
            def _get_value_at_timestamp(target: datetime) -> float:
                return carbon_emission_rates.loc[carbon_emission_rates.index >= target][0]
            def _get_next_timestamp(target: datetime) -> datetime:
                return carbon_emission_rates.iloc[carbon_emission_rates.index > target].index[0]
            marginal_rate_start = _get_value_at_timestamp(start)
            marginal_rate_end = _get_value_at_timestamp(end)
            step_size_start = _get_next_timestamp(start) - start
            step_size_end = _get_next_timestamp(end) - end
            return marginal_rate_end - marginal_rate_start, min(step_size_start, step_size_end)

        (m_input_wait, m_compute_wait, m_output_wait) = curr_wait_times_minutes.tolist()
        input_transfer_start = start + timedelta(minutes=m_input_wait)
        input_transfer_end = input_transfer_start + input_transfer_time
        compute_start = input_transfer_end + timedelta(minutes=m_compute_wait)
        compute_end = compute_start + runtime
        output_transfer_start = compute_end + timedelta(minutes=m_output_wait)
        output_transfer_end = output_transfer_start + output_transfer_time
        input_rate_delta, input_step_size = _impl_single_interval(input_transfer_start, input_transfer_end,
                                                                  transfer_carbon_intensity_rates)
        compute_rate_delta, compute_step_size = _impl_single_interval(compute_start, compute_end,
                                                                      compute_carbon_emission_rates)
        output_rate_delta, output_step_size = _impl_single_interval(output_transfer_start, output_transfer_end,
                                                                    transfer_carbon_intensity_rates)
        if moving_index == 0:
            return input_rate_delta + compute_rate_delta + output_rate_delta, min(input_step_size, compute_step_size, output_step_size)
        elif moving_index == 1:
            return compute_rate_delta + output_rate_delta, min(compute_step_size, output_step_size)
        elif moving_index == 2:
            return output_rate_delta, output_step_size
        else:
            raise ValueError('Invalid moving index.')

    def _update_emission_values():
        nonlocal min_total_emission, min_time_values, prev_wait_times_minutes, saved_emissions
        if current_emission < min_total_emission:
            min_total_emission = current_emission
            min_time_values = curr_wait_times_minutes.copy()
        prev_wait_times_minutes = curr_wait_times_minutes.copy()
        # saved_emissions[tuple(curr_wait_times_minutes)] = current_emission

    NUM_TIME_VARIABLES = 3  # input wait, compute wait and output wait

    t_wait_times_minutes = [0] * NUM_TIME_VARIABLES
    curr_wait_times_minutes = np.array(t_wait_times_minutes)
    prev_wait_times_minutes = curr_wait_times_minutes.copy()

    current_emission = _calculate_total_emission()
    min_total_emission = current_emission
    min_time_values = curr_wait_times_minutes.copy()

    saved_emissions = {}
    total_wait_limit_minutes = int(ceil(t_total_wait_limit.total_seconds() / timedelta(minutes=1).total_seconds()))
    # Ignore transfer time variables update if transfer is not applicable.
    # This is not ideal... Need a way to generate this on the fly.
    if transfer_carbon_intensity_rates.empty:
        all_wait_time_combinations = map(lambda t: (0, t, 0), range(total_wait_limit_minutes + 1))
    else:
        all_wait_time_combinations = product(range(total_wait_limit_minutes + 1), repeat=NUM_TIME_VARIABLES)
    for t_wait_times_minutes in all_wait_time_combinations:
        if sum(t_wait_times_minutes) > total_wait_limit_minutes:
            continue
        curr_wait_times_minutes = np.array(t_wait_times_minutes, dtype=int)
        if curr_wait_times_minutes.tolist() <= prev_wait_times_minutes.tolist():
            continue
        delta_wait_times_minutes = curr_wait_times_minutes - prev_wait_times_minutes
        print(t_wait_times_minutes, file=sys.stderr)
        moving_index = (delta_wait_times_minutes > 0).tolist().index(True)
        assert 0 <= moving_index < NUM_TIME_VARIABLES, "Invalid moving index."
        if np.sum(delta_wait_times_minutes == 0) != 2:
            # Find out the next step of the moving index.
            curr_wait_times_minutes[moving_index] -= delta_wait_times_minutes[moving_index]
            marginal_emission_rate, step_size = _get_marginal_emission_rate_delta_and_step_size(curr_wait_times_minutes, moving_index)
            curr_wait_times_minutes[moving_index] += int(ceil(step_size.total_seconds() / timedelta(minutes=1).total_seconds()))
            current_emission = _calculate_total_emission()
            _update_emission_values()
        else:
            marginal_emission_rate, step_size = _get_marginal_emission_rate_delta_and_step_size(prev_wait_times_minutes, moving_index)
            current_emission += marginal_emission_rate * step_size.total_seconds()
            curr_wait_times_minutes[moving_index] += int(step_size.total_seconds() / timedelta(minutes=1).total_seconds())
            _update_emission_values()

    curr_wait_times_minutes = min_time_values.copy()
    return (_calculate_total_emission(True), min_time_values.tolist())
