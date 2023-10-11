#!/usr/bin/env python3

from enum import Enum
import math
import numpy as np
import pandas as pd
from pandas.tseries.frequencies import to_offset
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
    current_app.logger.info('Calculating total carbon emissions ...')
    if runtime <= timedelta():
        raise BadRequest("Runtime must be positive.")

    if input_transfer_time + output_transfer_time > max_delay:
        raise ValueError("Not enough time to finish before deadline.")


    def _calculate_carbon_emission_across_intervals(start: datetime, end: datetime, carbon_emission_rates: pd.Series) -> float:
        if carbon_emission_rates.empty or start >= end:
            return 0.
        rates_sc = sc.Stairs.from_values(initial_value=0, values=carbon_emission_rates)
        # rates_sc = rates_sc.slice(pd.date_range(start, end))
        rates_sc = rates_sc.clip(start, end)
        return rates_sc.integral() / timedelta(seconds=1)

    def _calculate_total_emission(curr_wait_times: list[datetime], breakdown=False):
        (input_wait, compute_wait, output_wait) = curr_wait_times
        input_transfer_start = start + input_wait
        input_transfer_end = input_transfer_start + input_transfer_time
        compute_start = input_transfer_end + compute_wait
        compute_end = compute_start + runtime
        output_transfer_start = compute_end + output_wait
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

    def _get_marginal_emission_rate_delta_and_step_size(curr_wait_times: list[datetime],
                                                        moving_index: int) -> tuple[float, timedelta]:
        """Calculate the total marginal emission rate delta by moving the n-th wait time and the minimum step size across all three steps."""
        def _impl_single_interval(start: datetime, end: datetime, carbon_emission_rates: pd.Series):
            """Calculates the marginal emission rate delta and step size for a single interval."""
            if carbon_emission_rates.empty:
                return 0., timedelta(days=365)
            def _get_value_at_timestamp(target: datetime) -> float:
                return carbon_emission_rates.loc[carbon_emission_rates.index >= target][0]
            def _get_next_timestamp(target: datetime) -> datetime:
                filtered = carbon_emission_rates.iloc[carbon_emission_rates.index > target].index
                if filtered.size == 0:
                    # Return end of the time series
                    carbon_emission_rates_freq = pd.infer_freq(carbon_emission_rates.index)
                    end_time_of_series = carbon_emission_rates.index.max() + to_offset(carbon_emission_rates_freq)
                    return end_time_of_series.to_pydatetime()
                else:
                    return filtered[0]
            marginal_rate_start = _get_value_at_timestamp(start)
            marginal_rate_end = _get_value_at_timestamp(end)
            step_size_start = _get_next_timestamp(start) - start
            step_size_end = _get_next_timestamp(end) - end
            return marginal_rate_end - marginal_rate_start, min(step_size_start, step_size_end)

        assert moving_index >= 0 and moving_index < NUM_TIME_VARIABLES, "Invalid moving index."

        (input_wait, compute_wait, output_wait) = curr_wait_times
        input_transfer_start = start + input_wait
        input_transfer_end = input_transfer_start + input_transfer_time
        compute_start = input_transfer_end + compute_wait
        compute_end = compute_start + runtime
        output_transfer_start = compute_end + output_wait
        output_transfer_end = output_transfer_start + output_transfer_time

        sum_rate_delta = 0
        min_step_size = timedelta(days=365)
        # Moving the first time afects the latter two steps, and moving the second time affects the last step.
        if moving_index <= 0:
            input_rate_delta, input_step_size = _impl_single_interval(input_transfer_start, input_transfer_end,
                                                                      transfer_carbon_intensity_rates)
            sum_rate_delta += input_rate_delta
            min_step_size = min(min_step_size, input_step_size)
        if moving_index <= 1:
            compute_rate_delta, compute_step_size = _impl_single_interval(compute_start, compute_end,
                                                                          compute_carbon_emission_rates)
            sum_rate_delta += compute_rate_delta
            min_step_size = min(min_step_size, compute_step_size)
        if moving_index <= 2:
            output_rate_delta, output_step_size = _impl_single_interval(output_transfer_start, output_transfer_end,
                                                                        transfer_carbon_intensity_rates)
            sum_rate_delta += output_rate_delta
            min_step_size = min(min_step_size, output_step_size)

        return sum_rate_delta, min_step_size

    def _advance_wait_times_and_get_emission_delta(curr_wait_times: list[timedelta],
                      total_wait_limit: timedelta) -> tuple[list[timedelta], float]:
        """Advance the wait time to the next step and return the emission delta."""
        moving_index = NUM_TIME_VARIABLES - 1
        marginal_emission_rate_delta, step_size = _get_marginal_emission_rate_delta_and_step_size(curr_wait_times, moving_index)
        if sum(curr_wait_times, timedelta()) + step_size <= total_wait_limit:
            curr_wait_times[moving_index] += step_size
            emission_delta = marginal_emission_rate_delta * step_size.total_seconds()
            return emission_delta
        else:
            while moving_index > 0:
                curr_wait_times[moving_index] = timedelta()
                moving_index -= 1
                _, step_size = _get_marginal_emission_rate_delta_and_step_size(curr_wait_times, moving_index)
                if sum(curr_wait_times, timedelta()) + step_size <= total_wait_limit:
                    curr_wait_times[moving_index] += step_size
                    return math.nan
            curr_wait_times = [timedelta()] * len(curr_wait_times)
            raise StopIteration()

    NUM_TIME_VARIABLES = 3  # input wait, compute wait and output wait
    t_total_wait_limit = max_delay - input_transfer_time - output_transfer_time

    curr_wait_times = [timedelta()] * NUM_TIME_VARIABLES
    current_emission = _calculate_total_emission(curr_wait_times)
    min_time_values = curr_wait_times.copy()
    min_total_emission = current_emission

    try:
        while True:
            emission_delta = _advance_wait_times_and_get_emission_delta(curr_wait_times, t_total_wait_limit)
            if math.isnan(emission_delta):
                # Re-calculate total emission is delta is unknown
                current_emission = _calculate_total_emission(curr_wait_times)
            else:
                current_emission += emission_delta
            if current_emission < min_total_emission:
                min_total_emission = current_emission
                min_time_values = curr_wait_times.copy()
    except StopIteration:
        pass

    curr_wait_times = min_time_values.copy()
    return (_calculate_total_emission(curr_wait_times, True), min_time_values)
