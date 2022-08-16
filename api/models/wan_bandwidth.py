#!/usr/bin/env python3
import bisect
import os
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Union

from api.models.timeseries import TimeSeriesData
from api.util import xor, load_yaml_data, timedelta_to_time


class SimpleWANBandwidth:
    """This models the available wide-area network (WAN) bandwidth with a simple diurnal curve.

        Borrowing the simple example from TODO: TBD, and based on the 95%-tile pricing model,
            this model favors night time as more bandwidth is available.
    """

    def __init__(self, available_bandwidth: TimeSeriesData, concurrent_transfers=100):
        self.available_bandwidth = available_bandwidth
        self.concurrent_transfers = concurrent_transfers

    def available_bandwidth_at(self, index=-1, timestamp: Union[datetime | time] = None):
        if not xor(index >= 0, timestamp is not None):
            raise ValueError("One of index and timestamp is required")
        if timestamp is not None:
            # index:                        0 ..  1 ..  2 ...
            # interval starting at minute:  0 ..  5 .. 10 ...
            # bisect_right() picks the right side, then minus 1 to go left, e.g. 3min -> 0, 5min -> 1
            index = bisect.bisect(self.available_bandwidth.timestamps, timestamp) - 1
            if index < 0 or index >= len(self.available_bandwidth.timestamps):
                raise ValueError("timestamp out of range in WAN bandwidth data")
        return self.available_bandwidth.values[index]


def load_wan_bandwidth_model():
    config_path = os.path.join(Path(__file__).parent.absolute(), 'wan_bandwidth_data.yaml')
    yaml_data = load_yaml_data(config_path)
    traffic_data_5min_list_name = 'total_traffic_every_5min'
    assert yaml_data is not None and traffic_data_5min_list_name in yaml_data, \
        f'Failed to load {traffic_data_5min_list_name}'
    l_traffic_data_5min = yaml_data[traffic_data_5min_list_name]

    data_interval = timedelta(minutes=5)
    assert len(l_traffic_data_5min) == round(timedelta(days=1) / data_interval)

    l_times = [timedelta_to_time(data_interval * i) for i in range(len(l_traffic_data_5min))]
    max_usage = max(l_traffic_data_5min)
    l_available_bandwidth = [max_usage - usage for usage in l_traffic_data_5min]
    available_bandwidth = TimeSeriesData(l_times, l_available_bandwidth)

    return SimpleWANBandwidth(available_bandwidth)
