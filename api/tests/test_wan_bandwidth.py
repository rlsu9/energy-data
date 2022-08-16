#!/usr/bin/env python3

from datetime import time
from api.models.wan_bandwidth import load_wan_bandwidth_model
from api.util import Rate


def test_wan_bandwidth_lookup_by_index():
    wan_bandwidth = load_wan_bandwidth_model()
    num_datapoints = len(wan_bandwidth.available_bandwidth.values)
    assert all([wan_bandwidth.available_bandwidth_at(i) >= Rate(0) for i in range(num_datapoints)])


def test_wan_bandwidth_lookup_by_timestamp():
    wan_bandwidth = load_wan_bandwidth_model()
    # Each group should have equal value as they lie in the same 5-minute interval
    l_timestamp_group = [
        [
            time(minute=0),
            time(minute=1, second=25),
            time(minute=3),
            time(minute=4, second=59),
        ],
        [
            time(minute=5, second=1),
            time(minute=10, second=0),
        ],
        [
            time(minute=15, second=45),
            time(minute=15, second=1),
        ],
        [
            time(hour=2, minute=33, second=57),
            time(hour=2, minute=35, second=0),
        ],
        [
            time(hour=23, minute=55, second=1),
            time(hour=23, minute=59, second=59),
        ],
    ]
    for timestamp_group in l_timestamp_group:
        s_available_bandwidth = set()
        for timestamp in timestamp_group:
            available_bandwidth = wan_bandwidth.available_bandwidth_at(timestamp=timestamp)
            assert available_bandwidth >= Rate(0)
            s_available_bandwidth.add(available_bandwidth)
        assert len(s_available_bandwidth) == 1, f'Diffrent values for timestamps: {timestamp_group}'
