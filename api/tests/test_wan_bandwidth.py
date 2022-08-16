#!/usr/bin/env python3

from api.models.wan_bandwidth import load_wan_bandwidth_model


def test_wan_bandwidth_data():
    wan_bandwidth = load_wan_bandwidth_model()
    num_datapoints = len(wan_bandwidth.available_bandwidth.values)
    assert all([wan_bandwidth.available_bandwidth_at(i) >= 0 for i in range(num_datapoints)])
