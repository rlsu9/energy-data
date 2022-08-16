#!/usr/bin/env python3

from typing import Any, Union
from datetime import datetime, time


class TimeSeriesData:
    """A simple list-based representation of timeseries data."""
    def __init__(self, timestamps: list[Union[datetime, time]], values: list[Any]):
        if len(timestamps) != len(values):
            raise ValueError("timestamps and values must be of equal length")
        self.timestamps = timestamps
        self.values = values

    def at(self, index: int):
        if index >= len(self.timestamps):
            raise IndexError("Index out of range")
        return self.timestamps[index], self.values[index]


