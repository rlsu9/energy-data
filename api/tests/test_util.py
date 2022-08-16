#!/usr/bin/env python3

from datetime import date, datetime, timedelta, time
from dateutil import tz
import arrow

from api.util import round_down, xor, timedelta_to_time, Size, SizeUnit, RateUnit, Rate


def test_round_down_timestamp_no_timezone():
    original = datetime(2022, 1, 1, 12, 30, 44)
    round_to = timedelta(hours=1)
    expected = datetime(2022, 1, 1, 12, 0, 0)
    assert round_down(original, round_to) == expected


def test_round_down_timestamp_local_time():
    original = datetime(2022, 1, 1, 12, 30, 44, tzinfo=tz.gettz('America/Los_Angeles'))
    round_to = timedelta(hours=1)
    expected = datetime(2022, 1, 1, 12, 0, 0, tzinfo=tz.gettz('America/Los_Angeles'))
    assert round_down(original, round_to) == expected


def test_round_down_timestamp_utc_time():
    original = datetime(2022, 1, 1, 12, 30, 44, tzinfo=tz.UTC)
    round_to = timedelta(hours=1)
    expected = datetime(2022, 1, 1, 12, 0, 0, tzinfo=tz.UTC)
    assert round_down(original, round_to) == expected


def test_round_down_timestamp_now_to_today():
    original = arrow.get(datetime.now()).datetime
    round_to = timedelta(days=1)
    expected = arrow.get(date.today()).datetime
    assert abs(round_down(original, round_to) - expected) < timedelta(seconds=1)


def test_logical_xor_two_operands():
    assert xor(True, False) is True
    assert xor(False, True) is True
    assert xor(True, True) is False
    assert xor(False, False) is False


def test_logical_xor_multiple_operands():
    assert xor(True, False, True) is False
    assert xor(True, True, True) is True
    assert xor(True, False, False) is True
    assert xor(False, True, False) is True
    assert xor(True, False, True, False) is False
    assert xor(True, False, True, False, True, False) is True


def test_timedelta_to_time():
    assert timedelta_to_time(timedelta(minutes=1)) == time(minute=1)
    assert timedelta_to_time(timedelta(minutes=1*60)) == time(hour=1)
    assert timedelta_to_time(timedelta(minutes=2*60)) == time(hour=2)
    assert timedelta_to_time(timedelta(minutes=14*60)) == time(hour=14)
    assert timedelta_to_time(timedelta(minutes=24*60 - 1)) == time(hour=23, minute=59)


def test_unit_conversion():
    # Unit conversions
    assert Size(1, SizeUnit.GB) == Size(1024, SizeUnit.MB)
    assert Size(1, SizeUnit.GB) == Size(1024*1024*1024, SizeUnit.Bytes)
    assert Size(1, SizeUnit.TB) == Size(1024 * 1024 * 1024 * 1024, SizeUnit.Bytes)
    # assert Size(1, SizeUnit.GB) + Size(1024, SizeUnit.MB) == Size(2, SizeUnit.GB)
    assert Rate(100, RateUnit.Gbps) == Rate(100*1024, RateUnit.Mbps)
    # Multiplications
    actual = Rate(1, RateUnit.Gbps) * timedelta(seconds=8)
    expected = Size(1, SizeUnit.GB)
    assert actual == expected
    assert Rate(100, RateUnit.Gbps) * timedelta(seconds=1) == Size(12.5, SizeUnit.GB)
    # Divisions
    assert Size(1, SizeUnit.GB) / timedelta(seconds=1) == Rate(8, RateUnit.Gbps)
    assert Size(1, SizeUnit.GB) / Rate(1, RateUnit.Gbps) == timedelta(seconds=8)
