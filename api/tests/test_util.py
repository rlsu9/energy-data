#!/usr/bin/env python3

from datetime import date, datetime, timedelta, time
from dateutil import tz
import random
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
    assert timedelta_to_time(timedelta(minutes=1 * 60)) == time(hour=1)
    assert timedelta_to_time(timedelta(minutes=2 * 60)) == time(hour=2)
    assert timedelta_to_time(timedelta(minutes=14 * 60)) == time(hour=14)
    assert timedelta_to_time(timedelta(minutes=24 * 60 - 1)) == time(hour=23, minute=59)


def test_unit_conversion():
    # Unit conversions
    assert Size(1, SizeUnit.GB) == Size(1024, SizeUnit.MB)
    assert Size(1, SizeUnit.GB) == Size(1024 * 1024 * 1024, SizeUnit.Bytes)
    assert Size(1, SizeUnit.TB) == Size(1024 * 1024 * 1024 * 1024, SizeUnit.Bytes)
    # assert Size(1, SizeUnit.GB) + Size(1024, SizeUnit.MB) == Size(2, SizeUnit.GB)
    assert Rate(100, RateUnit.Gbps) == Rate(100 * 1024, RateUnit.Mbps)


def test_unit_mul_div_cross_unit():
    # Multiplication
    assert Rate(1, RateUnit.Gbps) * timedelta(seconds=8) == Size(1, SizeUnit.GB)
    assert Rate(100, RateUnit.Gbps) * timedelta(seconds=1) == Size(12.5, SizeUnit.GB)
    # Divisions
    assert Size(1, SizeUnit.GB) / timedelta(seconds=1) == Rate(8, RateUnit.Gbps)
    assert Size(1, SizeUnit.GB) / Rate(1, RateUnit.Gbps) == timedelta(seconds=8)


def test_unit_mul_div_serial():
    assert Size(2, SizeUnit.MB) / 2 == Size(1, SizeUnit.MB)
    assert Size(2, SizeUnit.MB) * 2 == Size(4, SizeUnit.MB)
    assert Rate(128, RateUnit.Mbps) * 8 == Rate(1, RateUnit.Gbps)
    assert Rate(1, RateUnit.Gbps) / 8 == Rate(128, RateUnit.Mbps)


def test_unit_comparison():
    s1 = Size(1, SizeUnit.GB)
    s2 = Size(2 * 1024, SizeUnit.MB)
    s3 = Size(3 * 1024 * 1024, SizeUnit.KB)
    s4 = Size(4 * 1024 * 1024 * 1024, SizeUnit.Bytes)
    s5 = Size(5 / 1024, SizeUnit.TB)
    l_size = [s1, s2, s3, s4, s5]
    l_shuffled = random.sample(l_size, len(l_size))
    l_ordered = sorted(l_shuffled)
    assert l_size == l_ordered


def test_unit_comparison_incompatible():
    s1 = Size(1, SizeUnit.MB)
    r1 = Rate(2, RateUnit.Mbps)
    try:
        sorted([s1, r1])
        assert False, "sorted() call should have failed"
    except ValueError as e:
        assert "Incompatible" in str(e), "s1 and r1 should be uncomparable"


def test_unit_add_subtract():
    assert Size(1.2, SizeUnit.GB) - Size(512, SizeUnit.MB) == Size(0.7, SizeUnit.GB)
    assert Size(1.2, SizeUnit.GB) + Size(256, SizeUnit.MB) == Size(1.45, SizeUnit.GB)
    assert Size(512, SizeUnit.GB) + Size(1, SizeUnit.TB) == Size(1.5, SizeUnit.TB)
    assert Size(1536, SizeUnit.GB) - Size(1, SizeUnit.TB) == Size(0.5, SizeUnit.TB)
    assert Rate(200, RateUnit.Gbps) - Rate(100, RateUnit.Gbps) == Rate(100, RateUnit.Gbps)
    assert Rate(1, RateUnit.Gbps) - Rate(128, RateUnit.Mbps) == Rate(896, RateUnit.Mbps)


def test_unit_add_subtract_copy():
    s1 = Size(1.2, SizeUnit.GB)
    s2 = Size(512, SizeUnit.MB)
    s3 = Size(0.7, SizeUnit.GB)
    assert s1 - s2 == s3
    assert s1.value == 1.2 and s1.unit == SizeUnit.GB
    assert s2.value == 512 and s2.unit == SizeUnit.MB
