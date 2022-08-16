#!/usr/bin/env python3

from enum import Enum, IntEnum
from typing import Any, Sequence, Union
from datetime import datetime, date, timedelta, time
import yaml
import traceback
import psycopg2
import dataclasses
from flask import current_app
from flask.json import JSONEncoder
from werkzeug.exceptions import HTTPException


def load_yaml_data(filepath):
    with open(filepath, 'r') as f:
        try:
            return yaml.safe_load(f)
        except yaml.YAMLError as e:
            current_app.logger.fatal('Failed to load YAML data from "%s"' % filepath)
            current_app.logger.fatal(e)
            current_app.logger.fatal(traceback.format_exc())
            return None


class CustomJSONEncoder(JSONEncoder):
    def default(self, o: object) -> Any:
        """This defines serialization for object types that `json` cannot handle by default."""
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        if isinstance(o, timedelta):
            return o.total_seconds()
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        if isinstance(o, Enum):
            return o.value
        raise TypeError("Type %s is not serializable" % type(o))


class DocstringDefaultException(HTTPException):
    """Subclass exception uses docstring as default message."""

    def __init__(self, message=None, *args: object, **kwargs):
        super().__init__(message or self.__doc__, *args, **kwargs)


class PSqlExecuteException(DocstringDefaultException):
    """Unknown database exception"""

    def __init__(self, message=None, *args: object, **kwargs):
        super().__init__(message, *args, **kwargs)
        self.code = 500


class CustomHTTPException(HTTPException):
    def __init__(self, description: str = None, code: int = None) -> None:
        super().__init__(description)
        if code:
            self.code = code


def get_psql_connection(host='/var/run/postgresql/', database="electricity-data") -> psycopg2.extensions.connection:
    """Get a new postgresql connection."""
    try:
        conn = psycopg2.connect(host=host, database=database, user="restapi_ro")
        conn.autocommit = True
        return conn
    except Exception as e:
        current_app.logger.fatal(f"get_psql_connection: {e}")
        current_app.logger.fatal(traceback.format_exc())
        raise PSqlExecuteException("Failed to connect to database.")


def psql_execute_scalar(cursor: psycopg2.extensions.cursor, query: str, args: Sequence[Any] = None) -> Any | None:
    """Execute the psql query and return the first column of first row."""
    try:
        cursor.execute(query, args)
        result = cursor.fetchone()
    except psycopg2.Error as e:
        current_app.logger.error(f'psql_execute_scalar("{query}", {args}): {e}')
        current_app.logger.error(traceback.format_exc())
        raise PSqlExecuteException("Failed to execute SQL query.")
    return result[0] if result is not None else None


def psql_execute_list(cursor: psycopg2.extensions.cursor, query: str,
                      args: Union[Sequence[Any], dict[str, str]] = None) -> list[tuple]:
    """Execute the psql query and return all rows as a list of tuples."""
    try:
        cursor.execute(query, args)
        result = cursor.fetchall()
    except psycopg2.Error as e:
        current_app.logger.error(f'psql_execute_scalar("{query}", {args}): {e}')
        current_app.logger.error(traceback.format_exc())
        raise PSqlExecuteException("Failed to execute SQL query.")
    return result


def get_all_enum_values(enum_type):
    """Get all values of a particular Enum type."""
    return [e.value for e in enum_type]


def round_down(dt: datetime, round_to: timedelta) -> datetime:
    """Round down the given datetime to the specified interval."""
    # datetime.min has tzinfo=None
    total_seconds = (dt.replace(tzinfo=None) - datetime.min).total_seconds()
    remainder_seconds = total_seconds % round_to.total_seconds()
    dt = dt.replace(microsecond=0)
    return dt - timedelta(seconds=remainder_seconds)


def xor(*args):
    """Logical XOR of boolean values."""
    return sum(map(bool, args)) % 2 == 1


def timedelta_to_time(dt: timedelta) -> time:
    if dt >= timedelta(days=1):
        raise ValueError("time cannot be greater than or equal to a day.")
    return (datetime.min + dt).time()


class UnitPrefix(IntEnum):
    Base = 0
    Kilo = 10
    Mega = 20
    Giga = 30
    Tera = 40

    # def __sub__(self, other):
    #     if isinstance(other, UnitPrefix):
    #         return self.value - other.value
    #     raise ValueError("Incompatible type for subtraction")


class SizeUnit(IntEnum):
    Bytes = UnitPrefix.Base
    KB = UnitPrefix.Kilo
    MB = UnitPrefix.Mega
    GB = UnitPrefix.Giga
    TB = UnitPrefix.Tera


class RateUnit(IntEnum):
    bps = UnitPrefix.Base
    Kbps = UnitPrefix.Kilo
    Mbps = UnitPrefix.Mega
    Gbps = UnitPrefix.Giga
    Tbps = UnitPrefix.Tera


POWER_BASE = 2
BITS_PER_BYTE = 8


class Size:
    def __init__(self, value=1., unit=SizeUnit.Bytes):
        self.value = value
        self.unit = unit

    def bytes(self):
        return self.value * pow(POWER_BASE, self.unit.value)

    def gigabytes(self):
        return self.value * pow(POWER_BASE, self.unit.value - SizeUnit.GB.value)

    def __eq__(self, other):
        if not isinstance(other, Size):
            return False
        return self.gigabytes() == other.gigabytes()

    def __truediv__(self, other):
        if isinstance(other, timedelta):
            value = BITS_PER_BYTE * self.value / other.total_seconds()
            unit = RateUnit(self.unit.value)
            return Rate(value, unit)
        elif isinstance(other, Rate):
            value = BITS_PER_BYTE * self.value / other.value
            unit = self.unit.value - other.unit.value
            return timedelta(seconds=value*pow(POWER_BASE, unit))


class Rate:
    def __init__(self, value=1., unit=RateUnit.bps):
        self.value = value
        self.unit = unit

    def bps(self):
        return self.value * pow(POWER_BASE, self.unit.value)

    def gbps(self):
        return self.value * pow(POWER_BASE, self.unit.value - RateUnit.Gbps.value)

    def __eq__(self, other):
        if not isinstance(other, Rate):
            return False
        return self.gbps() == other.gbps()

    def __mul__(self, other) -> Size:
        if not isinstance(other, timedelta):
            raise ValueError("Can only bandwidth multiply with timedelta")
        value = self.value * other.total_seconds() / BITS_PER_BYTE
        unit = SizeUnit(self.unit.value)
        return Size(value, unit)
