#!/usr/bin/env python3

from datetime import datetime
import psycopg2
from psycopg2 import sql
from werkzeug.exceptions import NotFound, BadRequest

from api.util import psql_execute_scalar


def validate_region_exists(conn: psycopg2.extensions.connection, region: str,
                           table_name: str = "energymixture", region_column: str = "region") -> None:
    cursor = conn.cursor()
    region_exists = psql_execute_scalar(cursor,
                                        sql.SQL("SELECT EXISTS(SELECT 1 FROM {table} WHERE {column} = %s)").format(
                                            table = sql.Identifier(table_name),
                                            column = sql.Identifier(region_column)
                                        ),
                                        [region])
    if not region_exists:
        raise NotFound(f"Region {region} doesn't exist.")

def _get_available_time_range(conn: psycopg2.extensions.connection, region: str,
                             table_name: str, region_column: str) -> \
        tuple[datetime, datetime]:
    """Get the timestamp range for which we have electricity data in given region."""
    cursor = conn.cursor()
    timestamp_min: datetime | None = \
        psql_execute_scalar(cursor,
                            sql.SQL("SELECT MIN(DateTime) FROM {table} WHERE {column} = %s;").format(
                                table = sql.Identifier(table_name),
                                column = sql.Identifier(region_column)
                            ),
                            [region])
    timestamp_max: datetime | None = \
        psql_execute_scalar(cursor,
                            sql.SQL("SELECT MAX(DateTime) FROM {table} WHERE {column} = %s;").format(
                                table = sql.Identifier(table_name),
                                column = sql.Identifier(region_column)
                            ),
                            [region])
    return timestamp_min, timestamp_max


def validate_time_range(conn: psycopg2.extensions.connection,
                        region: str, start: datetime, end: datetime,
                        table_name: str = "EnergyMixture", region_column: str = "Region") -> None:
    """Validate we have electricity data for the given time range."""
    if start > end:
        raise BadRequest("end must be before start")
    (available_start, available_end) = _get_available_time_range(conn, region, table_name, region_column)
    if start > available_end:
        raise BadRequest("Time range is too new. Data not yet available.")
    if end < available_start:
        raise BadRequest("Time range is too old. No data available.")
