#!/usr/bin/env python3

from enum import Enum
from typing import Any, Sequence
from datetime import datetime, date, timedelta
import yaml
import logging
import traceback
import psycopg2
import dataclasses

def getLogger():
    return logging.getLogger('gunicorn.error')

logger = getLogger()

def loadYamlData(filepath):
    with open(filepath, 'r') as f:
        try:
            return yaml.safe_load(f)
        except yaml.YAMLError as e:
            logger.fatal('Failed to load YAML data from "%s"' % filepath)
            logger.fatal(e)
            logger.fatal(traceback.format_exc())
            return None

def json_serialize(self, o: object) -> str:
    """This defines serilization for object types that `json` cannot handle by default."""
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, timedelta):
        return o.total_seconds()
    if dataclasses.is_dataclass(o):
        return dataclasses.asdict(o)
    if isinstance(o, Enum):
        return o.value
    raise TypeError("Type %s is not serializable" % type(o))

class PSqlExecuteException(Exception):
    pass

def get_psql_connection(host='/var/run/postgresql/', database="electricity-data") -> psycopg2.extensions.connection:
    """Get a new postgresql connection."""
    try:
        conn = psycopg2.connect(host=host, database=database, user="restapi_ro")
        return conn
    except Exception as e:
        logging.error(f"get_psql_connection: {e}")
        logger.fatal(traceback.format_exc())
        raise PSqlExecuteException("Failed to connect to database.")

def psql_execute_scalar(cursor: psycopg2.extensions.cursor, query: str, vars: Sequence[Any] = []) -> Any|None:
    """Execute the psql query and return the first column of first row."""
    try:
        cursor.execute(query, vars)
        result = cursor.fetchone()
    except Exception as e:
        logging.error(f'psql_execute_scalar("{query}", {vars}): {e}')
        logger.fatal(traceback.format_exc())
        raise PSqlExecuteException("Failed to execute SQL query.")
    return result[0] if result is not None else None

def psql_execute_list(cursor: psycopg2.extensions.cursor, query: str, vars: Sequence[Any] = []) -> list[tuple]:
    """Execute the psql query and return all rows as a list of tuples."""
    try:
        cursor.execute(query, vars)
        result = cursor.fetchall()
    except Exception as e:
        logging.error(f'psql_execute_scalar("{query}", {vars}): {e}')
        logger.fatal(traceback.format_exc())
        raise PSqlExecuteException("Failed to execute SQL query.")
    return result
