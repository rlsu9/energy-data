#!/usr/bin/env python3

from typing import Any, Sequence
import yaml
import logging
import traceback
import psycopg2

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

def get_psql_connection(host='/var/run/postgresql/', database="electricity-data") -> psycopg2.extensions.connection:
    """Get a new postgresql connection."""
    try:
        conn = psycopg2.connect(host=host, database=database, user="postgres")
        return conn
    except Exception as e:
        logging.error("Failed to connect to database.")
        raise e

def psql_execute_scalar(cursor: psycopg2.extensions.cursor, query: str, vars: Sequence[Any]) -> Any|None:
    """Execute the psql query and return the first column of first row."""
    try:
        cursor.execute(query, vars)
        result = cursor.fetchone()
    except Exception as e:
        logging.error(f'Failed to execute query "{query}" with params: {vars}.')
        raise e
    return result[0] if result is not None else None

def psql_execute_list(cursor: psycopg2.extensions.cursor, query: str, vars: Sequence[Any]) -> list[tuple]:
    """Execute the psql query and return all rows as a list of tuples."""
    try:
        cursor.execute(query, vars)
        result = cursor.fetchall()
    except Exception as e:
        logging.error(f'Failed to execute query "{query}" with params: {vars}.')
        raise e
    return result
