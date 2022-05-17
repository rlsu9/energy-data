#!/usr/bin/env python3

import os
import numpy as np
import psycopg2
from pathlib import Path
from datetime import datetime
from flask_restful import Resource
from webargs import fields
from webargs.flaskparser import use_kwargs

from api.util import logger, loadYamlData, get_psql_connection, psql_execute_list, psql_execute_scalar
from api.resources.balancing_authority import convert_watttime_ba_abbrev_to_region, lookup_watttime_balancing_authority

class ElectricityDataLookupException(Exception):
    pass

def get_map_carbon_intensity_by_fuel_source(config_path: os.path) -> dict[str, float]:
    '''Load the carbon intensity per fuel source map from config.'''
    # Load ISO-to-WattTime-BA mapping from yaml config
    yaml_data = loadYamlData(config_path)
    carbon_intensity_map_name = 'carbon_intensity_by_fuel_source'
    assert yaml_data is not None and carbon_intensity_map_name in yaml_data, \
        f'Failed to load {carbon_intensity_map_name} from config.'
    return yaml_data[carbon_intensity_map_name]

MAP_CARBON_INTENSITY_BY_FUEL_SOURCE = get_map_carbon_intensity_by_fuel_source(os.path.join(Path(__file__).parent.absolute(), 'carbon_intensity.yaml'))
DEFAULT_CARBON_INTENSITY_FOR_UNKNOWN_SOURCE = 700

def validate_region_exists(conn: psycopg2.extensions.connection, region: str) -> None:
    cursor = conn.cursor()
    region_exists = psql_execute_scalar(cursor,
        "SELECT EXISTS(SELECT 1 FROM EnergyMixture WHERE region = %s)",
        [region])
    if not region_exists:
        raise ElectricityDataLookupException(f"Region {region} doesn't exist in database.")

def get_matching_timestamp(conn: psycopg2.extensions.connection, region: str, timestamp: datetime) -> datetime:
    """Get the matching stamp in electricity generation records for the given time."""
    cursor = conn.cursor()
    timestamp_before: datetime|None = psql_execute_scalar(cursor,
        "SELECT MAX(DateTime) FROM EnergyMixture WHERE Region = %s AND DateTime <= %s;"
        , [region, timestamp])
    timestamp_after: datetime|None = psql_execute_scalar(cursor,
        "SELECT MIN(DateTime) FROM EnergyMixture WHERE Region = %s AND DateTime >= %s;"
        , [region, timestamp])
    if timestamp_before is None:
        raise ElectricityDataLookupException("Timestamp is too old. No data available.")
    if timestamp_after is None:
        raise ElectricityDataLookupException("Timestamp is too new. Data not yet available.")
    assert timestamp_before <= timestamp_after, "before must be less than after"
    # Always choose the beginning of the period
    return timestamp_before

def get_power_by_fuel_source(conn: psycopg2.extensions.connection, region: str, timestamp: datetime) -> dict[str, float]:
    cursor = conn.cursor()
    records: list[tuple[str, float]] = psql_execute_list(cursor,
        "SELECT category, power_mw FROM EnergyMixture WHERE region = %s AND datetime = %s;",
        [region, timestamp])
    d_power_by_fuel_source: dict[str, float] = {}
    for (category, power_mw) in records:
        d_power_by_fuel_source[category] = power_mw
    return d_power_by_fuel_source

def calculate_average_carbon_intensity(power_by_fuel_source: dict[str, float]) -> float:
    l_carbon_intensity = []
    l_weight = []   # aka power in MW
    for fuel_source, power_in_mw in power_by_fuel_source.items():
        if fuel_source not in MAP_CARBON_INTENSITY_BY_FUEL_SOURCE:
            carbon_intensity = DEFAULT_CARBON_INTENSITY_FOR_UNKNOWN_SOURCE
        else:
            carbon_intensity = MAP_CARBON_INTENSITY_BY_FUEL_SOURCE[fuel_source]
        l_carbon_intensity.append(carbon_intensity)
        l_weight.append(power_in_mw)
    return np.average(l_carbon_intensity, weights=l_weight)

def get_carbon_intensity(region: str, timestamp: datetime) -> float:
    conn = get_psql_connection()
    validate_region_exists(conn, region)
    matching_timestamp = get_matching_timestamp(conn, region, timestamp)
    power_by_fuel_source = get_power_by_fuel_source(conn, region, matching_timestamp)
    return calculate_average_carbon_intensity(power_by_fuel_source)

carbon_intensity_args = {
    'latitude': fields.Float(required=True, validate=lambda x: abs(x) <= 90.),
    'longitude': fields.Float(required=True, validate=lambda x: abs(x) <= 180.),
    'timestamp': fields.DateTime(format="iso", required=True),
}

class CarbonIntensity(Resource):
    @use_kwargs(carbon_intensity_args, location='query')
    def get(self, latitude: float, longitude: float, timestamp: datetime):
        logger.info("get(%f, %f, %s)" % (latitude, longitude, timestamp.isoformat()))
        orig_request = { 'request': {
            'latitude': latitude,
            'longitude': longitude,
            'timestamp': timestamp.isoformat(),
        } }

        watttime_lookup_result, error_status_code = lookup_watttime_balancing_authority(latitude, longitude)
        if error_status_code:
            return orig_request | watttime_lookup_result, error_status_code

        region = convert_watttime_ba_abbrev_to_region(watttime_lookup_result['watttime_abbrev'])
        try:
            carbon_intensity = get_carbon_intensity(region, timestamp)
        except ElectricityDataLookupException as e:
            return orig_request | {
                'error': str(e)
            }

        return orig_request | watttime_lookup_result | {
            'region': region,
            'carbon_intensity': carbon_intensity,
        }
