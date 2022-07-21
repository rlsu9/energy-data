#!/usr/bin/env python3

import os
from pathlib import Path
from flask_restful import Resource
from webargs import fields
from webargs.flaskparser import use_kwargs
from flask import current_app

from api.util import PSqlExecuteException, get_psql_connection, loadYamlData, psql_execute_list
from api.external.watttime.ba_from_loc import get_ba_from_loc

YAML_CONFIG = 'balancing_authority.yaml'

def get_mapping_wattime_ba_to_region(config_path: os.path):
    '''Load the region-to-WattTime-BA mapping from config and inverse it to provide direct lookup table'''
    # Load region-to-WattTime-BA mapping from yaml config
    yaml_data = loadYamlData(config_path)
    region_to_watttime_ba_map_name = 'map_region_to_watttime_ba'
    assert yaml_data is not None and region_to_watttime_ba_map_name in yaml_data, \
        f'Failed to load {region_to_watttime_ba_map_name}'
    reverse_mapping = yaml_data[region_to_watttime_ba_map_name]
    # Inverse the one-to-many mapping to get direct lookup table (WattTime BA -> region)
    lookup_table = {}
    for region, l_watttime_ba in reverse_mapping.items():
        for watttime_ba in l_watttime_ba:
            assert watttime_ba not in lookup_table, "Duplicate ba in region-to-WattTime-BA mapping table: %s" % watttime_ba
            lookup_table[watttime_ba] = region
    return lookup_table

MAPPING_WATTTIME_BA_TO_REGION = get_mapping_wattime_ba_to_region(os.path.join(Path(__file__).parent.absolute(), YAML_CONFIG))

def convert_watttime_ba_abbrev_to_region(watttime_abbrev) -> str:
    if watttime_abbrev in MAPPING_WATTTIME_BA_TO_REGION:
        return MAPPING_WATTTIME_BA_TO_REGION[watttime_abbrev]
    else:
        current_app.logger.warning('Unknown watttime abbrev "%s"' % watttime_abbrev)
        return 'unknown:' + watttime_abbrev

def lookup_watttime_balancing_authority(latitude: float, longitude: float) -> tuple[dict, int]:
    """
        Lookup the balancing authority from WattTime API, and returns:
        1) parsed information, or error message, and optionally 2) error status code."""
    watttime_response = get_ba_from_loc(latitude, longitude)
    watttime_json = watttime_response.json()

    if not watttime_response.ok:
        error = watttime_json['error'] if 'error' in watttime_json else 'Unknown error from WattTime API'
        current_app.logger.warning('WattTime error: %s' % error)
        return { 'error': error }, watttime_response.status_code

    try:
        watttime_abbrev = watttime_json['abbrev']
        watttime_name = watttime_json['name']
        watttime_id = watttime_json['id']
    except Exception as e:
        current_app.logger.error('Response: %s' % watttime_json)
        current_app.logger.error(f"Failed to parse watttime response: {e}")
        return {
            'error': 'Failed to parse WattTime API response'
        }, 500

    return {
        'watttime_abbrev': watttime_abbrev,
        'watttime_name': watttime_name,
        'watttime_id': watttime_id,
    }, None

balancing_authority_args = {
    'latitude': fields.Float(required=True, validate=lambda x: abs(x) <= 90.),
    'longitude': fields.Float(required=True, validate=lambda x: abs(x) <= 180.),
}

class BalancingAuthority(Resource):
    @use_kwargs(balancing_authority_args, location='query')
    def get(self, latitude: float, longitude: float):
        current_app.logger.info("BalancingAuthority.get(%f, %f)" % (latitude, longitude))
        orig_request = { 'request': {
            'latitude': latitude,
            'longitude': longitude,
        } }

        watttime_lookup_result, error_status_code = lookup_watttime_balancing_authority(latitude, longitude)
        if error_status_code:
            return orig_request | watttime_lookup_result, error_status_code

        region = convert_watttime_ba_abbrev_to_region(watttime_lookup_result['watttime_abbrev'])
        return orig_request | watttime_lookup_result | {
            'region': region,
        }

def get_all_balancing_authorities():
    """Return a list of all balancing authorities for which we have collect data."""
    conn = get_psql_connection()
    cursor = conn.cursor()
    results: list[tuple[str]] = psql_execute_list(cursor, "SELECT DISTINCT region FROM EnergyMixture ORDER BY region;")
    return [row[0] for row in results]  # one column per row

class BalancingAuthorityList(Resource):
    def get(self):
        current_app.logger.info("BalancingAuthorityList.get()")
        try:
            return get_all_balancing_authorities()
        except PSqlExecuteException as e:
            return {
                'error': str(e)
            }, 500
