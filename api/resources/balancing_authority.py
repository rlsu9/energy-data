#!/usr/bin/env python3

import os, traceback
from pathlib import Path
from flask_restful import Resource
from webargs import fields
from webargs.flaskparser import use_kwargs
from ..util import getLogger
import yaml

from ..external.watttime.ba_from_loc import get_ba_from_loc

logger = getLogger()

YAML_CONFIG = 'balancing_authority.yaml'

balancing_authority_args = {
    'latitude': fields.Float(required=True, validate=lambda x: abs(x) <= 90.),
    'longitude': fields.Float(required=True, validate=lambda x: abs(x) <= 180.),
}

def get_mapping_wattime_ba_to_iso(reverse_mapping: dict):
    '''Invert the reverse mapping table to provide direct lookup table'''
    lookup_table = {}
    for iso, l_watttime_ba in reverse_mapping.items():
        for watttime_ba in l_watttime_ba:
            assert watttime_ba not in lookup_table, "Duplicate ba in ISO-to-WattTime-BA mapping table: %s" % watttime_ba
            lookup_table[watttime_ba] = iso
    return lookup_table

def load_map_iso_to_watttime_ba():
    with open(os.path.join(Path(__file__).parent.absolute(), YAML_CONFIG), 'r') as f:
        try:
            yaml_data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            logger.fatal('Failed to load ISO-to-WattTime-ba mapping')
            logger.fatal(e)
            logger.fatal(traceback.format_exc())
    assert 'map_iso_to_watttime_ba' in yaml_data, 'map_iso_to_watttime_ba not found'
    return yaml_data['map_iso_to_watttime_ba']

MAPPING_WATTTIME_BA_TO_ISO = get_mapping_wattime_ba_to_iso(load_map_iso_to_watttime_ba())

def convert_watttime_ba_abbrev(watttime_abbrev):
    if watttime_abbrev in MAPPING_WATTTIME_BA_TO_ISO:
        return MAPPING_WATTTIME_BA_TO_ISO[watttime_abbrev]
    else:
        logger.warning('Unknown watttime abbrev "%s"' % watttime_abbrev)
        return 'unknown:' + watttime_abbrev

class BalancingAuthority(Resource):
    @use_kwargs(balancing_authority_args, location='query')
    def get(self, latitude, longitude):
        logger.info("get(%f, %f)" % (latitude, longitude))
        watttime_response = get_ba_from_loc(latitude, longitude)
        watttime_json = watttime_response.json()
        response = {
            'latitude': latitude,
            'longitude': longitude,
        }

        if not watttime_response.ok:
            error = watttime_json['error'] if 'error' in watttime_json else 'Unknown error from WattTime API'
            logger.warning('WattTime error: %s' % error)
            return response | { 'error': error }, watttime_response.status_code

        try:
            watttime_abbrev = watttime_json['abbrev']
            watttime_name = watttime_json['name']
            watttime_id = watttime_json['id']
        except Exception as e:
            logger.error('Response: %s' % watttime_json)
            logger.error(f"Failed to parse watttime response: {e}")
            return {
                'error': 'Failed to parse WattTime API response'
            }, 500

        iso = convert_watttime_ba_abbrev(watttime_abbrev)
        return response | {
            'iso': iso,
            'watttime_abbrev': watttime_abbrev,
            'watttime_name': watttime_name,
            'watttime_id': watttime_id,
        }
