#!/usr/bin/env python3

from flask_restful import Resource
from webargs import fields
from webargs.flaskparser import use_kwargs
import logging

from ..external.watttime.ba_from_loc import get_ba_from_loc

balancing_authority_args = {
    'latitude': fields.Float(required=True, validate=lambda x: abs(x) <= 90.),
    'longitude': fields.Float(required=True, validate=lambda x: abs(x) <= 180.),
}

REVERSE_MAPPING_WATTTIME_BA = {
    'US-CA': [
        'CAISO_ESCONDIDO',
        'CAISO_LONGBEACH',
        'CAISO_NORTH',
        'CAISO_PALMSPRINGS',
        'CAISO_REDDING',
        'CAISO_SANBERNARDINO',
        'CAISO_SANDIEGO',
    ],
    'US-BPA': [
        'BPA'
    ],
    'US-ERCOT': [
        'ERCOT_AUSTIN',
        'ERCOT_COAST',
        'ERCOT_EASTTX',
        'ERCOT_HIDALGO',
        'ERCOT_NORTHCENTRAL',
        'ERCOT_PANHANDLE',
        'ERCOT_SANANTONIO',
        'ERCOT_SECOAST',
        'ERCOT_SOUTHTX',
        'ERCOT_WESTTX',
    ],
    'US-MISO': [
        'MISO_BEAUMONT',
        'MISO_DETROIT',
        'MISO_EAU_CLAIRE',
        'MISO_GRAND_RAPIDS',
        'MISO_INDIANAPOLIS',
        'MISO_LAFAYETTE',
        'MISO_LOWER_MS_RIVER',
        'MISO_MADISON',
        'MISO_MASON_CITY',
        'MISO_MINNEAPOLIS',
        'MISO_NEW_ORLEANS',
        'MISO_N_DAKOTA',
        'MISO_SAINT_LOUIS',
        'MISO_SPRINGFIELD',
        'MISO_UPPER_PENINSULA',
        'MISO_WORTHINGTON',
    ],
    'US-NEISO': [
        # None
    ],
    'US-NY': [
        'NYISO_CAPITAL',
        'NYISO_CENTRAL',
        'NYISO_HUDSON',
        'NYISO_LONG',
        'NYISO_MOHAWK',
        'NYISO_NORTH',
        'NYISO_NYC',
        'NYISO_WEST',
    ],
    'US-PJM': [
        'PJM_CHICAGO',
        'PJM_DC',
        'PJM_EASTERN_KY',
        'PJM_EASTERN_OH',
        'PJM_NJ',
        'PJM_ROANOKE',
        'PJM_SOUTHWEST_OH',
        'PJM_WESTERN_KY',
    ],
    'US-PR': [
        # None
    ],
    'US-SPP': [
        'SPP_FORTPECK',
        'SPP_KANSAS',
        'SPP_KC',
        'SPP_MEMPHIS',
        'SPP_ND',
        'SPP_OKCTY',
        'SPP_SIOUX',
        'SPP_SPRINGFIELD',
        'SPP_SWOK',
        'SPP_TX',
        'SPP_WESTNE',
    ],
}

def get_mapping_wattime_ba_to_iso(reverse_mapping: dict):
    '''Invert the reverse mapping table to provide direct lookup table'''
    lookup_table = {}
    for iso, l_watttime_ba in reverse_mapping.items():
        for watttime_ba in l_watttime_ba:
            assert watttime_ba not in lookup_table, "Duplicate ba in ISO-to-WattTime-BA mapping table: %s" % watttime_ba
            lookup_table[watttime_ba] = iso
    return lookup_table

MAPPING_WATTTIME_BA_TO_ISO = get_mapping_wattime_ba_to_iso(REVERSE_MAPPING_WATTTIME_BA)

def convert_watttime_ba_abbrev(watttime_abbrev):
    if watttime_abbrev in MAPPING_WATTTIME_BA_TO_ISO:
        return MAPPING_WATTTIME_BA_TO_ISO[watttime_abbrev]
    else:
        logging.warning('Unknown watttime abbrev "%s"' % watttime_abbrev)
        return 'unknown:' + watttime_abbrev

class BalancingAuthority(Resource):
    @use_kwargs(balancing_authority_args, location='query')
    def get(self, latitude, longitude):
        watttime_response = get_ba_from_loc(latitude, longitude)
        watttime_json = watttime_response.json()
        response = {
            'latitude': latitude,
            'longitude': longitude,
        }

        if not watttime_response.ok:
            error = watttime_json['error'] if 'error' in watttime_json else 'Unknown error from WattTime API'
            return response | { 'error': error }, watttime_response.status_code

        try:
            watttime_abbrev = watttime_json['abbrev']
            watttime_name = watttime_json['name']
            watttime_id = watttime_json['id']
        except Exception as e:
            logging.error('Response: %s' % watttime_json)
            logging.error(f"Failed to parse watttime response: {e}")
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
