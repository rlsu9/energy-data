#!/usr/bin/env python3

from flask_restful import Resource
from webargs import fields
from webargs.flaskparser import use_kwargs
from flask import current_app

from api.helpers.balancing_authority import convert_watttime_ba_abbrev_to_region, lookup_watttime_balancing_authority, \
    get_all_balancing_authorities

balancing_authority_args = {
    'latitude': fields.Float(required=True, validate=lambda x: abs(x) <= 90.),
    'longitude': fields.Float(required=True, validate=lambda x: abs(x) <= 180.),
}


class BalancingAuthority(Resource):
    @use_kwargs(balancing_authority_args, location='query')
    def get(self, latitude: float, longitude: float):
        current_app.logger.info("BalancingAuthority.get(%f, %f)" % (latitude, longitude))
        orig_request = {'request': {
            'latitude': latitude,
            'longitude': longitude,
        }}

        watttime_lookup_result = lookup_watttime_balancing_authority(latitude, longitude)
        region = convert_watttime_ba_abbrev_to_region(watttime_lookup_result['watttime_abbrev'])
        return orig_request | watttime_lookup_result | {
            'region': region,
        }


class BalancingAuthorityList(Resource):
    def get(self):
        current_app.logger.info("BalancingAuthorityList.get()")
        return get_all_balancing_authorities()
