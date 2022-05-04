#!/usr/bin/env python3

from flask_restful import Resource
from webargs import fields
from webargs.flaskparser import use_kwargs

from ..external.watttime.ba_from_loc import get_ba_from_loc

balancing_authority_args = {
    'latitude': fields.Float(required=True, validate=lambda x: abs(x) <= 90.),
    'longitude': fields.Float(required=True, validate=lambda x: abs(x) <= 180.),
}

class BalancingAuthority(Resource):
    @use_kwargs(balancing_authority_args, location='query')
    def get(self, latitude, longitude):
        (watttime_response, watttime_status_code) = get_ba_from_loc(latitude, longitude)
        return {
            'latitude': latitude,
            'longitude': longitude,
            'balancing_authority': watttime_response
        }, watttime_status_code
