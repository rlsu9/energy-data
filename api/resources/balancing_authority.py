#!/usr/bin/env python3

from flask_restful import Resource
from webargs import fields
from webargs.flaskparser import use_kwargs

balancing_authority_args = {
    'latitude': fields.Float(required=True, validate=lambda x: abs(x) <= 90.),
    'longitude': fields.Float(required=True, validate=lambda x: abs(x) <= 180.),
}

class BalancingAuthority(Resource):
    @use_kwargs(balancing_authority_args, location='query')
    def get(self, latitude, longitude):
        # TODO: query this from WattTime API
        balancing_authority_name = 'TBD'
        return {
            'latitude': latitude,
            'longitude': longitude,
            'balancing_authority': balancing_authority_name
        }
