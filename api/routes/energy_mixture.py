#!/usr/bin/env python3

from datetime import datetime
from flask_restful import Resource
from webargs import fields
from webargs.flaskparser import use_kwargs
from flask import current_app

from api.helpers.carbon_intensity import get_power_by_fuel_type
from api.routes.balancing_authority import convert_watttime_ba_abbrev_to_region, lookup_watttime_balancing_authority

energy_mixture_args = {
    'latitude': fields.Float(required=True, validate=lambda x: abs(x) <= 90.),
    'longitude': fields.Float(required=True, validate=lambda x: abs(x) <= 180.),
    'start': fields.DateTime(format="iso", required=True),
    'end': fields.DateTime(format="iso", required=True),
}


class EnergyMixture(Resource):
    @use_kwargs(energy_mixture_args, location='query')
    def get(self, latitude: float, longitude: float, start: datetime, end: datetime):
        orig_request = {'request': {
            'latitude': latitude,
            'longitude': longitude,
            'start': start,
            'end': end,
        }}
        current_app.logger.info("EnergyMixture.get(%f, %f, %s, %s)" % (latitude, longitude, start, end))

        watttime_lookup_result = lookup_watttime_balancing_authority(latitude, longitude)
        region = convert_watttime_ba_abbrev_to_region(watttime_lookup_result['watttime_abbrev'])
        power_by_fuel_type = get_power_by_fuel_type(region, start, end)

        return orig_request | watttime_lookup_result | {
            'region': region,
            'power_by_fuel_type': power_by_fuel_type,
        }
