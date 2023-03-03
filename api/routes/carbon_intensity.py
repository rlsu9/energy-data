#!/usr/bin/env python3

from datetime import datetime
from flask_restful import Resource
from webargs import fields
from webargs.flaskparser import use_kwargs
from flask import current_app

from api.helpers.carbon_intensity_c3lab import get_carbon_intensity_list
from api.routes.balancing_authority import convert_watttime_ba_abbrev_to_region, lookup_watttime_balancing_authority

carbon_intensity_args = {
    'latitude': fields.Float(required=True, validate=lambda x: abs(x) <= 90.),
    'longitude': fields.Float(required=True, validate=lambda x: abs(x) <= 180.),
    'start': fields.DateTime(format="iso", required=True),
    'end': fields.DateTime(format="iso", required=True),
}


class CarbonIntensity(Resource):
    @use_kwargs(carbon_intensity_args, location='query')
    def get(self, latitude: float, longitude: float, start: datetime, end: datetime):
        orig_request = {'request': {
            'latitude': latitude,
            'longitude': longitude,
            'start': start,
            'end': end,
        }}
        current_app.logger.info("CarbonIntensity.get(%f, %f, %s, %s)" % (latitude, longitude, start, end))

        watttime_lookup_result = lookup_watttime_balancing_authority(latitude, longitude)
        iso = watttime_lookup_result['watttime_abbrev']
        region = convert_watttime_ba_abbrev_to_region(iso)
        l_carbon_intensity = get_carbon_intensity_list(iso, start, end)

        return orig_request | watttime_lookup_result | {
            'region': region,
            'iso': iso,
            'carbon_intensities': l_carbon_intensity,
        }
