#!/usr/bin/env python3

from datetime import datetime
from flask_restful import Resource
from webargs.flaskparser import use_args
from flask import current_app
import marshmallow_dataclass

from api.helpers.carbon_intensity_c3lab import get_carbon_intensity_list
from api.models.dataclass_extensions import *
from api.routes.balancing_authority import convert_watttime_ba_abbrev_to_c3lab_region, \
    lookup_watttime_balancing_authority


@marshmallow_dataclass.dataclass
class CarbonIntensityRequest:
    latitude: float = field_with_validation(lambda x: abs(x) <= 90.)
    longitude: float = field_with_validation(lambda x: abs(x) <= 180.)
    start: datetime = field_default()
    end: datetime = field_default()


class CarbonIntensity(Resource):
    @use_args(marshmallow_dataclass.class_schema(CarbonIntensityRequest)(), location='query')
    def get(self, request: CarbonIntensityRequest):
        orig_request = { 'request': request }
        current_app.logger.info("CarbonIntensity.get(%s)" % request)

        watttime_lookup_result = lookup_watttime_balancing_authority(request.latitude, request.longitude)
        iso = watttime_lookup_result['watttime_abbrev']
        region = convert_watttime_ba_abbrev_to_c3lab_region(iso)
        l_carbon_intensity = get_carbon_intensity_list(iso, request.start, request.end)

        return orig_request | watttime_lookup_result | {
            'region': region,
            'iso': iso,
            'carbon_intensities': l_carbon_intensity,
        }
