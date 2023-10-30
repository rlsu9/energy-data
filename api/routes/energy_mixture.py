#!/usr/bin/env python3

from datetime import datetime
from flask_restful import Resource
import marshmallow_dataclass
from webargs import fields
from webargs.flaskparser import use_args
from flask import current_app
from api.helpers.balancing_authority import get_iso_from_gps

from api.helpers.carbon_intensity_c3lab import get_power_by_fuel_type
from api.models.common import ISO_PREFIX_C3LAB, IsoFormat
from api.models.dataclass_extensions import *


@marshmallow_dataclass.dataclass
class EnergyMixtureRequest:
    latitude: float = field_with_validation(lambda x: abs(x) <= 90.)
    longitude: float = field_with_validation(lambda x: abs(x) <= 180.)
    start: datetime = field_default()
    end: datetime = field_default()
    iso_format: IsoFormat = field_enum(IsoFormat, IsoFormat.WattTime)

class EnergyMixture(Resource):
    @use_args(marshmallow_dataclass.class_schema(EnergyMixtureRequest)(), location='query')
    def get(self, args: EnergyMixtureRequest):
        orig_request = {'request': {
            'latitude': args.latitude,
            'longitude': args.longitude,
            'start': args.start,
            'end': args.end,
        }}
        current_app.logger.info("EnergyMixture.get(%f, %f, %s, %s)" % 
                                (args.latitude, args.longitude, args.start, args.end))

        # assert args.iso_format == IsoFormat.C3Lab, "Only C3Lab is supported for energy mixture endpoint"
        iso = get_iso_from_gps(args.latitude, args.longitude, args.iso_format)
        region = get_iso_from_gps(args.latitude, args.longitude, IsoFormat.C3Lab).removeprefix(ISO_PREFIX_C3LAB)
        power_by_fuel_type = get_power_by_fuel_type(iso, args.start, args.end)

        return orig_request | {
            'iso': iso,
            'region': region,
            'power_by_fuel_type': power_by_fuel_type,
        }
