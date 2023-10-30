#!/usr/bin/env python3

from flask_restful import Resource
import marshmallow_dataclass
from webargs.flaskparser import use_args
from flask import current_app

from api.helpers.balancing_authority import get_iso_from_gps, lookup_watttime_balancing_authority
from api.models.common import ISO_PREFIX_C3LAB, IsoFormat
from api.models.dataclass_extensions import *


@marshmallow_dataclass.dataclass
class BalancingAuthorityRequest:
    latitude: float = field_with_validation(lambda x: abs(x) <= 90.)
    longitude: float = field_with_validation(lambda x: abs(x) <= 180.)
    iso_format: IsoFormat = field_enum(IsoFormat, IsoFormat.WattTime)


class BalancingAuthority(Resource):
    @use_args(marshmallow_dataclass.class_schema(BalancingAuthorityRequest)(), location='query')
    def get(self, args: BalancingAuthorityRequest):
        current_app.logger.info("BalancingAuthority.get(%f, %f)" % (args.latitude, args.longitude))
        orig_request = {'request': {
            'latitude': args.latitude,
            'longitude': args.longitude,
        }}

        iso = get_iso_from_gps(args.latitude, args.longitude, args.iso_format)
        if args.iso_format == IsoFormat.EMap:
            return orig_request | {
                'iso': iso,
            }
        else:   # For legacy compatibility
            watttime_lookup_result = lookup_watttime_balancing_authority(args.latitude, args.longitude)
            region = get_iso_from_gps(args.latitude, args.longitude, IsoFormat.C3Lab).removeprefix(ISO_PREFIX_C3LAB)
            return orig_request | watttime_lookup_result | {
                'iso': iso,
                'region': region,
            }


# class BalancingAuthorityList(Resource):
#     def get(self):
#         current_app.logger.info("BalancingAuthorityList.get()")
#         return get_all_balancing_authorities()
