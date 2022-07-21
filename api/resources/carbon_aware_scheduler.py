#!/usr/bin/env python3

from datetime import datetime
from flask_restful import Resource
from webargs import fields
from webargs.flaskparser import use_args, use_kwargs
import marshmallow_dataclass

from api.util import logger, PSqlExecuteException
from api.resources.balancing_authority import convert_watttime_ba_abbrev_to_region, lookup_watttime_balancing_authority
from api.resources.cloud_location import CloudLocationLookupException, CloudLocationManager
from api.resources.models.workload import Workload

g_cloud_manager = CloudLocationManager()


carbon_aware_scheduler_args = {
    'start': fields.DateTime(format="iso", required=True),
    'end': fields.DateTime(format="iso", required=True),
}

class CarbonAwareScheduler(Resource):
    @use_kwargs(carbon_aware_scheduler_args, location='query')
    @use_args(marshmallow_dataclass.class_schema(Workload)())
    def get(self, args: Workload, start: datetime, end: datetime):
        workload = args
        orig_request = { 'request': {
            'start': start,
            'end': end,
            'workload': workload
        } }
        logger.info("CarbonAwareScheduler.get(%s, %s, %s)" % (start, end, workload))

        try:
            (latitude, longitude) = g_cloud_manager.get_gps_coordinate(args.preferred_cloud_location.cloud_provider,
                                                                    args.preferred_cloud_location.region_code)
        except CloudLocationLookupException as e:
            return orig_request | {
                'error': 'Requested cloud location not found: ' + str(e)
            }, 404

        watttime_lookup_result, error_status_code = lookup_watttime_balancing_authority(latitude, longitude)
        if error_status_code:
            return orig_request | watttime_lookup_result, error_status_code

        region = convert_watttime_ba_abbrev_to_region(watttime_lookup_result['watttime_abbrev'])
        try:
            return orig_request | watttime_lookup_result | {
                'error': 'Not implemented'
            }, 500
            # l_carbon_intensity = get_carbon_intensity_list(region, start, end)
        except PSqlExecuteException as e:
            return orig_request | watttime_lookup_result | {
                'region': region,
                'error': str(e)
            }, 500
