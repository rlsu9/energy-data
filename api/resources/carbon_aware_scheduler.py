#!/usr/bin/env python3

from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
from flask_restful import Resource
from webargs import fields
from webargs.flaskparser import use_args, use_kwargs
import marshmallow_dataclass
from marshmallow import validate

from api.util import logger, PSqlExecuteException
from api.resources.balancing_authority import convert_watttime_ba_abbrev_to_region, lookup_watttime_balancing_authority
from api.resources.cloud_location import CloudLocationManager

g_cloud_locations = CloudLocationManager.get_all_cloud_locations()

validate_timedelta_is_positive = lambda dt: dt.total_seconds() > 0
metadata_timedelta = dict(
    validate = validate_timedelta_is_positive,
    precision = 'seconds',
    serialization_type = float
)
validate_number_is_nonnegative = validate.Range(min=0, min_inclusive=True)

class ScheduleType(Enum):
    UNIFORM_RANDOM = "uniform-random"
    POISSON = "poisson"

@dataclass
class WorkloadSchedule:
    type: ScheduleType = field(metadata=dict(by_value=True))
    interval: timedelta = field(metadata=metadata_timedelta)

@dataclass
class Dataset:
    input_size_gb: float = field(metadata=dict(validate=validate_number_is_nonnegative))
    output_size_gb: float = field(metadata=dict(validate=validate_number_is_nonnegative))

@dataclass
class Workload:
    runtime: timedelta = field(metadata=metadata_timedelta)
    schedule: WorkloadSchedule
    dataset: Dataset


carbon_aware_scheduler_args = {
    'latitude': fields.Float(required=True, validate=lambda x: abs(x) <= 90.),
    'longitude': fields.Float(required=True, validate=lambda x: abs(x) <= 180.),
    'start': fields.DateTime(format="iso", required=True),
    'end': fields.DateTime(format="iso", required=True),
}

class CarbonAwareScheduler(Resource):
    @use_kwargs(carbon_aware_scheduler_args, location='query')
    @use_args(marshmallow_dataclass.class_schema(Workload)())
    def get(self, args: Workload, latitude: float, longitude: float, start: datetime, end: datetime):
        workload = args
        orig_request = { 'request': {
            'latitude': latitude,
            'longitude': longitude,
            'start': start,
            'end': end,
            'workload': workload
        } }
        logger.info("CarbonAwareScheduler.get(%f, %f, %s, %s, %s)" % (latitude, longitude, start, end, workload))

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
