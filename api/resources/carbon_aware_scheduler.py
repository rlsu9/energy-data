#!/usr/bin/env python3

from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
from flask_restful import Resource
from webargs import fields
from webargs.flaskparser import use_args, use_kwargs
import marshmallow_dataclass
from marshmallow import validate, validates_schema, ValidationError

from api.util import logger, PSqlExecuteException
from api.resources.balancing_authority import convert_watttime_ba_abbrev_to_region, lookup_watttime_balancing_authority
from api.resources.cloud_location import CloudLocationLookupException, CloudLocationManager

g_cloud_manager = CloudLocationManager()

validate_timedelta_is_positive = lambda dt: dt.total_seconds() > 0
metadata_timedelta = dict(
    validate = validate_timedelta_is_positive,
    precision = 'seconds',
    serialization_type = float
)
validate_number_is_nonnegative = validate.Range(min=0, min_inclusive=True)

def field_default():
    return field(metadata=dict())

def field_with_validation(validation_function):
    return field(metadata=dict(validate=validation_function))

class ScheduleType(Enum):
    UNIFORM_RANDOM = "uniform-random"
    POISSON = "poisson"

@dataclass
class WorkloadSchedule:
    type: ScheduleType = field(metadata=dict(by_value=True))
    interval: timedelta = field(metadata=metadata_timedelta)

@dataclass
class Dataset:
    input_size_gb: float = field_with_validation(validate_number_is_nonnegative)
    output_size_gb: float = field_with_validation(validate_number_is_nonnegative)

@dataclass
class CloudLocation:
    cloud_provider: str = field_with_validation(validate.OneOf(g_cloud_manager.get_all_cloud_providers()))
    region_code: str = field_default() #field_with_validation(validate.OneOf(g_cloud_manager.get_cloud_region_codes(cloud_provider)))

    @validates_schema
    def validate_schema(self, data, **kwargs):
        # cloud_provider has been validated by its field-specific validation function
        all_region_codes = g_cloud_manager.get_cloud_region_codes(data['cloud_provider'])
        if data['region_code'] not in all_region_codes:
            raise ValidationError({
                'region_code': 'Must be one of: %s.' % ', '.join(all_region_codes)
            })

@dataclass
class Workload:
    preferred_cloud_location: CloudLocation
    runtime: timedelta = field(metadata=metadata_timedelta)
    schedule: WorkloadSchedule
    dataset: Dataset


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
