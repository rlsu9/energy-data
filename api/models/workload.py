#!/usr/bin/env python3

from dataclasses import field
from enum import Enum
from datetime import datetime, timedelta, timezone
from typing import Optional
from marshmallow_dataclass import dataclass
from marshmallow import validate, validates_schema, ValidationError

from api.models.cloud_location import CloudLocationManager
from api.util import get_all_enum_values

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

def field_enum(enum_type):
    return field(metadata=dict(by_value=True, error=custom_validation_error_enum(enum_type)))

def custom_validation_error_enum(enum_type):
    possible_values = get_all_enum_values(enum_type)
    return f'Must be one of %s.' % ', '.join(possible_values)

class ScheduleType(Enum):
    UNIFORM_RANDOM = "uniform-random"
    POISSON = "poisson"
    ONETIME = "onetime"

@dataclass
class WorkloadSchedule:
    type: ScheduleType = field_enum(ScheduleType)
    start_time: datetime = field_with_validation(validate.Range(min=datetime.now(timezone.utc)))
    interval: Optional[timedelta] = field(metadata=metadata_timedelta, default=None)

    @validates_schema
    def validate_schema(self, data, **kwargs):
        errors = dict()
        if 'type' not in data:
            return
        if data['type'] is ScheduleType.ONETIME and 'interval' in data and data['interval']:
            errors['interval'] = 'interval must be empty for one-time workload'
        if data['type'] is not ScheduleType.ONETIME and ('interval' not in data or not data['interval']):
            errors['interval'] = 'interval must be specified for recurring workload'
        if errors:
            raise ValidationError(errors)

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
