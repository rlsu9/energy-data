#!/usr/bin/env python3

from dataclasses import field
from enum import Enum
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple
from marshmallow_dataclass import dataclass
from marshmallow import validate, validates_schema, ValidationError
import numpy as np

from api.models.cloud_location import CloudLocationManager
from api.models.dataclass_extensions import *

g_cloud_manager = CloudLocationManager()

class ScheduleType(Enum):
    UNIFORM_RANDOM = "uniform-random"
    POISSON = "poisson"
    ONETIME = "onetime"

@dataclass
class WorkloadSchedule:
    type: ScheduleType = field_enum(ScheduleType)
    start_time: Optional[datetime] = optional_field_with_validation(validate.Range(min=datetime.now(timezone.utc)))
    interval: Optional[timedelta] = field(metadata=metadata_timedelta_nonzero, default=None)
    max_delay: Optional[timedelta] = field(metadata=metadata_timedelta, default=timedelta())

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

    def __str__(self) -> str:
        return f'{self.cloud_provider}:{self.region_code}'

    @validates_schema
    def validate_schema(self, data, **kwargs):
        # cloud_provider has been validated by its field-specific validation function
        all_region_codes = g_cloud_manager.get_cloud_region_codes(data['cloud_provider'])
        if data['region_code'] not in all_region_codes:
            raise ValidationError({
                'region_code': 'Must be one of: %s.' % ', '.join(all_region_codes)
            })

# Average of Xeon Platinum 8275CL, which has 48 HTs and a TDP of 240W
DEFAULT_CPU_POWER_PER_CORE = 240 / 48 # in watt

@dataclass
class Workload:
    preferred_cloud_location: CloudLocation
    runtime: timedelta = field(metadata=metadata_timedelta_nonzero)
    schedule: WorkloadSchedule
    dataset: Dataset

    def get_cputime_in_24h(self) -> timedelta:
        run_count = 0
        match self.schedule.type:
            case ScheduleType.ONETIME:
                run_count = 1
            case ScheduleType.ScheduleType.POISSON:
                run_count = timedelta(days=1) // self.schedule.interval
            case ScheduleType.ScheduleType.UNIFORM_RANDOM:
                run_count = timedelta(days=1) // self.schedule.interval
            case _:
                raise NotImplementedError()
        return self.runtime * run_count

    def get_running_intervals_in_24h(self) -> list[Tuple[datetime, datetime]]:
        intervals = []
        def _add_current_run_to_interval(start: datetime):
            intervals.append((start, start + self.runtime))

        match self.schedule.type:
            case ScheduleType.ONETIME:
                _add_current_run_to_interval(self.schedule.start_time)
            case ScheduleType.ScheduleType.POISSON:
                current_start = self.schedule.start_time
                lam = 1. / self.schedule.interval.total_seconds()
                # Draw poisson distribution every second
                distribution_size = timedelta(days=1).total_seconds()
                poisson_distributions = np.random.poisson(lam, distribution_size)
                for i in range(poisson_distributions):
                    for _ in range (poisson_distributions[i]):
                        time_elapsed = timedelta(seconds=i)
                        start = self.schedule.start_time + time_elapsed
                        _add_current_run_to_interval(start)
            case ScheduleType.ScheduleType.UNIFORM_RANDOM:
                current_start = self.schedule.start_time
                while current_start < self.schedule.start_time + timedelta(days=1):
                    _add_current_run_to_interval(current_start)
                    current_start += self.schedule.interval
            case _:
                raise NotImplementedError()
        return intervals

    def get_energy_usage_24h(self) -> float:
        """Get the energy usage in a 24h period, in kWh."""
        cpu_usage_hours = self.get_cputime_in_24h().total_seconds() / timedelta(hours=1).total_seconds()
        return DEFAULT_CPU_POWER_PER_CORE / 1000 * cpu_usage_hours
