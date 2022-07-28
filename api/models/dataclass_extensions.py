#!/usr/bin/env python3

from dataclasses import field
from marshmallow import validate

from api.util import get_all_enum_values

metadata_timedelta_nonzero = dict(
    validate = lambda dt: dt.total_seconds() > 0,
    precision = 'seconds',
    serialization_type = float
)

metadata_timedelta = dict(
    validate = lambda dt: dt.total_seconds() >= 0,
    precision = 'seconds',
    serialization_type = float
)

validate_number_is_nonnegative = validate.Range(min=0, min_inclusive=True)

def field_default():
    return field(metadata=dict())

def field_with_validation(validation_function):
    return field(metadata=dict(validate=validation_function))

def optional_field_with_validation(validation_function):
    return field(metadata=dict(validate=validation_function, default=None))

def field_enum(enum_type):
    return field(metadata=dict(by_value=True, error=custom_validation_error_enum(enum_type)))

def custom_validation_error_enum(enum_type):
    possible_values = get_all_enum_values(enum_type)
    return f'Must be one of %s.' % ', '.join(possible_values)
