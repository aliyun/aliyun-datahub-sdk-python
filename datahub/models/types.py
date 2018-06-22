#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import

import abc
import decimal

import six

from . import FieldType
from .. import utils
from ..exceptions import InvalidParameterException


@six.add_metaclass(abc.ABCMeta)
class DataType(object):
    """
    Abstract singleton data type
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(DataType, cls).__new__(cls, *args, **kwargs)
            return cls._instance
        else:
            return object.__new__(cls)

    def __repr__(self):
        return type(self).__name__.upper()

    def can_implicit_cast(self, other):
        return isinstance(self, type(other))

    def can_explicit_cast(self, other):
        return self.can_implicit_cast(other)

    def validate_value(self, val):
        # directly return True means without checking
        return True

    def _can_cast_or_throw(self, value, data_type):
        if not self.can_implicit_cast(data_type):
            raise InvalidParameterException('Cannot cast value(%s) from type(%s) to type(%s)' % (
                value, data_type, self))

    @abc.abstractmethod
    def cast_type(self):
        pass

    def cast_value(self, value, data_type):
        self._can_cast_or_throw(value, data_type)
        builtin_type = self.cast_type()
        try:
            if callable(builtin_type):
                ret = builtin_type(value)
            else:
                raise InvalidParameterException("builtin type not callable")
        except ValueError as e:
            raise InvalidParameterException(e)
        return ret


# Bigint
class Bigint(DataType):
    _bounds = (-9223372036854775808, 9223372036854775807)

    def can_implicit_cast(self, other):
        if isinstance(other, (Double, String, Timestamp)):
            return True
        return super(Bigint, self).can_implicit_cast(other)

    def validate_value(self, val):
        if val is None and self.nullable:
            return True
        smallest, largest = self._bounds
        if smallest <= val <= largest:
            return True
        raise InvalidParameterException('InvalidData: Bigint(%s) out of range' % val)

    def cast_type(self):
        if six.PY2:
            return long
        return int


# Double
class Double(DataType):
    def can_implicit_cast(self, other):
        if isinstance(other, (Bigint, String)):
            return True
        return super(Double, self).can_implicit_cast(other)

    def cast_type(self):
        return float


# Decimal
class Decimal(DataType):
    def can_implicit_cast(self, other):
        if isinstance(other, (Bigint, String)):
            return True
        return super(Decimal, self).can_implicit_cast(other)

    def cast_type(self):
        return decimal.Decimal


# String
class String(DataType):
    _max_length = 1 * 1024 * 1024  # 1M

    def can_implicit_cast(self, other):
        if isinstance(other, (Bigint, Double, Timestamp)):
            return True
        return super(String, self).can_implicit_cast(other)

    def validate_value(self, val):
        if val is None and self.nullable:
            return True
        if len(val) <= self._max_length:
            return True
        raise InvalidParameterException("InvalidData: Length of string(%s) is more than 1M.'" % val)

    def cast_type(self):
        return utils.to_text


# Timestamp
class Timestamp(DataType):
    _ticks_bound = (-62135798400000000, 253402271999000000)

    def can_implicit_cast(self, other):
        if isinstance(other, String):
            return True
        return super(Timestamp, self).can_implicit_cast(other)

    def validate_value(self, val):
        if val is None and self.nullable:
            return True

        smallest, largest = self._ticks_bound
        if smallest <= val <= largest:
            return True
        raise InvalidParameterException('InvalidData: Timestamp(%s) out of range' % val)

    def cast_type(self):
        if six.PY2:
            return long
        return int


# Boolean
class Boolean(DataType):

    def can_implicit_cast(self, other):
        if isinstance(other, String):
            return True
        return super(Boolean, self).can_implicit_cast(other)

    def cast_type(self):
        return bool


#####################################################################
# above is 5 type defined to verify field value
#####################################################################

float_builtins = (float,)
bool_builtins = (bool,)
integer_builtins = six.integer_types if isinstance(six.integer_types, tuple) else (six.integer_types,)
string_builtins = six.string_types if isinstance(six.string_types, tuple) else (six.string_types,)
decimal_builtins = (decimal.Decimal,)

try:
    import numpy as np

    integer_builtins += (np.integer,)
    float_builtins += (np.float,)
except ImportError:
    pass

bigint_type = Bigint()
double_type = Double()
string_type = String()
timestamp_type = Timestamp()
boolean_type = Boolean()
decimal_type = Decimal()


_datahub_types_dict = {
    FieldType.BIGINT: bigint_type,
    FieldType.DOUBLE: double_type,
    FieldType.STRING: string_type,
    FieldType.TIMESTAMP: timestamp_type,
    FieldType.BOOLEAN: boolean_type,
    FieldType.DECIMAL: decimal_type
}

_builtin_types_dict = {
    bigint_type: integer_builtins,
    double_type: float_builtins,
    string_type: string_builtins,
    timestamp_type: integer_builtins,
    boolean_type: bool_builtins,
    decimal_type: decimal_builtins
}


def infer_builtin_type(value):
    for datahub_type, builtin_types in six.iteritems(_builtin_types_dict):
        if isinstance(value, builtin_types):
            return datahub_type


def _validate_builtin_value(value, data_type):
    if value is None:
        return None
    if isinstance(value, (bytearray, six.binary_type)):
        value = value.decode('utf-8')

    builtin_types = _builtin_types_dict[data_type]
    if type(value) in builtin_types:
        return value

    inferred_data_type = infer_builtin_type(value)
    if inferred_data_type is None:
        raise InvalidParameterException('Unknown value type,'
                                        ' cannot infer from value: %s, type: %s' % (value, type(value)))

    return data_type.cast_value(value, inferred_data_type)


def validate_value(value, field_type):
    datahub_type = _datahub_types_dict[field_type]
    result = _validate_builtin_value(value, datahub_type)
    datahub_type.validate_value(result)
    return result
