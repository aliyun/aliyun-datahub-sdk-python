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

import re
import time
import datetime
import decimal

from ..thirdparty import six
from .. import utils

class DataType(object):
    """
    Abstract data type
    """
    _singleton = True
    __slots__ = 'nullable',

    def __new__(cls, *args, **kwargs):
        if cls._singleton:
            if not hasattr(cls, '_instance'):
                cls._instance = object.__new__(cls)
                cls._hash = hash(cls)
            return cls._instance
        else:
            return object.__new__(cls)

    def __init__(self, nullable=True):
        self.nullable = nullable

    def __call__(self, nullable=True):
        return self._factory(nullable=nullable)

    def _factory(self, nullable=True):
        return type(self)(nullable=nullable)

    def __ne__(self, other):
        return not (self == other)

    def __eq__(self, other):
        return self._equals(other)

    def _equals(self, other):
        if self is other:
            return True

        other = validate_data_type(other)

        if self.nullable != other.nullable:
            return False
        if type(self) == type(other):
            return True
        return isinstance(other, type(self))

    def __hash__(self):
        return self._hash

    @property
    def name(self):
        return type(self).__name__.lower()

    def __repr__(self):
        if self.nullable:
            return self.name
        return '{0}[non-nullable]'.format(self.name)

    def __str__(self):
        return self.name.upper()

    def can_implicit_cast(self, other):
        if isinstance(other, six.string_types):
            other = validate_data_type(other)

        return isinstance(self, type(other))

    def can_explicit_cast(self, other):
        return self.can_implicit_cast(other)

    def validate_value(self, val):
        # directly return True means without checking
        return True

    def _can_cast_or_throw(self, value, data_type):
        if not self.can_implicit_cast(data_type):
            raise ValueError('Cannot cast value(%s) from type(%s) to type(%s)' % (
                value, data_type, self))

    def cast_value(self, value, data_type):
        raise NotImplementedError

class DatahubPrimitive(DataType):
    __slots__ = ()

# Bigint
class Bigint(DatahubPrimitive):
    __slots__ = ()

    _bounds = (-9223372036854775808, 9223372036854775807)

    def can_implicit_cast(self, other):
        if isinstance(other, six.string_types):
            other = validate_data_type(other)

        if isinstance(other, (Double, String, Timestamp)):
            return True
        return super(Bigint, self).can_implicit_cast(other)

    def validate_value(self, val):
        if val is None and self.nullable:
            return True
        smallest, largest = self._bounds
        if smallest <= val <= largest:
            return True
        raise ValueError('InvalidData: Bigint(%s) out of range' % val)

    def cast_value(self, value, data_type):
        self._can_cast_or_throw(value, data_type)

        return long(value)

# Double
class Double(DatahubPrimitive):
    __slots__ = ()

    def can_implicit_cast(self, other):
        if isinstance(other, six.string_types):
            other = validate_data_type(other)

        if isinstance(other, (Bigint, String)):
            return True
        return super(Double, self).can_implicit_cast(other)

    def cast_value(self, value, data_type):
        self._can_cast_or_throw(value, data_type)

        return float(value)

# String
class String(DatahubPrimitive):
    __slots__ = ()

    _max_length = 1 * 1024 * 1024  # 1M

    def can_implicit_cast(self, other):
        if isinstance(other, six.string_types):
            other = validate_data_type(other)

        if isinstance(other, (Bigint, Double, Timestamp)):
            return True
        return super(String, self).can_implicit_cast(other)

    def validate_value(self, val):
        if val is None and self.nullable:
            return True
        if len(val) <= self._max_length:
            return True
        raise ValueError("InvalidData: Length of string(%s) is more than 1M.'" % val)

    def cast_value(self, value, data_type):
        self._can_cast_or_throw(value, data_type)

        val = utils.to_text(value)
        return val

#Timestamp
class Timestamp(DatahubPrimitive):
    __slots__ = ()

    _ticks_bound = (-62135798400000000,253402271999000000)

    def can_implicit_cast(self, other):
        if isinstance(other, six.string_types):
            other = validate_data_type(other)

        if isinstance(other, String):
            return True
        return super(Timestamp, self).can_implicit_cast(other)

    def validate_value(self, val):
        if val is None and self.nullable:
            return True

        smallest, largest = self._ticks_bound
        if smallest <= val <= largest:
            return True
        raise ValueError('InvalidData: Timestamp(%s) out of range' % val)

    def cast_value(self, value, data_type):
        self._can_cast_or_throw(value, data_type)
        return long(value)

# Boolean
class Boolean(DatahubPrimitive):
    __slots__ = ()

    def cast_value(self, value, data_type):
        if isinstance(data_type, six.string_types):
            data_type = validate_data_type(data_type)

        if isinstance(data_type, String):
            if 'true' == value.lower():
                return True
            elif 'false' == value.lower():
                return False

        self._can_cast_or_throw(value, data_type)
        return value

bigint = Bigint()
double = Double()
string = String()
timestamp = Timestamp()
boolean = Boolean()

_datahub_primitive_data_types = dict(
    [(t.name, t) for t in (
        bigint, double, string, timestamp, boolean
    )]
)

def validate_data_type(data_type):
    if isinstance(data_type, DataType):
        return data_type

    if isinstance(data_type, six.string_types):
        data_type = data_type.lower()
        if data_type in _datahub_primitive_data_types:
            return _datahub_primitive_data_types[data_type]

    raise ValueError('Invalid data type: %s' % repr(data_type))

integer_builtins = six.integer_types
float_builtins = (float,)
try:
    import numpy as np
    integer_builtins += (np.integer,)
    float_builtins += (np.float,)
except ImportError:
    pass

_datahub_primitive_to_builtin_types = {
    bigint: integer_builtins,
    double: float_builtins,
    string: six.string_types,
    timestamp: integer_builtins,
    boolean: bool
}


def infer_primitive_data_type(value):
    for data_type, builtin_types in six.iteritems(_datahub_primitive_to_builtin_types):
        if isinstance(value, builtin_types):
            return data_type


def _validate_primitive_value(value, data_type):
    if value is None:
        return None
    if isinstance(value, (bytearray, six.binary_type)):
        value = value.decode('utf-8')

    builtin_types = _datahub_primitive_to_builtin_types[data_type]
    if isinstance(value, builtin_types):
        return value

    inferred_data_type = infer_primitive_data_type(value)
    if inferred_data_type is None:
        raise ValueError(
            'Unknown value type, cannot infer from value: %s, type: %s' % (value, type(value)))

    return data_type.cast_value(value, inferred_data_type)


def validate_value(value, data_type):
    if data_type in _datahub_primitive_to_builtin_types:
        res = _validate_primitive_value(value, data_type)
    else:
        raise ValueError('Unknown data type: %s' % data_type)

    data_type.validate_value(res)
    return res
