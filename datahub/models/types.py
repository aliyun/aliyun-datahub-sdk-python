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
    def do_cast(self, value, data_type):
        raise NotImplementedError

    def cast_value(self, value, data_type):
        self._can_cast_or_throw(value, data_type)
        try:
            return self.do_cast(value, data_type)
        except ValueError as e:
            raise InvalidParameterException(e)


# Tinyint
class Tinyint(DataType):
    _bounds = (-128, 127)

    def can_implicit_cast(self, other):
        if isinstance(other, (Smallint, Integer, Bigint, Float, Double, String, Timestamp)):
            return True
        return super(Tinyint, self).can_implicit_cast(other)

    def validate_value(self, val):
        if val is None:
            return True
        smallest, largest = self._bounds
        if smallest <= val <= largest:
            return True
        raise InvalidParameterException('InvalidData: Tinyint(%s) out of range' % val)

    def do_cast(self, value, data_type):
        return int(value)


# Smallint
class Smallint(DataType):
    _bounds = (-32768, 32767)

    def can_implicit_cast(self, other):
        if isinstance(other, (Tinyint, Integer, Bigint, Float, Double, String, Timestamp)):
            return True
        return super(Smallint, self).can_implicit_cast(other)

    def validate_value(self, val):
        if val is None:
            return True
        smallest, largest = self._bounds
        if smallest <= val <= largest:
            return True
        raise InvalidParameterException('InvalidData: Smallint(%s) out of range' % val)

    def do_cast(self, value, data_type):
        return int(value)


# Integer
class Integer(DataType):
    _bounds = (-2147483648, 2147483647)

    def can_implicit_cast(self, other):
        if isinstance(other, (Tinyint, Smallint, Bigint, Float, Double, String, Timestamp)):
            return True
        return super(Integer, self).can_implicit_cast(other)

    def validate_value(self, val):
        if val is None:
            return True
        smallest, largest = self._bounds
        if smallest <= val <= largest:
            return True
        raise InvalidParameterException('InvalidData: Integer(%s) out of range' % val)

    def do_cast(self, value, data_type):
        return int(value)


# Bigint
class Bigint(DataType):
    _bounds = (-9223372036854775808, 9223372036854775807)

    def can_implicit_cast(self, other):
        if isinstance(other, (Tinyint, Smallint, Integer, Float, Double, String, Timestamp)):
            return True
        return super(Bigint, self).can_implicit_cast(other)

    def validate_value(self, val):
        if val is None:
            return True
        smallest, largest = self._bounds
        if smallest <= val <= largest:
            return True
        raise InvalidParameterException('InvalidData: Bigint(%s) out of range' % val)

    def do_cast(self, value, data_type):
        if six.PY2:
            return long(value)
        return int(value)


# Float
class Float(DataType):
    def can_implicit_cast(self, other):
        if isinstance(other, (Tinyint, Smallint, Integer, Double, Bigint, String)):
            return True
        return super(Float, self).can_implicit_cast(other)

    def do_cast(self, value, data_type):
        return float(value)


# Double
class Double(DataType):
    def can_implicit_cast(self, other):
        if isinstance(other, (Tinyint, Smallint, Integer, Float, Bigint, String)):
            return True
        return super(Double, self).can_implicit_cast(other)

    def do_cast(self, value, data_type):
        return float(value)


# Decimal
class Decimal(DataType):
    def can_implicit_cast(self, other):
        if isinstance(other, (Tinyint, Smallint, Integer, Float, Double, Bigint, String)):
            return True
        return super(Decimal, self).can_implicit_cast(other)

    def do_cast(self, value, data_type):
        return decimal.Decimal(value)


# String
class String(DataType):
    def can_implicit_cast(self, other):
        if isinstance(other, (Tinyint, Smallint, Integer, Bigint, Float, Double, Timestamp)):
            return True
        return super(String, self).can_implicit_cast(other)

    def validate_value(self, val):
        return True

    def do_cast(self, value, data_type):
        return utils.to_text(value)


# Timestamp
class Timestamp(DataType):
    _ticks_bound = (-62135798400000000, 253402271999000000)

    def can_implicit_cast(self, other):
        if isinstance(other, String):
            return True
        return super(Timestamp, self).can_implicit_cast(other)

    def validate_value(self, val):
        if val is None:
            return True

        smallest, largest = self._ticks_bound
        if smallest <= val <= largest:
            return True
        raise InvalidParameterException('InvalidData: Timestamp(%s) out of range' % val)

    def do_cast(self, value, data_type):
        if six.PY2:
            return long(value)
        return int(value)


# Boolean
class Boolean(DataType):

    def can_implicit_cast(self, other):
        if isinstance(other, String):
            return True
        return super(Boolean, self).can_implicit_cast(other)

    def do_cast(self, value, data_type):
        if isinstance(data_type, String):
            if 'true' == value.lower():
                return True
            elif 'false' == value.lower():
                return False
        raise ValueError('can not cast to [%s] bool' % value)


#####################################################################
# above is 10 type defined to verify field value
#####################################################################

float_builtins = (float,)
bool_builtins = (bool,)
integer_builtins = six.integer_types if isinstance(six.integer_types, tuple) else (six.integer_types,)
string_builtins = six.string_types if isinstance(six.string_types, tuple) else (six.string_types,)
decimal_builtins = (decimal.Decimal,)

try:
    import numpy as np

    integer_builtins += (np.integer, )
    float_builtins += (np.float16, np.float32, np.float64, np.float128, )
    if np.__version__ < "1.20.0":
        float_builtins += (np.float, )
except ImportError:
    pass

tinyint_type = Tinyint()
smallint_type = Smallint()
integer_type = Integer()
bigint_type = Bigint()
float_type = Float()
double_type = Double()
string_type = String()
timestamp_type = Timestamp()
boolean_type = Boolean()
decimal_type = Decimal()


_datahub_types_dict = {
    FieldType.TINYINT: tinyint_type,
    FieldType.SMALLINT: smallint_type,
    FieldType.INTEGER: integer_type,
    FieldType.BIGINT: bigint_type,
    FieldType.FLOAT: float_type,
    FieldType.DOUBLE: double_type,
    FieldType.STRING: string_type,
    FieldType.TIMESTAMP: timestamp_type,
    FieldType.BOOLEAN: boolean_type,
    FieldType.DECIMAL: decimal_type
}

_builtin_types_dict = {
    tinyint_type: integer_builtins,
    smallint_type: integer_builtins,
    integer_type: integer_builtins,
    bigint_type: integer_builtins,
    float_type: float_builtins,
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


def validate_value(value, field):
    if field.allow_null and field.type != FieldType.STRING and value == '':
        return None
    datahub_type = _datahub_types_dict[field.type]
    result = _validate_builtin_value(value, datahub_type)
    datahub_type.validate_value(result)
    return result
