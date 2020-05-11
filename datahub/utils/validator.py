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

from __future__ import absolute_import, print_function

import re
from functools import wraps

import six
from funcsigs import signature

from . import ErrorMessage
from ..exceptions import InvalidParameterException

PROJECT_NAME_REGULAR_EXPRESSION = r'^[a-zA-Z]+[a-zA-Z0-9_]*'
TOPIC_NAME_REGULAR_EXPRESSION = r'^[a-zA-Z]+[a-zA-Z0-9_]*'

PROJECT_NAME_MIN_LENGTH = 3
PROJECT_NAME_MAX_LENGTH = 32
TOPIC_NAME_MIN_LENGTH = 1
TOPIC_NAME_MAX_LENGTH = 128


def is_valid_str(text, regular_expression, min_length, max_length):
    if not text or not isinstance(text, six.string_types):
        return False
    if len(text) < min_length or len(text) > max_length:
        return False
    pattern = re.compile(regular_expression)
    result = pattern.match(text)
    return result is not None and len(result.group()) == len(text)


def check_project_name_valid(project_name):
    return is_valid_str(project_name, PROJECT_NAME_REGULAR_EXPRESSION, PROJECT_NAME_MIN_LENGTH,
                        PROJECT_NAME_MAX_LENGTH)


def check_topic_name_valid(topic_name):
    return is_valid_str(topic_name, TOPIC_NAME_REGULAR_EXPRESSION, TOPIC_NAME_MIN_LENGTH, TOPIC_NAME_MAX_LENGTH)


def check_empty(variable):
    return variable is None or not variable


def check_type(variable, *args):
    for data_type in args:
        if isinstance(variable, data_type):
            return True
    return False


def check_negative(variable):
    return variable < 0


def check_positive(variable):
    return variable > 0


def type_assert(*type_args, **type_kwargs):
    def decorate(func):
        signature_ = signature(func)
        bound_types = signature_.bind_partial(*type_args, **type_kwargs).arguments

        @wraps(func)
        def wrapper(*args, **kwargs):
            bound_values = signature_.bind(*args, **kwargs)
            # Enforce type assertions across supplied arguments
            for name, value in bound_values.arguments.items():
                if name in bound_types:
                    # pre check for special argument
                    if name == 'cursor' and type(value).__name__ == 'GetCursorResult':
                        raise InvalidParameterException('param cursor should be type of str, get the cursor from '
                                                        'getCursorResult')

                    if not isinstance(value, bound_types[name]):
                        # for python 2.7
                        if not (type(value).__name__ == "unicode" and (str in bound_types[name] if isinstance(bound_types[name], tuple) else bound_types[name] == str)):
                            raise InvalidParameterException(ErrorMessage.INVALID_TYPE %
                                                            (name, bound_types[name].__name__)
                                                            + ", input is: " + type(value).__name__)
            return func(*args, **kwargs)

        return wrapper

    return decorate
