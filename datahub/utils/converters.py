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

import datetime

import six

GMT_FORMAT = '%a, %d %b %Y %H:%M:%S GMT'


def indent(text, n_spaces):
    if n_spaces <= 0:
        return text
    block = ' ' * n_spaces
    return '\n'.join((block + it) if not it else it
                     for it in text.split('\n'))


def gen_rfc822_date():
    date_str = datetime.datetime.utcnow().strftime(GMT_FORMAT)
    return date_str


def to_binary(text, encoding='utf-8'):
    if text is None:
        return text
    if isinstance(text, six.text_type):
        return text.encode(encoding)
    elif isinstance(text, (six.binary_type, bytearray)):
        return bytes(text)
    return str(text).encode(encoding) if six.PY3 else str(text)


def to_text(binary, encoding='utf-8'):
    if binary is None:
        return binary
    if isinstance(binary, (six.binary_type, bytearray)):
        return binary.decode(encoding)
    elif isinstance(binary, six.text_type):
        return binary
    return str(binary) if six.PY3 else str(binary).decode(encoding)


def to_str(text, encoding='utf-8'):
    return to_text(text, encoding=encoding) if six.PY3 else to_binary(text, encoding=encoding)


def bool_to_str(value):
    return to_str(None) if value is None else to_str(value).lower()
