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


from .utils import *
from ..exceptions import DatahubException

RECORD_HEADER_SIZE = 16


class RecordHeader:
    def __init__(self, encode_type=None, schema_version=None, total_size=None, attr_offset=None):
        self._encode_type = encode_type
        self._schema_version = schema_version
        self._total_size = total_size
        self._attr_offset = attr_offset

    @staticmethod
    def serialize(encode_type, schema_version, total_size, attr_offset):
        header = bytes()
        header += int2byte(encode_type)
        header += int2byte(schema_version)
        header += int2byte(total_size, unsigned=True)
        header += int2byte(attr_offset, unsigned=True)
        return header

    @staticmethod
    def deserialize(header):
        if len(header) != RECORD_HEADER_SIZE:
            raise DatahubException("Record header length should be {}".format(RECORD_HEADER_SIZE))
        return RecordHeader(
            byte2int(header[:4]),
            byte2int(header[4:8]),
            byte2int(header[8:12]),
            byte2int(header[12:16])
        )

    @property
    def encode_type(self):
        return self._encode_type

    @encode_type.setter
    def encode_type(self, encode_type):
        self._encode_type = encode_type

    @property
    def schema_version(self):
        return self._schema_version

    @schema_version.setter
    def schema_version(self, schema_version):
        self._schema_version = schema_version

    @property
    def total_size(self):
        return self._total_size

    @total_size.setter
    def total_size(self, total_size):
        self._total_size = total_size

    @property
    def attr_offset(self):
        return self._attr_offset

    @attr_offset.setter
    def attr_offset(self, attr_offset):
        self._attr_offset = attr_offset
