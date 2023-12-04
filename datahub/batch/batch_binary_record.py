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


import crcmod.predefined
from .binary_record import BinaryRecord
from .batch_header import BatchHeader, BATCH_HEAD_SIZE
from ..models.compress import *
from ..exceptions import DatahubException, InvalidParameterException


class BatchBinaryRecord:
    """
    Batch binary record
    """
    def __init__(self, records=None):
        self._version = None
        self._length = None
        self._raw_size = None
        self._crc32 = None
        self._attributes = None
        self._record_count = None

        self._records = records if records else []      # list of BinaryRecord
        self._buffer = bytes()

    def add_record(self, record):
        if not record or not isinstance(record, BinaryRecord):
            raise InvalidParameterException("Add record fail. record must be a valid BinaryRecord instance")
        self._records.append(record)

    def serialize(self, compress_type=None):
        try:
            # Add BinaryRecord list
            for record in self._records:
                record_byte = record.serialize()
                self._buffer += record_byte

            # compress
            self.__compress(compress_type)

            crc32c = crcmod.predefined.mkCrcFun('crc-32c')
            self._crc32 = crc32c(self._buffer) & 0xffffffff
            self._version = 0
            self._record_count = len(self._records)

            # Add Batch header
            header_byte = BatchHeader.serialize(
                self._version,
                self._length,
                self._raw_size,
                self._crc32,
                self._attributes,
                self._record_count
            )
            if len(header_byte) != BATCH_HEAD_SIZE:
                raise DatahubException("Batch header size should be {}, it is {}".format(BATCH_HEAD_SIZE, len(header_byte)))
            return header_byte + self._buffer
        except Exception as e:
            raise DatahubException("Serialize batch record fail. {}".format(e))

    def __compress(self, compress_type=None):
        self._raw_size = len(self._buffer)
        self._length = self._raw_size + BATCH_HEAD_SIZE

        try:
            data_compressor = get_compressor(compress_type)
            compress_data = data_compressor.compress(self._buffer)

            if len(compress_data) < self._raw_size:
                self._attributes = compress_type.get_index() | 8
                self._buffer = compress_data
                self._length = BATCH_HEAD_SIZE + len(compress_data)
            else:
                self._attributes = CompressFormat.NONE.get_index() | 8
        except Exception as e:
            raise DatahubException("Compress data fail. {}".format(e))

    @property
    def records(self):
        return self._records

    @property
    def buffer(self):
        return self._buffer

    @buffer.setter
    def buffer(self, buffer):
        self._buffer = buffer
