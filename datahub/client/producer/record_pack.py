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


import time
from concurrent.futures import Future


class RecordPack:

    def __init__(self, max_buffer_size, max_buffer_record_count, max_buffer_time):
        self._is_ready = False
        self._init_time = time.time()
        self._curr_size = 0
        self._curr_count = 0

        self._max_buffer_size = max_buffer_size
        self._max_buffer_record_count = max_buffer_record_count
        self._max_buffer_time = max_buffer_time

        self._records = []
        self._write_result_futures = []

    def is_ready(self):
        return self._is_ready or time.time() - self._init_time >= self._max_buffer_time

    def try_append(self, records):
        size = self.__get_total_records_size(records)
        if (self._curr_size + size < self._max_buffer_size and self._curr_count + len(records) <= self._max_buffer_record_count) or self._curr_count == 0:
            return self.__append_records(records, size)
        else:
            self._is_ready = True
            return None

    def __append_records(self, records, size):
        self._records += records
        self._curr_size += size
        self._curr_count += len(records)

        write_future = Future()
        self._write_result_futures.append(write_future)
        return write_future

    @property
    def init_time(self):
        return self._init_time

    @property
    def curr_size(self):
        return self._curr_size

    @property
    def curr_count(self):
        return self._curr_count

    @property
    def records(self):
        return self._records

    @property
    def write_result_futures(self):
        return self._write_result_futures

    def __get_total_records_size(self, records):
        return sum([len(record.encode_values()) for record in records])
