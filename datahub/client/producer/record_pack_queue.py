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
import queue
import threading
from .record_pack import RecordPack


class RecordPackQueue:

    def __init__(self, max_buffer_size, max_buffer_record_count, max_buffer_time, max_record_pack_queue_limit):
        self._lock = threading.Lock()
        self._last_obtain_time = time.time()
        self._max_buffer_size = max_buffer_size
        self._max_buffer_record_count = max_buffer_record_count
        self._max_buffer_time = max_buffer_time
        self._ready_record_packs = queue.Queue(max_record_pack_queue_limit)
        self._current_record_pack = None

    def flush(self):
        self.__merge_current_pack(True)

    def obtain_ready_record_pack(self):
        try:
            return self._ready_record_packs.get_nowait()
        except queue.Empty:
            return None

    def append_record(self, records):
        result = self.__try_append(records)

        with self._lock:
            self.__merge_current_pack(False)

        if result is None:
            with self._lock:
                self._current_record_pack = RecordPack(self._max_buffer_size, self._max_buffer_record_count, self._max_buffer_time)
                result = self._current_record_pack.try_append(records)
        return result

    def __try_append(self, records):
        with self._lock:
            return None if self._current_record_pack is None else self._current_record_pack.try_append(records)

    def __merge_current_pack(self, force):
        if self._current_record_pack is not None and (force or self._current_record_pack.is_ready()):
            self._ready_record_packs.put(self._current_record_pack)
            self._current_record_pack = None
