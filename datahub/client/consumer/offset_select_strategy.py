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


from ..common.timer import Timer
from ..common.constant import Constant


class OffsetSelectStrategy:

    def __init__(self):
        self._timer = Timer(Constant.READER_SELECT_EMPTY_SHARD_TIMEOUT)
        self._offset_map = dict()
        self._empty_shards = set()

    def add_shard(self, shard_id):
        self._offset_map[shard_id] = -1
        if shard_id in self._empty_shards:
            self._empty_shards.remove(shard_id)

    def remove_shard(self, shard_id):
        if shard_id in set(self._offset_map.keys()):
            self._offset_map.pop(shard_id)
        if shard_id in set(self._empty_shards):
            self._empty_shards.add(shard_id)

    def after_read(self, shard_id, record):
        if not record:
            self._empty_shards.add(shard_id)
        else:
            self._offset_map[shard_id] = record.system_time

    def get_next_shard(self):
        shard_id = self.__find_oldest_shard()

        if shard_id is None or self._timer.is_expired():
            self._empty_shards.clear()
            self._timer.reset()
        return shard_id

    def __find_oldest_shard(self):
        if len(self._offset_map) == 0:
            return None
        oldest_timestamp = -1
        oldest_shard_id = None
        for shard_id, timestamp in self._offset_map.items():
            if shard_id in self._empty_shards:
                continue
            if oldest_shard_id is None or timestamp < oldest_timestamp:
                oldest_timestamp = timestamp
                oldest_shard_id = shard_id
        return oldest_shard_id
