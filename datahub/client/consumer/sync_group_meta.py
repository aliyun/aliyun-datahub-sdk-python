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


class SyncGroupMeta:

    def __init__(self):
        self._release_shards = set()
        self._read_end_shards = set()
        self._active_shards = set()

    def on_shard_release(self, shards):
        for shard in shards:
            if shard in self._read_end_shards:
                self._read_end_shards.remove(shard)
                self._active_shards.remove(shard)
            if shard not in self._release_shards:
                self._release_shards.add(shard)

    def on_shard_read_end(self, shards):
        for shard in shards:
            if shard not in self._read_end_shards:
                self._read_end_shards.add(shard)
            if shard in self._release_shards:
                self._release_shards.remove(shard)

    def need_sync_group(self):
        return len(self._release_shards) != 0 or len(self._read_end_shards) != 0

    def clear_shard_release(self):
        self._release_shards.clear()

    def on_heartbeat_done(self, shardIds):
        for shard in shardIds:
            self._active_shards.add(shard)

    def get_valid_shards(self):
        return self._active_shards.difference(self._read_end_shards)

    @property
    def release_shards(self):
        return self._release_shards

    @property
    def read_end_shards(self):
        return self._read_end_shards
