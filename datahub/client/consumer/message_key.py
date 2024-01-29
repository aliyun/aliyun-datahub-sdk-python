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


import atomic


class MessageKey:

    def __init__(self, shard_id, offset):
        self._ready = atomic.AtomicLong(0)
        self._shard_id = shard_id
        self._offset = offset

    def ack(self):
        self._ready.get_and_set(1)

    def is_ready(self):
        return self._ready.value != 0

    def to_string(self):
        return "({}@{}:{}:{})".format(self._shard_id, self._offset.sequence, self._offset.timestamp, self._offset.batch_index)

    @property
    def offset(self):
        return self._offset

    @property
    def shard_id(self):
        return self._shard_id
