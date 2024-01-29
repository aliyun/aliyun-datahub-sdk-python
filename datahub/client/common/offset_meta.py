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


class OffsetMeta:

    def __init__(self, version_id, session_id):
        self._version_id = version_id
        self._session_id = session_id

    @property
    def version_id(self):
        return self._version_id

    @version_id.setter
    def version_id(self, value):
        self._version_id = value

    @property
    def session_id(self):
        return self._session_id

    @session_id.setter
    def session_id(self, value):
        self._session_id = value


class ConsumeOffset(OffsetMeta):
    def __init__(self, sequence, timestamp, batch_index=0, version_id=-1, session_id=""):
        super().__init__(version_id, session_id)
        self._sequence = sequence
        self._timestamp = timestamp
        self._batch_index = batch_index
        self._next_cursor = ""

    def reset_timestamp(self, timestamp):
        self._next_cursor = None
        self._sequence = -1
        self._batch_index = 0
        self._timestamp = timestamp

    def to_string(self):
        return "({}:{}:{})".format(self._sequence, self._timestamp, self._batch_index)

    @property
    def next_cursor(self):
        return self._next_cursor

    @next_cursor.setter
    def next_cursor(self, value):
        self._next_cursor = value

    @property
    def sequence(self):
        return self._sequence

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def batch_index(self):
        return self._batch_index
