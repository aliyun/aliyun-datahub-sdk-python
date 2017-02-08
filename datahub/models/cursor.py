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

from __future__ import absolute_import

import json
from .rest import HTTPMethod, RestModel
from .. import errors

class CursorType(object):
    """
    Cursor type
    """
    OLDEST = 'OLDEST'
    LATEST = 'LATEST'
    SYSTEM_TIME = 'SYSTEM_TIME'

class Cursor(RestModel):
    """
    Cursor class

    When get records from a shard, you first set a cursor
    """
    __slots__ = ('_project_name', '_topic_name', '_shard_id', '_type', '_system_time', '_cursor', '_sequence', '_record_time')

    def __init__(self, *args, **kwds):
        super(Cursor, self).__init__(*args, **kwds)
        self._project_name = kwds['project_name'] if 'project_name' in kwds else ''
        self._topic_name = kwds['topic_name'] if 'topic_name' in kwds else ''
        self._shard_id = kwds['shard_id'] if 'shard_id' in kwds else ''
        self._type = kwds['type'] if 'type' in kwds else ''

    @property
    def project_name(self):
        return self._project_name

    @project_name.setter
    def project_name(self, value):
        self._project_name = value

    @property
    def topic_name(self):
        return self._topic_name

    @topic_name.setter
    def topic_name(self, value):
        self._topic_name = value

    @property
    def shard_id(self):
        return self._shard_id

    @shard_id.setter
    def shard_id(self, value):
        self._shard_id = value

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        self._type = value

    @property
    def system_time(self):
        return self._system_time

    @system_time.setter
    def system_time(self, value):
        self._system_time = value

    @property
    def cursor(self):
        return self._cursor

    @cursor.setter
    def cursor(self, value):
        self._cursor = value

    @property
    def sequence(self):
        return self._sequence

    @sequence.setter
    def sequence(self, value):
        self._sequence = value

    @property
    def record_time(self):
        return self._record_time

    @record_time.setter
    def record_time(self, value):
        self._record_time = value

    def __str__(self):
        return self._cursor

    def __hash__(self):
        return hash((type(self), self._project_name, self._topic_name, self._cursor))

    def throw_exception(self, response_result):
        if 'NoSuchProject' == response_result.error_code or 'NoSuchTopic' == response_result.error_code or 'NoSuchShard' == response_result.error_code:
            raise errors.NoSuchObjectException(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)
        elif 'InvalidParameter' == response_result.error_code:
            raise errors.InvalidParameterException(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)
        elif response_result.status_code >= 500:
            raise errors.ServerInternalError(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)
        else:
            raise errors.DatahubException(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)

    def resource(self):
        if not self._project_name or not self._topic_name or not self._shard_id:
            raise ValueError('project name, topic name and shard id must not be empty')
        return "/projects/%s/topics/%s/shards/%s" %(self._project_name, self._topic_name, self._shard_id)

    def encode(self, method):
        ret = {}
        if HTTPMethod.POST == method:
            data = {
                "Action": "cursor",
                "Type": self._type
            }
            if CursorType.SYSTEM_TIME == self._type:
                if not self._system_time:
                    raise ValueError('SYSTEM_TIME cursor must be set system time value')
                data['SystemTime'] = self._system_time
            elif CursorType.OLDEST == self._type or CursorType.LATEST:
                pass
            else:
                raise ValueError('%s cursor not support' % self._type)

            ret['data'] = json.dumps(data)

        return ret

    def decode(self, method, resp):
        if HTTPMethod.POST == method:
            content = json.loads(resp.content)
            self.cursor = content['Cursor']
            self.sequence = content['Sequence'] if 'Sequence' in content else ''
            self.record_time = content['RecordTime'] if 'RecordTime' in content else ''
