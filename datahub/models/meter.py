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

class MeteringInfo(RestModel):
    """
    Metering info class
    """
    __slots__ = ('_project_name', '_topic_name', '_shard_id', '_active_time', '_storage')
    def __init__(self, *args, **kwds):
        super(MeteringInfo, self).__init__(*args, **kwds)
        self._project_name = kwds['project_name'] if 'project_name' in kwds else ''
        self._topic_name = kwds['topic_name'] if 'topic_name' in kwds else ''
        self._shard_id = kwds['shard_id'] if 'shard_id' in kwds else ''
        self._active_time = kwds['active_time'] if 'active_time' in kwds else 0
        self._storage = kwds['storage'] if 'storage' in kwds else 0

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
    def active_time(self):
        return self._active_time

    @active_time.setter
    def active_time(self, value):
        self._active_time = value

    @property
    def storage(self):
        return self._storage

    @storage.setter
    def storage(self, value):
        self._storage = value

    def to_json(self):
        return {
            "Project": self._project_name,
            "Topic": self._topic_name,
            "ShardId": self._shard_id,
            "ActiveTime": self._active_time,
            "Storage": self._storage
        }

    def __str__(self):
        return json.dumps(self.to_json())

    def __hash__(self):
        return hash((type(self), self._project_name, self._topic_name, self._shard_id))

    def __eq__(self, other):
        if other is None:
            return False

        if not isinstance(other, type(self)):
            return False

        return self._project_name == other._project_name and \
                self._topic_name == other._topic_name and \
                self._shard_id == other._shard_id

    def throw_exception(self, response_result):
        if 'NoSuchProject' == response_result.error_code or 'NoSuchTopic' == response_result.error_code or 'NoSuchShard' == response_result.error_code or 'NoSuchMeteringInfo' == response_result.error_code:
            raise errors.NoSuchObjectException(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)
        elif 'InvalidShardOperation' == response_result.error_code:
            raise errors.InvalidShardOperationException(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)
        elif 'LimitExceeded' == response_result.error_code:
            raise errors.LimitExceededException(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)
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
            data = {}
            data['Action'] = 'meter'
            ret['data'] = json.dumps(data)

        return ret

    def decode(self, method, resp):
        if HTTPMethod.POST == method:
            content = json.loads(resp.content)
            self.active_time = content['ActiveTime']
            self.storage = content['Storage']
