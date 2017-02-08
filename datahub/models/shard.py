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

class ShardState(object):
    """
    Shard state
    """
    OPENING = 'OPENING'
    ACTIVE = 'ACTIVE'
    CLOSED = 'CLOSED'
    CLOSING = 'CLOSING'

class Shard(object):
    """
    Shard class
    """
    __slots__ = ('_shard_id', '_state', '_closed_time', '_begin_hash_key', '_end_hash_key', '_parent_shard_ids', '_left_shard_id', '_right_shard_id')
    def __init__(self, *args, **kwds):
        self._shard_id = kwds['ShardId'] if 'ShardId' in kwds else ''
        self._state = kwds['State'] if 'State' in kwds else ''
        self._closed_time = kwds['ClosedTime'] if 'ClosedTime' in kwds else 0
        self._begin_hash_key = kwds['BeginHashKey'] if 'BeginHashKey' in kwds else ''
        self._end_hash_key = kwds['EndHashKey'] if 'EndHashKey' in kwds else ''
        self._parent_shard_ids = kwds['ParentShardIds'] if 'ParentShardIds' in kwds else []
        self._left_shard_id = kwds['LeftShardId'] if 'LeftShardId' in kwds else ''
        self._right_shard_id = kwds['RightShardId'] if 'RightShardId' in kwds else ''

    @property
    def shard_id(self):
        return self._shard_id

    @shard_id.setter
    def shard_id(self, value):
        self._shard_id = value

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state = value

    @property
    def closed_time(self):
        return self._closed_time

    @closed_time.setter
    def closed_time(self, value):
        self._closed_time = value

    @property
    def begin_hash_key(self):
        return self._begin_hash_key

    @begin_hash_key.setter
    def begin_hash_key(self, value):
        self._begin_hash_key = value

    @property
    def end_hash_key(self):
        return self._end_hash_key

    @end_hash_key.setter
    def end_hash_key(self, value):
        self._end_hash_key = value

    @property
    def parent_shard_ids(self):
        return self._parent_shard_ids

    @parent_shard_ids.setter
    def parent_shard_ids(self, value):
        self._parent_shard_ids = value

    @property
    def left_shard_id(self):
        return self._left_shard_id

    @left_shard_id.setter
    def left_shard_id(self, value):
        self._left_shard_id = value

    @property
    def right_shard_id(self):
        return self._right_shard_id

    @right_shard_id.setter
    def right_shard_id(self, value):
        self._right_shard_id = value

    def to_json(self):
        return {
            "ShardId": self._shard_id,
            "State": self._state,
            "ClosedTime": self._closed_time,
            "BeginHashKey": self._begin_hash_key,
            "EndHashKey": self._end_hash_key,
            "ParentShardIds": self._parent_shard_ids,
            "LeftShardId": self._left_shard_id,
            "RightShardId": self._right_shard_id
        }

    def __str__(self):
        return json.dumps(self.to_json())

    def __hash__(self):
        return hash((type(self), self._shard_id))

    def __eq__(self, other):
        if other is None:
            return False

        if not isinstance(other, type(self)):
            return False

        return self._shard_id == other._shard_id

class ShardAction(object):
    """
    Shard action
    """
    LIST = 'list'
    MERGE = 'merge'
    SPLIT = 'split'

class Shards(RestModel):
    """
    Shards class, will be used by list shard interface
    """
    def __init__(self, action, *args, **kwds):
        self.__action = action
        super(Shards, self).__init__(*args, **kwds)
        self._project_name = kwds['project_name'] if 'project_name' in kwds else ''
        self._topic_name = kwds['topic_name'] if 'topic_name' in kwds else ''
        self._shard_list = []

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
    def shard_list(self):
        return self._shard_list

    def set_splitinfo(self, split_shard_id, split_key):
        self.__split_shard_id = split_shard_id
        self.__split_key = split_key

    def set_mergeinfo(self, shard_id, adj_shard_id):
        self.__shard_id = shard_id
        self.__adj_shard_id = adj_shard_id

    def __len__(self):
        return len(self._shard_list)

    def append(self, shard):
        self._shard_list.append(shard)

    def extend(self, shards):
        self._shard_list.extend(shards)

    def __setitem__(self, index, shard):
        if index < 0  or index > len(self._shard_list) - 1:
            raise ValueError('index out range')
        self._shard_list[index] = shard

    def __getitem__(self, index):
        if index < 0  or index > len(self._shard_list) - 1:
            raise ValueError('index out range')
        return self._shard_list[index]

    def __str__(self):
        shardsjson = {}
        shardsjson['Shards'] = []
        for shard in self._shard_list:
            shardsjson['Shards'].append(shard.to_json())
        return json.dumps(shardsjson)

    def __iter__(self):
        for shard in self._shard_list:
            yield shard

    def throw_exception(self, response_result):
        if 'NoSuchProject' == response_result.error_code or 'NoSuchTopic' == response_result.error_code or 'NoSuchShard' == response_result.error_code:
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
        if not self._project_name or not self._topic_name:
            raise ValueError('project and topic name must not be empty')
        return "/projects/%s/topics/%s/shards" %(self._project_name, self._topic_name)

    def encode(self, method):
        ret = {}
        if HTTPMethod.POST == method:
            if ShardAction.MERGE == self.__action:
                if not self.__shard_id or not self.__adj_shard_id:
                    raise ValueError('merge shard action must provide shard id and adjacent shard id')
                data = {}
                data['Action'] = 'merge'
                data['ShardId'] = self.__shard_id
                data['AdjacentShardId'] = self.__adj_shard_id
                ret['data'] = json.dumps(data)
            elif ShardAction.SPLIT == self.__action:
                if not self.__split_shard_id or not self.__split_key:
                    raise ValueError('split shard action must provide shard id and split key')
                data = {}
                data['Action'] = 'split'
                data['ShardId'] = self.__split_shard_id
                data['SplitKey'] = self.__split_key
                ret['data'] = json.dumps(data)

        return ret

    def decode(self, method, resp):
        if HTTPMethod.GET == method:
            content = json.loads(resp.content)
            for item in content['Shards']:
                shard = Shard()
                shard.shard_id = item['ShardId']
                shard.state = item['State']
                if ShardState.CLOSED == shard.state:
                    shard.closed_time = item['ClosedTime']
                shard.begin_hash_key = item['BeginHashKey']
                shard.end_hash_key = item['EndHashKey']
                shard.parent_shard_ids = item['ParentShardIds'] if 'ParentShardIds' in item else []
                shard.left_shard_id = item['LeftShardId'] if 'LeftShardId' in item else ''
                shard.right_shard_id = item['RightShardId'] if 'RightShardId' in item else ''
                self.append(shard)
        elif HTTPMethod.POST == method:
            content = json.loads(resp.content)
            if ShardAction.MERGE == self.__action:
                shard = Shard()
                shard.shard_id = content['ShardId']
                shard.begin_hash_key = content['BeginHashKey']
                shard.end_hash_key = content['EndHashKey']
                self.append(shard)
            elif ShardAction.SPLIT == self.__action:
                for item in content['NewShards']:
                    shard = Shard()
                    shard.shard_id = item['ShardId']
                    shard.begin_hash_key = item['BeginHashKey']
                    shard.end_hash_key = item['EndHashKey']
                    self.append(shard)
