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
from .record import Schema, RecordSchema, RecordType
from .. import errors

class Topic(RestModel):
    """
    Topic class, there was two topic type: ``Tuple`` and ``Blob``

    :Example:

    >>> topic = Topic(name=topic_name)
    >>>
    >>> topic.project_name = project_name
    >>>
    >>> topic.shard_count = 3
    >>>
    >>> topic.life_cycle = 7
    >>>
    >>> topic.record_type = RecordType.TUPLE
    >>>
    >>> topic.record_schema = RecordSchema.from_lists(['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'], [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

    .. seealso:: :class:`datahub.models.RecordSchema`, :class:`datahub.models.RecordType`, :class:`datahub.models.FieldType`
    """
    __slots__ = ('_project_name', '_shard_count', '_life_cycle', '_record_type', '_record_schema')

    def __init__(self, *args, **kwds):
        super(Topic, self).__init__(*args, **kwds)
        self._project_name = kwds['project_name'] if 'project_name' in kwds else ''
        self._shard_count = kwds['shard_count'] if 'shard_count' in kwds else 0
        self._life_cycle = kwds['life_cycle'] if 'life_cycle' in kwds else 0
        self._record_type = kwds['record_type'] if 'record_type' in kwds else ''
        self._record_schema = kwds['record_schema'] if 'record_schema' in kwds else None

    @property
    def project_name(self):
        return self._project_name

    @project_name.setter
    def project_name(self, value):
        self._project_name = value

    @property
    def shard_count(self):
        return self._shard_count

    @shard_count.setter
    def shard_count(self, value):
        self._shard_count = value

    @property
    def life_cycle(self):
        return self._life_cycle

    @life_cycle.setter
    def life_cycle(self, value):
        self._life_cycle = value

    @property
    def record_type(self):
        return self._record_type

    @record_type.setter
    def record_type(self, value):
        self._record_type = value

    @property
    def record_schema(self):
        return self._record_schema

    @record_schema.setter
    def record_schema(self, value):
        self._record_schema = value

    def __str__(self):
        topicjson = {
            "name": "%s" % self._name,
            "shard_count": self._shard_count,
            "life_cycle": self._life_cycle,
            "record_type": "%s" % self._record_type,
            "comment": "%s" % self._comment,
            "create_time": self._create_time,
            "last_modify_time": self._last_modify_time
        }
        if RecordType.TUPLE == self._record_type:
            topicjson["record_schema"] = self._record_schema.to_json_string()
        return json.dumps(topicjson)

    def __hash__(self):
        return hash((type(self), self._name, self._shard_count, self._life_cycle, self._record_type, self._record_schema, self._comment, self._create_time, self._last_modify_time))

    def throw_exception(self, response_result):
        if 'TopicAlreadyExist' == response_result.error_code:
            raise errors.ObjectAlreadyExistException(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)
        elif 'NoSuchProject' == response_result.error_code or 'NoSuchTopic' == response_result.error_code:
            raise errors.NoSuchObjectException(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)
        elif 'InvalidParameter' == response_result.error_code:
            raise errors.InvalidParameterException(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)
        elif response_result.status_code >= 500:
            raise errors.ServerInternalError(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)
        else:
            raise errors.DatahubException(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)

    def resource(self):
        if not self._project_name:
            raise ValueError('project name must not be empty')
        return "/projects/%s/topics/%s" %(self._project_name, self._name)

    def encode(self, method):
        ret = {}
        if HTTPMethod.POST == method:
            data = {
                "ShardCount": self._shard_count,
                "Lifecycle": self._life_cycle,
                "RecordType": "%s" % self._record_type,
                "Comment": "%s" % self._comment 
            }
            if RecordType.TUPLE == self._record_type:
                if isinstance(self._record_schema, RecordSchema):
                    data['RecordSchema'] = self._record_schema.to_json_string()
                elif isinstance(self._record_schema, dict):
                    data['RecordSchema'] = RecordSchema.from_dict(self._record_schema).to_json_string()
                else:
                    data['RecordSchema'] = self._record_schema
            ret["data"] = json.dumps(data)
        elif HTTPMethod.PUT == method:
            data = {
                "Lifecycle": self._life_cycle,
                "Comment": "%s" % self._comment
            }
            ret["data"] = json.dumps(data)

        return ret

    def decode(self, method, resp):
        if HTTPMethod.GET == method:
            content = json.loads(resp.content)
            self._shard_count = content['ShardCount']
            self._life_cycle = content['Lifecycle']
            self._record_type = content['RecordType']
            if RecordType.TUPLE == self._record_type:
                self._record_schema = RecordSchema.from_jsonstring(content['RecordSchema'])
            self._comment = content['Comment']
            self._create_time = content['CreateTime']
            self._last_modify_time = content['LastModifyTime']

class Topics(RestModel):
    """
    Topics class.
    List topics of a project interface will use it.
    """

    __slots__ = ('_project_name', '_topic_names')

    def __init__(self, project_name=''):
        self._project_name = project_name
        self._topic_names = []

    @property
    def project_name(self):
        return self._project_name

    @project_name.setter
    def project_name(self, value):
        self._project_name = value

    def __len__(self):
        return len(self._topic_names)

    def append(self, topic_name):
        self._topic_names.append(topic_name)

    def extend(self, topic_names):
        self._topic_names.extend(topic_names)

    def __setitem__(self, index, topic_name):
        if index < 0  or index > len(self._topic_names) - 1:
            raise ValueError('index out range')
        self._topic_names[index] = topic_name

    def __getitem__(self, index):
        if index < 0  or index > len(self._topic_names) - 1:
            raise ValueError('index out range')
        return self._topic_names[index]

    def __str__(self):
        topicsjson = {}
        topicsjson['TopicNames'] = []
        for topic_name in self._topic_names:
            topicsjson['TopicNames'].append(topic_name)
        return json.dumps(topicsjson)

    def __iter__(self):
        for name in self._topic_names:
            yield name

    def throw_exception(self, response_result):
        if 'NoSuchProject' == response_result.error_code:
            raise errors.NoSuchObjectException(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)
        elif response_result.status_code >= 500:
            raise errors.ServerInternalError(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)
        else:
            raise errors.DatahubException(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)

    def resource(self):
        if not self._project_name:
            raise ValueError('project name must be provide')
        return "/projects/%s/topics" % self._project_name

    def encode(self, method):
        ret = {}
        return ret

    def decode(self, method, resp):
        if HTTPMethod.GET == method:
            content = json.loads(resp.content)
            for topic_name in content['TopicNames']:
                self.append(topic_name)
