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

import abc
import json

import six
from cprotobuf.internal import encode_data

from ..batch.batch_serializer import BatchSerializer
from ..batch.utils import SchemaObject
from ..models import CursorType, RecordType, RecordSchema
from ..proto.datahub_record_proto_pb import PutRecordsRequest, GetRecordsRequest, PutBinaryRecordsRequest
from ..rest import ContentType, Headers
from ..utils import pb_message_wrap


@six.add_metaclass(abc.ABCMeta)
class RequestParams(object):
    """
    Abstract class to be implement
    """

    @abc.abstractmethod
    def content(self):
        pass

    @staticmethod
    def extra_headers():
        return {}

    def __repr__(self):
        return self.content()


class CreateProjectRequestParams(RequestParams):
    """
    Request params of create project api
    """

    __slots__ = '_comment'

    def __init__(self, comment):
        self._comment = comment

    @property
    def comment(self):
        return self._comment

    @comment.setter
    def comment(self, value):
        self._comment = value

    def content(self):
        return json.dumps({
            "Comment": self._comment
        })


class UpdateProjectRequestParams(RequestParams):
    """
    Request params of update project api
    """

    __slots__ = '_comment'

    def __init__(self, comment):
        self._comment = comment

    @property
    def comment(self):
        return self._comment

    @comment.setter
    def comment(self, value):
        self._comment = value

    def content(self):
        return json.dumps({
            "Comment": self._comment
        })


class CreateTopicRequestParams(RequestParams):
    """
    Request params of create topic api
    """

    __slots__ = ('_shard_count', '_life_cycle', '_record_type', '_record_schema',
                 '_extend_mode', '_comment')

    def __init__(self, shard_count, life_cycle, record_type, record_schema, extend_mode, comment):
        self._shard_count = shard_count
        self._life_cycle = life_cycle
        self._record_type = record_type
        self._record_schema = record_schema
        self._extend_mode = extend_mode
        self._comment = comment

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

    @property
    def extend_mode(self):
        return self._extend_mode

    @extend_mode.setter
    def extend_mode(self, value):
        self._extend_mode = value

    @property
    def comment(self):
        return self._comment

    @comment.setter
    def comment(self, value):
        self._comment = value

    def content(self):
        data = {
            "ShardCount": self._shard_count,
            "Lifecycle": self._life_cycle,
            "RecordType": self._record_type.value,
            "Comment": self._comment
        }

        if RecordType.TUPLE == self._record_type:
            if isinstance(self._record_schema, RecordSchema):
                data['RecordSchema'] = self._record_schema.to_json_string()
            else:
                data['RecordSchema'] = self._record_schema

        if self._extend_mode is not None:
            data['ExpandMode'] = 'extend' if self._extend_mode else 'split'

        return json.dumps(data)


class UpdateTopicRequestParams(RequestParams):
    """
    Request params of update topic api
    """

    __slots__ = ('_life_cycle', '_comment')

    def __init__(self, life_cycle, comment):
        self._life_cycle = life_cycle
        self._comment = comment

    @property
    def life_cycle(self):
        return self._life_cycle

    @life_cycle.setter
    def life_cycle(self, value):
        self._life_cycle = value

    @property
    def comment(self):
        return self._comment

    @comment.setter
    def comment(self, value):
        self._comment = value

    def content(self):
        return json.dumps({
            "Lifecycle": self._life_cycle,
            "Comment": self._comment
        })


class MergeShardRequestParams(RequestParams):
    """
    Request params of merge shard api
    """

    __slots__ = ('_shard_id', '_adj_shard_id')

    def __init__(self, shard_id, adj_shard_id):
        self._shard_id = shard_id
        self._adj_shard_id = adj_shard_id

    @property
    def shard_id(self):
        return self._shard_id

    @shard_id.setter
    def shard_id(self, value):
        self._shard_id = value

    @property
    def adj_shard_id(self):
        return self._adj_shard_id

    @adj_shard_id.setter
    def adj_shard_id(self, value):
        self._adj_shard_id = value

    def content(self):
        return json.dumps({
            "Action": "merge",
            "ShardId": self._shard_id,
            "AdjacentShardId": self._adj_shard_id
        })


class SplitShardRequestParams(RequestParams):
    """
    Request params of split shard api
    """

    __slots__ = ('_shard_id', '_split_key')

    def __init__(self, shard_id, split_key):
        self._shard_id = shard_id
        self._split_key = split_key

    @property
    def shard_id(self):
        return self._shard_id

    @shard_id.setter
    def shard_id(self, value):
        self._shard_id = value

    @property
    def split_key(self):
        return self._split_key

    @split_key.setter
    def split_key(self, value):
        self._split_key = value

    def content(self):
        return json.dumps({
            "Action": "split",
            "ShardId": self._shard_id,
            "SplitKey": self._split_key
        })


class GetCursorRequestParams(RequestParams):
    """
    Request params of get cursor api
    """

    __slots__ = ('_type', '_param')

    def __init__(self, cursor_type, param):
        self._type = cursor_type
        self._param = param

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        self._type = value

    @property
    def param(self):
        return self._param

    @param.setter
    def param(self, value):
        self._param = value

    def content(self):
        data = {
            "Action": "cursor",
            "Type": self._type.name
        }
        if CursorType.SYSTEM_TIME == self._type:
            data['SystemTime'] = self._param
        elif CursorType.SEQUENCE == self._type:
            data['Sequence'] = self._param
        return json.dumps(data)


class PutRecordsRequestParams(RequestParams):
    """
    Request params of put records api
    """

    __slots__ = '_record_list'

    action = 'pub'

    def __init__(self, record_list):
        self._record_list = record_list

    @property
    def record_list(self):
        return self._record_list

    @record_list.setter
    def record_list(self, value):
        self._record_list = value

    def content(self):
        return json.dumps({
            "Action": PutRecordsRequestParams.action,
            "Records": [record.to_json() for record in self._record_list]
        })


class PutPBRecordsRequestParams(PutRecordsRequestParams):
    """
    Protobuf Request params of put records api
    """
    def content(self):
        pb_put_record_request = {
            'records': []
        }
        for record in self._record_list:
            pb_put_record_request['records'].append(record.to_pb_record_entry())
        pb_data = encode_data(PutRecordsRequest, pb_put_record_request)
        return pb_message_wrap(pb_data)

    @staticmethod
    def extra_headers():
        return {
            Headers.REQUEST_ACTION: PutRecordsRequestParams.action,
            Headers.CONTENT_TYPE: ContentType.HTTP_PROTOBUF.value
        }


class PutBatchRecordsRequestParams(PutRecordsRequestParams):
    """
    Batch Request params of put records api
    """

    def __init__(self, record_list, project_name, topic_name, compress_type, schema_register):
        super().__init__(record_list)
        self._project_name = project_name
        self._topic_name = topic_name
        self._compress_type = compress_type
        self._schema_register = schema_register

    def content(self):
        schema_object = SchemaObject(self._project_name, self._topic_name, self._schema_register)
        record_data = BatchSerializer.serialize(self._compress_type, schema_object, self._record_list)
        batch_put_record_request = {
            'records': [{'data': record_data}]
        }
        batch_data = encode_data(PutBinaryRecordsRequest, batch_put_record_request)
        return pb_message_wrap(batch_data)

    @staticmethod
    def extra_headers():
        return {
            Headers.REQUEST_ACTION: PutRecordsRequestParams.action,
            Headers.CONTENT_TYPE: ContentType.HTTP_BATCH.value
        }


class GetRecordsRequestParams(RequestParams):
    """
    Request params of get records api
    """

    __slots__ = ('_cursor', '_limit_num')

    action = 'sub'

    def __init__(self, cursor, limit_num):
        self._cursor = cursor
        self._limit_num = limit_num

    @property
    def cursor(self):
        return self._cursor

    @cursor.setter
    def cursor(self, value):
        self._cursor = value

    @property
    def limit_num(self):
        return self._limit_num

    @limit_num.setter
    def limit_num(self, value):
        self._limit_num = value

    def content(self):
        return json.dumps({
            "Action": GetRecordsRequestParams.action,
            "Cursor": self._cursor,
            "Limit": self._limit_num
        })


class GetPBRecordsRequestParams(GetRecordsRequestParams):
    """
    Protobuf Request params of get records api
    """

    def content(self):
        pb_get_record_request = {
            'cursor': self._cursor,
            'limit': self._limit_num
        }
        pb_data = encode_data(GetRecordsRequest, pb_get_record_request)
        return pb_message_wrap(pb_data)

    @staticmethod
    def extra_headers(sub_id=None):
        header = {
            Headers.REQUEST_ACTION: GetRecordsRequestParams.action,
            Headers.CONTENT_TYPE: ContentType.HTTP_PROTOBUF.value
        }
        if sub_id is not None:
            header[Headers.CONTENT_SUB_ID] = sub_id
        return header


class GetBatchRecordsRequestParams(GetRecordsRequestParams):
    """
    Batch Request params of get records api
    """
    def content(self):
        batch_get_record_request = {
            'cursor': self._cursor,
            'limit': self._limit_num
        }
        batch_data = encode_data(GetRecordsRequest, batch_get_record_request)
        return pb_message_wrap(batch_data)

    @staticmethod
    def extra_headers(sub_id=None):
        header = {
            Headers.REQUEST_ACTION: GetRecordsRequestParams.action,
            Headers.CONTENT_TYPE: ContentType.HTTP_BATCH.value
        }
        if sub_id is not None:
            header[Headers.CONTENT_SUB_ID] = sub_id
        return header


class GetMeteringInfoRequestParams(RequestParams):
    """
    Request params of get metering info api
    """

    def content(self):
        return json.dumps({
            "Action": "meter"
        })


class CreateConnectorParams(RequestParams):
    """
    Request params of create data connector api
    """

    __slots__ = ('_column_fields', '_config', '_start_time')

    def __init__(self, column_fields, config, start_time):
        self._column_fields = column_fields
        if not self._column_fields:
            self._column_fields = []
        self._config = config
        self._start_time = start_time

    @property
    def column_fields(self):
        return self._column_fields

    @column_fields.setter
    def column_fields(self, value):
        self._column_fields = value

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, value):
        self._config = value

    @property
    def start_time(self):
        return self._start_time

    @start_time.setter
    def start_time(self, value):
        self._start_time = value

    def content(self):
        return json.dumps({
            "Action": "Create",
            "ColumnFields": self._column_fields,
            "Config": self._config.to_json(),
            "SinkStartTime": self._start_time
        })


class UpdateConnectorParams(RequestParams):
    """
    Request params of update data connector config api
    """

    __slots__ = '_config'

    def __init__(self, config):
        self._config = config

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, value):
        self._config = value

    def content(self):
        return json.dumps({
            "Action": "updateconfig",
            "Config": self._config.to_json()
        })


class GetConnectorShardStatusParams(RequestParams):
    """
    Request params of get data connector shard status api
    """

    __slots__ = '_shard_id'

    def __init__(self, shard_id):
        self._shard_id = shard_id

    @property
    def shard_id(self):
        return self._shard_id

    @shard_id.setter
    def shard_id(self, value):
        self._shard_id = value

    def content(self):
        data = {
            "Action": "Status"
        }
        if self._shard_id:
            data['ShardId'] = self._shard_id
        return json.dumps(data)


class ReloadConnectorParams(RequestParams):
    """
    Request params of get data connector shard status api
    """

    __slots__ = '_shard_id'

    def __init__(self, shard_id):
        self._shard_id = shard_id

    @property
    def shard_id(self):
        return self._shard_id

    @shard_id.setter
    def shard_id(self, value):
        self._shard_id = value

    def content(self):
        data = {
            "Action": "Reload"
        }
        if self._shard_id:
            data['ShardId'] = self._shard_id
        return json.dumps(data)


class AppendFieldParams(RequestParams):
    """
    Request params of append field api
    """

    __slots__ = ('_field_name', '_field_type')

    def __init__(self, field_name, field_type):
        self._field_name = field_name
        self._field_type = field_type

    @property
    def field_name(self):
        return self._field_name

    @field_name.setter
    def field_name(self, value):
        self._field_name = value

    @property
    def field_type(self):
        return self._field_type

    @field_type.setter
    def field_type(self, value):
        self._field_type = value

    def content(self):
        return json.dumps({
            "Action": "appendfield",
            "FieldName": self._field_name,
            "FieldType": self._field_type.value
        })


class AppendConnectorFieldParams(RequestParams):
    """
    Request params of append data connector field api
    """

    __slots__ = '_field_name'

    def __init__(self, field_name):
        self._field_name = field_name

    @property
    def field_name(self):
        return self._field_name

    @field_name.setter
    def field_name(self, value):
        self._field_name = value

    def content(self):
        return json.dumps({
            "Action": "appendfield",
            "FieldName": self._field_name
        })


class InitAndGetSubscriptionOffsetParams(RequestParams):
    """
    Request params of init and get subscription offset api
    """

    __slots__ = '_shard_ids'

    def __init__(self, shard_ids):
        self._shard_ids = shard_ids

    @property
    def shard_ids(self):
        return self._shard_ids

    @shard_ids.setter
    def shard_ids(self, value):
        self._shard_ids = value

    def content(self):
        return json.dumps({
            "Action": "open",
            "ShardIds": self._shard_ids
        })


class GetSubscriptionOffsetParams(RequestParams):
    """
    Request params of get subscription offset api
    """

    __slots__ = '_shard_ids'

    def __init__(self, shard_ids):
        self._shard_ids = shard_ids

    @property
    def shard_ids(self):
        return self._shard_ids

    @shard_ids.setter
    def shard_ids(self, value):
        self._shard_ids = value

    def content(self):
        data = {
            "Action": "get"
        }
        if self._shard_ids:
            data['ShardIds'] = self._shard_ids
        return json.dumps(data)


class UpdateSubscriptionOffsetParams(RequestParams):
    """
    Request params of update subscription offset api
    """

    __slots__ = '_offsets'

    def __init__(self, offsets):
        self._offsets = offsets

    @property
    def offsets(self):
        return self._offsets

    @offsets.setter
    def offsets(self, value):
        self._offsets = value

    def content(self):
        offsets = {}
        for (k, v) in self._offsets.items():
            offsets.update({
                k: v.to_json()
            })
        return json.dumps({
            "Action": "commit",
            "Offsets": offsets
        })


class UpdateConnectorStateParams(RequestParams):
    """
    Request params of update connector state
    """

    __slots__ = '_connector_state'

    def __init__(self, connector_state):
        self._connector_state = connector_state

    @property
    def connector_state(self):
        return self._connector_state

    @connector_state.setter
    def connector_state(self, value):
        self._connector_state = value

    def content(self):
        return json.dumps({
            'Action': 'UpdateState',
            'State': self._connector_state.value
        })


class UpdateConnectorOffsetParams(RequestParams):
    """
    Request params of update connector state
    """

    __slots__ = ('_shard_id', '_connector_offset')

    def __init__(self, shard_id, connector_offset):
        self._shard_id = shard_id
        self._connector_offset = connector_offset

    @property
    def shard_id(self):
        return self._shard_id

    @shard_id.setter
    def shard_id(self, value):
        self._shard_id = value

    @property
    def connector_offset(self):
        return self._connector_offset

    @connector_offset.setter
    def connector_offset(self, value):
        self._connector_offset = value

    def content(self):
        data = {
            'Action': 'UpdateShardContext'
        }
        if self._shard_id:
            data['ShardId'] = self._shard_id
        if self._connector_offset.sequence > -1:
            data["CurrentSequence"] = self._connector_offset.sequence
        if self._connector_offset.timestamp > -1:
            data["CurrentTime"] = self._connector_offset.timestamp

        return json.dumps(data)


class UpdateConnectorShardContextParams(RequestParams):
    """
    Request params of update connector shard context
    """

    __slots__ = ('_shard_id', '_start_sequence', '_end_sequence', '_current_sequence')

    def __init__(self, shard_id, start_sequence, end_sequence, current_sequence):
        self._shard_id = shard_id
        self._start_sequence = start_sequence
        self._end_sequence = end_sequence
        self._current_sequence = current_sequence

    @property
    def shard_id(self):
        return self._shard_id

    @shard_id.setter
    def shard_id(self, value):
        self._shard_id = value

    @property
    def start_sequence(self):
        return self._start_sequence

    @start_sequence.setter
    def start_sequence(self, value):
        self._start_sequence = value

    @property
    def end_sequence(self):
        return self._end_sequence

    @end_sequence.setter
    def end_sequence(self, value):
        self._end_sequence = value

    @property
    def current_sequence(self):
        return self._current_sequence

    @current_sequence.setter
    def current_sequence(self, value):
        self._current_sequence = value

    def content(self):
        data = {
            'Action': 'UpdateShardContext',
            'ShardId': self._shard_id,
            'CurrentSequence': self._current_sequence
        }
        if self._start_sequence >= 0:
            data['StartSequence'] = self._start_sequence
        if self._end_sequence >= 0:
            data['EndSequence'] = self._end_sequence
        return json.dumps(data)


class CreateSubscriptionParams(RequestParams):
    """
    Request params of create subscription
    """

    __slots__ = '_comment'

    def __init__(self, comment):
        self._comment = comment

    @property
    def comment(self):
        return self._comment

    @comment.setter
    def comment(self, value):
        self._comment = value

    def content(self):
        return json.dumps({
            'Action': 'create',
            'Comment': self._comment
        })


class UpdateSubscriptionParams(RequestParams):
    """
    Request params of update subscription
    """

    __slots__ = '_comment'

    def __init__(self, comment):
        self._comment = comment

    @property
    def comment(self):
        return self._comment

    @comment.setter
    def comment(self, value):
        self._comment = value

    def content(self):
        return json.dumps({
            'Comment': self._comment
        })


class UpdateSubscriptionStateParams(RequestParams):
    """
    Request params of update subscription state
    """

    __slots__ = '_state'

    def __init__(self, state):
        self._state = state

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state = value

    def content(self):
        return json.dumps({
            'State': self._state.value
        })


class ListSubscriptionParams(RequestParams):
    """
    Request params of query subscription
    """

    __slots__ = ('_query_key', '_page_index', '_page_size')

    def __init__(self, query_key, page_index, page_size):
        self._page_index = page_index
        self._page_size = page_size
        self._query_key = query_key

    @property
    def page_index(self):
        return self._page_index

    @page_index.setter
    def page_index(self, value):
        self._page_index = value

    @property
    def page_size(self):
        return self._page_size

    @page_size.setter
    def page_size(self, value):
        self._page_size = value

    @property
    def query_key(self):
        return self._query_key

    @query_key.setter
    def query_key(self, value):
        self._query_key = value

    def content(self):
        data = {
            'Action': 'list',
            'PageIndex': self._page_index,
            'PageSize': self._page_size
        }
        if self._query_key:
            data['Search'] = self._query_key
        return json.dumps(data)


class ResetSubscriptionOffsetParams(RequestParams):
    """
    Request params of reset subscription offset
    """

    __slots__ = '_offsets'

    def __init__(self, offsets):
        self._offsets = offsets

    @property
    def offsets(self):
        return self._offsets

    @offsets.setter
    def offsets(self, value):
        self._offsets = value

    def content(self):
        offsets = {}
        for (k, v) in self._offsets.items():
            offsets.update({
                k: v.to_json()
            })
        return json.dumps({
            "Action": "reset",
            "Offsets": offsets
        })


class ListTopicSchemaParams(RequestParams):
    """
    Request params of list topic schema
    """
    __slots__ = '_page_number', '_page_size'

    def __init__(self, page_number, page_size):
        self._page_number = page_number
        self._page_size = page_size

    @property
    def page_number(self):
        return self._page_number

    @page_number.setter
    def page_number(self, page_number):
        self._page_number = page_number

    @property
    def page_size(self):
        return self._page_size

    @page_size.setter
    def page_size(self, page_size):
        self._page_size = page_size

    def content(self):
        return json.dumps({
            "Action": "ListSchema",
            "PageNumber": self._page_number,
            "PageSize": self._page_size
        })


class GetTopicSchemaParams(RequestParams):
    """
    Request params of get topic schema
    """
    __slots__ = '_version_id', '_record_schema'

    def __init__(self, version_id, record_schema):
        self._version_id = version_id
        self._record_schema = record_schema

    @property
    def version_id(self):
        return self._version_id

    @version_id.setter
    def version_id(self, version_id):
        self._version_id = version_id

    @property
    def record_schema(self):
        return self._record_schema

    @record_schema.setter
    def record_schema(self, record_schema):
        self._record_schema = record_schema

    def content(self):
        return json.dumps({
            "Action": "GetSchema",
            "VersionId": self._version_id,
            "RecordSchema": self._record_schema.to_json_string() if self._record_schema else ""
        })


class RegisterTopicSchemaParams(RequestParams):
    """
    Request params of register topic schema
    """
    __slots__ = '_record_schema'

    def __init__(self, record_schema):
        self._record_schema = record_schema

    @property
    def record_schema(self):
        return self._record_schema

    @record_schema.setter
    def record_schema(self, record_schema):
        self._record_schema = record_schema

    def content(self):
        return json.dumps({
            "Action": "RegisterSchema",
            "RecordSchema": self._record_schema.to_json_string() if self._record_schema else ""
        })


class DeleteTopicSchemaParams(RequestParams):
    """
    Request params of delete topic schema
    """
    __slots__ = '_version_id', '_record_schema'

    def __init__(self, version_id):
        self._version_id = version_id

    @property
    def version_id(self):
        return self._version_id

    @version_id.setter
    def version_id(self, version_id):
        self._version_id = version_id

    def content(self):
        return json.dumps({
            "Action": "DeleteSchema",
            "VersionId": self._version_id
        })
