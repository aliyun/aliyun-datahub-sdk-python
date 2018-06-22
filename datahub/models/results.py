#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# 'License'); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import

import abc
import json

import six

from datahub.exceptions import DatahubException
from .connector import ConnectorType, ConnectorState, get_connector_builder_by_type, \
    ConnectorShardStatus
from .record import FailedRecord, BlobRecord, TupleRecord, RecordType
from .schema import RecordSchema
from .shard import Shard, ShardBase, ShardContext
from .subscription import OffsetWithSession, OffsetWithVersion, Subscription
from ..proto.datahub_record_proto_pb import GetRecordsResponse, PutRecordsResponse
from ..utils import to_text, unwrap_pb_frame


@six.add_metaclass(abc.ABCMeta)
class Result(object):
    """
    Abstract class to be implement
    """

    @classmethod
    @abc.abstractmethod
    def parse_content(cls, content, **kwargs):
        pass

    @abc.abstractmethod
    def to_json(self):
        pass

    def __repr__(self):
        return to_text(self.to_json())


class ListProjectResult(Result):
    """
    Request params of list projects api

    Members:
        project_names (:class:`list`): list of project names
    """

    __slots__ = '_project_names'

    def __init__(self, project_names):
        self._project_names = project_names

    @property
    def project_names(self):
        return self._project_names

    @project_names.setter
    def project_names(self, value):
        self._project_names = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        return cls(content['ProjectNames'])

    def to_json(self):
        return {
            'ProjectNames': self._project_names
        }


class GetProjectResult(Result):
    """
    Result of get project api

    Members:
        project_name (:class:`str`): project name

        comment (:class:`str`): project description

        create_time (:class:`int`): create time

        last_modify_time(:class:`int`): last modify time
    """

    __slots__ = ('_project_name', '_comment', '_create_time', '_last_modify_time')

    def __init__(self, project_name, comment, create_time, last_modify_time):
        self._project_name = project_name
        self._comment = comment
        self._create_time = create_time
        self._last_modify_time = last_modify_time

    @property
    def project_name(self):
        return self._project_name

    @project_name.setter
    def project_name(self, value):
        self._project_name = value

    @property
    def comment(self):
        return self._comment

    @comment.setter
    def comment(self, value):
        self._comment = value

    @property
    def create_time(self):
        return self._create_time

    @create_time.setter
    def create_time(self, value):
        self._create_time = value

    @property
    def last_modify_time(self):
        return self._last_modify_time

    @last_modify_time.setter
    def last_modify_time(self, value):
        self._last_modify_time = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        return cls(kwargs['project_name'], content['Comment'], content['CreateTime'], content['LastModifyTime'])

    def to_json(self):
        return {
            'ProjectName': self._project_name,
            'Comment': self._comment,
            'CreateTime': self._create_time,
            'LastModifyTime': self.last_modify_time
        }


class ListTopicResult(Result):
    """
    Result of list topics api

    Members:
        topic_names (:class:`list`): list of topic names
    """

    __slots__ = '_topic_names'

    def __init__(self, topic_names):
        self._topic_names = topic_names

    @property
    def topic_names(self):
        return self._topic_names

    @topic_names.setter
    def topic_names(self, value):
        self._topic_names = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        return cls(content['TopicNames'])

    def to_json(self):
        return {
            'TopicNames': self._topic_names
        }


class GetTopicResult(Result):
    """
    Result of get topic api

    Members:
        project_name (:class:`str`): project name

        topic_name (:class:`str`): topic name

        shard_count (:class:`int`) shard count

        life_cycle (:class:`int`) life cycle

        record_type (:class:`datahub.models.RecordType`): record type

        record_schema (:class:`datahub.models.RecordSchema`): record schema

        comment (:class:`str`): project description

        create_time (:class:`int`): create time

        last_modify_time(:class:`int`): last modify time
    """

    __slots__ = ('_project_name', '_topic_name', '_shard_count', '_life_cycle', '_record_type', '_record_schema',
                 '_comment', '_create_time', '_last_modify_time')

    def __init__(self, **kwargs):
        self._project_name = kwargs['project_name'] if 'project_name' in kwargs else ''
        self._topic_name = kwargs['topic_name'] if 'topic_name' in kwargs else ''
        self._shard_count = kwargs['shard_count'] if 'shard_count' in kwargs else 0
        self._life_cycle = kwargs['life_cycle'] if 'life_cycle' in kwargs else 0
        self._record_type = RecordType(kwargs['record_type']) if 'record_type' in kwargs else ''
        self._record_schema = kwargs['record_schema'] if 'record_schema' in kwargs else None
        self._comment = kwargs['comment'] if 'comment' in kwargs else ''
        self._create_time = kwargs['create_time'] if 'create_time' in kwargs else 0
        self._last_modify_time = kwargs['last_modify_time'] if 'last_modify_time' in kwargs else 0

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
    def comment(self):
        return self._comment

    @comment.setter
    def comment(self, value):
        self._comment = value

    @property
    def record_schema(self):
        return self._record_schema

    @record_schema.setter
    def record_schema(self, value):
        self._record_schema = value

    @property
    def create_time(self):
        return self._create_time

    @create_time.setter
    def create_time(self, value):
        self._create_time = value

    @property
    def last_modify_time(self):
        return self._last_modify_time

    @last_modify_time.setter
    def last_modify_time(self, value):
        self._last_modify_time = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        record_schema = None
        record_type = RecordType(content['RecordType'])
        if record_type == RecordType.TUPLE:
            record_schema = RecordSchema.from_json_str(content['RecordSchema'])
        return cls(project_name=kwargs['project_name'], topic_name=kwargs['topic_name'],
                   comment=content['Comment'], create_time=content['CreateTime'],
                   last_modify_time=content['LastModifyTime'], life_cycle=content['Lifecycle'],
                   record_type=record_type, record_schema=record_schema, shard_count=content['ShardCount'])

    def to_json(self):
        data = {
            'ProjectName': self._project_name,
            'TopicName': self._topic_name,
            'Comment': self._comment,
            'CreateTime': self._create_time,
            'LastModifyTime': self._last_modify_time,
            'Lifecycle': self._life_cycle,
            'RecordType': self._record_type.value,
            'ShardCount': self._shard_count
        }
        if self._record_type == RecordType.TUPLE:
            data['RecordSchema'] = self._record_schema.to_json()
        return data


class ListShardResult(Result):
    """
    Result of list shards api

    Members:
        shards (:class:`list`): list of :obj:`datahub.models.Shard`
    """

    __slots__ = '_shards'

    def __init__(self, shards):
        self._shards = shards

    @property
    def shards(self):
        return self._shards

    @shards.setter
    def shards(self, value):
        self._shards = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        shards = [Shard.from_dict(item) for item in content['Shards']]
        return cls(shards)

    def to_json(self):
        return {
            'Shards': [shard.to_json() for shard in self._shards]
        }


class MergeShardResult(ShardBase, Result):
    """
    Result of merge shard api

    Members:
        shard_id (:class:`str`): shard id

        begin_hash_key (:class:`str`): begin hash key

        end_hash_key (:class:`str`): end hash key
    """

    __slots__ = ('_shard_id', '_begin_hash_key', '_end_hash_key')

    def __init__(self, shard_id, begin_hash_key, end_hash_key):
        super(MergeShardResult, self).__init__(shard_id, begin_hash_key, end_hash_key)

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        return cls(content['ShardId'], content['BeginHashKey'], content['EndHashKey'])

    def to_json(self):
        return super(MergeShardResult, self).to_json()


class SplitShardResult(Result):
    """
    Result of split shard api

    Members:
        new_shards (:class:`list`): list of :obj:`datahub.models.ShardBase`
    """

    __slots__ = '_new_shards'

    def __init__(self, new_shards):
        self._new_shards = new_shards

    @property
    def new_shards(self):
        return self._new_shards

    @new_shards.setter
    def new_shards(self, value):
        self._new_shards = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        new_shards = [ShardBase.from_dict(item) for item in content['NewShards']]
        return cls(new_shards)

    def to_json(self):
        return {
            'NewShards': [shard.to_json() for shard in self._new_shards]
        }


class GetCursorResult(Result):
    """
    Request params of get cursor api

    Members:
        cursor (:class:`str`): cursor

        record_time (:class:`int`): record time

        sequence (:class:`int`): sequence
    """

    __slots__ = ('_cursor', '_record_time', '_sequence')

    def __init__(self, cursor, record_time, sequence):
        self._cursor = cursor
        self._record_time = record_time
        self._sequence = sequence

    @property
    def cursor(self):
        return self._cursor

    @cursor.setter
    def cursor(self, value):
        self._cursor = value

    @property
    def record_time(self):
        return self._record_time

    @record_time.setter
    def record_time(self, value):
        self._record_time = value

    @property
    def sequence(self):
        return self._sequence

    @sequence.setter
    def sequence(self, value):
        self._sequence = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        return cls(content['Cursor'], content['Sequence'], content['RecordTime'])

    def to_json(self):
        return {
            'Cursor': self._cursor,
            'Sequence': self._sequence,
            'RecordTime': self._record_time
        }


class PutRecordsResult(Result):
    """
    Result of put records api

    Members:
        failed_record_count (:class:`int`): failed record count

        failed_records (:class:`list`): list of :obj:`datahub.models.FailedRecord`
    """

    __slots__ = ('_failed_record_count', '_failed_records')

    def __init__(self, failed_record_count, failed_records):
        self._failed_record_count = failed_record_count
        self._failed_records = failed_records

    @property
    def failed_record_count(self):
        return self._failed_record_count

    @failed_record_count.setter
    def failed_record_count(self, value):
        self._failed_record_count = value

    @property
    def failed_records(self):
        return self._failed_records

    @failed_records.setter
    def failed_records(self, value):
        self._failed_records = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        failed_records = [
            FailedRecord(item['Index'], item['ErrorCode'], item['ErrorMessage'])
            for item in content['FailedRecords']
        ]
        return cls(content['FailedRecordCount'], failed_records)

    def to_json(self):
        return {
            'FailedRecordCount': self.failed_record_count,
            'FailedRecords': [failed_record.to_json() for failed_record in self._failed_records]
        }


class PutPBRecordsResult(PutRecordsResult):
    """
    Protobuf Result of put records api
    """

    @classmethod
    def parse_content(cls, content, **kwargs):
        crc, compute_crc, pb_str = unwrap_pb_frame(content)
        if crc != compute_crc:
            raise DatahubException('Parse pb response body fail, error: crc check error. crc: %s, compute crc: %s'
                                   % (crc, compute_crc))
        pb_put_record_response = PutRecordsResponse()
        pb_put_record_response.ParseFromString(pb_str)
        pb_failed_records = pb_put_record_response.failed_records
        failed_records = [FailedRecord.from_pb_message(pb_failed_record) for pb_failed_record in pb_failed_records]
        return cls(pb_put_record_response.failed_count, failed_records)


class GetRecordsResult(Result):
    """
    Result of get records api

    Members:
        next_cursor (:class:`str`): next cursor

        record_count (:class:`int`): record count

        start_squ (:class:`int`): start sequence

        records (:class:`list`): list of :obj:`datahub.models.BlobRecord`/:obj:`datahub.models.TupleRecord`
    """

    __slots__ = ('_next_cursor', '_record_count', '_start_seq', '_records')

    def __init__(self, next_cursor, record_count, start_seq, records):
        self._next_cursor = next_cursor
        self._record_count = record_count
        self._start_seq = start_seq
        self._records = records

    @property
    def next_cursor(self):
        return self._next_cursor

    @next_cursor.setter
    def next_cursor(self, value):
        self._next_cursor = value

    @property
    def record_count(self):
        return self._record_count

    @record_count.setter
    def record_count(self, value):
        self._record_count = value

    @property
    def start_seq(self):
        return self._start_seq

    @start_seq.setter
    def start_seq(self, value):
        self._start_seq = value

    @property
    def records(self):
        return self._records

    @records.setter
    def records(self, value):
        self._records = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))

        records = []
        sequence = content['StartSeq']
        for item in content['Records']:
            data = item['Data']
            if isinstance(data, six.string_types):
                record = BlobRecord(values=data)
            else:
                record_schema = kwargs['record_schema']
                record = TupleRecord(schema=record_schema, values=data)
            if 'Attributes' in item:
                record.attributes = item['Attributes']
            record.sequence = sequence
            record.system_time = item['SystemTime']
            sequence += 1
            records.append(record)
        return cls(content['NextCursor'], content['RecordCount'], content['StartSeq'], records)

    def to_json(self):
        return {
            'NextCursor': self._next_cursor,
            'RecordCount': self._record_count,
            'StartSeq': self._start_seq,
            'Records': [record.to_json() for record in self._records]
        }


class GetPBRecordsResult(GetRecordsResult):
    """
    Protobuf Result of get records api
    """

    @classmethod
    def parse_content(cls, content, **kwargs):
        crc, compute_crc, pb_str = unwrap_pb_frame(content)
        if crc != compute_crc:
            raise DatahubException('Parse pb response body fail, error: crc check error. crc: %s, compute crc: %s'
                                   % (crc, compute_crc))

        pb_get_record_response = GetRecordsResponse()
        pb_get_record_response.ParseFromString(pb_str)
        next_cursor = pb_get_record_response.next_cursor
        record_count = pb_get_record_response.record_count
        start_sequence = pb_get_record_response.start_sequence
        records = []
        sequence = start_sequence
        for pb_record in pb_get_record_response.records:
            record_schema = kwargs['record_schema']
            if record_schema:
                values = [bp_field_data.value for bp_field_data in pb_record.data.data]
                record = TupleRecord(schema=record_schema, values=values)
            else:
                record = BlobRecord(blob_data=pb_record.data.data[0].value)
            record._attributes = {
                attribute.key: attribute.value for attribute in pb_record.attributes.attributes
            }
            record.system_time = pb_record.system_time
            record.sequence = sequence
            sequence += 1
            records.append(record)
        return cls(next_cursor, record_count, start_sequence, records)

        # st = time.time()
        # pb_get_record_response = GetRecordsResponse()
        # pb_get_record_response.ParseFromString(content[12:])
        # Result.parse += time.time() - st
        # next_cursor = pb_get_record_response.next_cursor
        # record_count = pb_get_record_response.record_count
        # start_sequence = pb_get_record_response.start_sequence
        # records = []
        # sequence = start_sequence
        # for pb_record in pb_get_record_response.records:
        #     record_schema = kwargs['record_schema']
        #     if record_schema:
        #         values = [bp_field_data.value for bp_field_data in pb_record.data.data]
        #         record = TupleRecord(schema=record_schema, values=values)
        #     else:
        #         record = BlobRecord(blob_data=pb_record.data.data[0].value)
        #     record._attributes = pb_record.attributes
        #     record.system_time = pb_record.system_time
        #     record.sequence = sequence
        #     sequence += 1
        #     records.append(record)
        # Result.decode += time.time() - st
        # return cls(next_cursor, record_count, start_sequence, records)


class GetMeteringInfoResult(Result):
    """
    Result of get metering info api;

    Members:
        active_time (:class:`int`): active time

        storage (:class:`int`): storage
    """

    __slots__ = ('_active_time', '_storage')

    def __init__(self, active_time, storage):
        self._active_time = active_time
        self._storage = storage

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

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        return cls(content['ActiveTime'], content['Storage'])

    def to_json(self):
        return {
            'ActiveTime': self._active_time,
            'Storage': self._storage
        }


class ListConnectorResult(Result):
    """
    Result of list data connector

    Members:
        connector_names (:class:`list`): list of data connector names
    """

    __slots__ = '_connector_names'

    def __init__(self, connector_names):
        self._connector_names = connector_names

    @property
    def connector_names(self):
        return self._connector_names

    @connector_names.setter
    def connector_names(self, value):
        self._connector_names = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        return cls(content['Connectors'])

    def to_json(self):
        return {
            'Connectors': self._connector_names
        }


class GetConnectorResult(Result):
    """
    Result of get data connector

    Members:
        column_fields (:class:`list`): list of column fields

        type (:class:`datahub.models.ConnectorType`): connector type

        state (:class:`datahub.models.ConnectorState`): connector state

        creator (:class:`str`): creator

        owner (:class:`str`): owner

        config (:class:`datahub.models.OdpsConnectorConfig`): config

        shard_contexts (:class:`list`): list of :obj:`datahub.models.ShardContext`
    """

    __slots__ = ('_column_fields', '_type', '_state', '_creator', '_owner', '_config', '_shard_contexts')

    def __init__(self, column_fields, connector_type, state, creator, owner, config, shard_contexts):
        self._column_fields = column_fields
        self._type = connector_type
        self._state = state
        self._creator = creator
        self._owner = owner
        self._config = config
        self._shard_contexts = shard_contexts

    @property
    def column_fields(self):
        return self._column_fields

    @column_fields.setter
    def column_fields(self, value):
        self._column_fields = value

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        self._type = value

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state = value

    @property
    def creator(self):
        return self._creator

    @creator.setter
    def creator(self, value):
        self._creator = value

    @property
    def owner(self):
        return self._owner

    @owner.setter
    def owner(self, value):
        self._owner = value

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, value):
        self._config = value

    @property
    def shard_contexts(self):
        return self._shard_contexts

    @shard_contexts.setter
    def shard_contexts(self, value):
        self._shard_contexts = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        connector_type = ConnectorType(content['Type'])
        builder = get_connector_builder_by_type(connector_type)
        connector_config = builder.from_dict(content['Config'])
        shard_contexts = [ShardContext.from_dict(item) for item in content['ShardContexts']]
        return cls(content['ColumnFields'], connector_type, ConnectorState(content['State']),
                   content['Creator'], content['Owner'], connector_config, shard_contexts)

    def to_json(self):
        return {
            'ColumnFields': self._column_fields,
            'Type': self._type.value,
            'State': self.state.value,
            'Creator': self._creator,
            'Owner': self._owner,
            'ConnectorConfig': self._config.to_json(),
            'ShardContexts': [shard_context.to_json() for shard_context in self._shard_contexts]
        }


class GetConnectorShardStatusResult(Result):
    """
    Result of get data connector shard status

    Members:
        start_sequence (:class:`int`): start sequence

        end_sequence (:class:`int`): end sequence

        current_sequence (:class:`int`): current sequence

        last_error_message (:class:`str`): last error message

        state (:class:`datahub.models.connector.ConnectorShardStatus`): state

        update_time (:class:`int`): update time

        record_time (:class:`int`): record time

        discard_count (:class:`int`): discard count
    """

    __slots__ = ('_start_sequence', '_end_sequence', '_current_sequence', '_last_error_message',
                 '_state', '_update_time', '_record_time', '_discard_count')

    def __init__(self, start_sequence, end_sequence, current_sequence, last_error_message, state,
                 update_time, record_time, discard_count):
        self._start_sequence = start_sequence
        self._end_sequence = end_sequence
        self._current_sequence = current_sequence
        self._last_error_message = last_error_message
        self._state = state
        self._update_time = update_time
        self._record_time = record_time
        self._discard_count = discard_count

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

    @property
    def last_error_message(self):
        return self._last_error_message

    @last_error_message.setter
    def last_error_message(self, value):
        self._last_error_message = value

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state = value

    @property
    def update_time(self):
        return self._update_time

    @update_time.setter
    def update_time(self, value):
        self._update_time = value

    @property
    def record_time(self):
        return self._record_time

    @record_time.setter
    def record_time(self, value):
        self._record_time = value

    @property
    def discard_count(self):
        return self._discard_count

    @discard_count.setter
    def discard_count(self, value):
        self._discard_count = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        return cls(content['StartSequence'], content['EndSequence'], content['CurrentSequence'],
                   content['LastErrorMessage'], ConnectorShardStatus(content['State']), content['UpdateTime'],
                   content['RecordTime'], content['DiscardCount'])

    def to_json(self):
        return {
            'StartSequence': self._start_sequence,
            'EndSequence': self._end_sequence,
            'CurrentSequence': self._current_sequence,
            'LastErrorMessage': self._last_error_message,
            'State': self._state.value,
            'UpdateTime': self._update_time,
            'RecordTime': self._record_time
        }


class InitAndGetSubscriptionOffsetResult(Result):
    """
    Result of init and get subscription offset api

    Members:
        offsets (:class:`list`): list of :obj:`datahub.models.OffsetWithSession`
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

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        offsets = {}
        for (k, v) in content['Offsets'].items():
            offsets.update({
                k: OffsetWithSession.from_dict(v)
            })
        return cls(offsets)

    def to_json(self):
        offsets = {}
        for (k, v) in self._offsets.items():
            offsets.update({
                k: v.to_json()
            })
        return {
            'Offsets': offsets
        }


class GetSubscriptionOffsetResult(Result):
    """
    Result of get subscription offset api

    Members:
        offsets (:class:`list`): list of :obj:`datahub.models.OffsetWithVersion`
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

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        offsets = {}
        for (k, v) in content['Offsets'].items():
            offsets.update({
                k: OffsetWithVersion.from_dict(v)
            })
        return cls(offsets)

    def to_json(self):
        offsets = {}
        for (k, v) in self._offsets.items():
            offsets.update({
                k: v.to_json()
            })
        return {
            'Offsets': offsets
        }


class GetConnectorDoneTimeResult(Result):
    """
    Result of get connector done time api

    Members:
        done_time (:class`int`): done time
    """

    __slots__ = '_done_time'

    def __init__(self, done_time):
        self._done_time = done_time

    @property
    def done_time(self):
        return self._done_time

    @done_time.setter
    def done_time(self, value):
        self._done_time = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        return cls(content['DoneTime'])

    def to_json(self):
        return {
            'DoneTime': self._done_time
        }


class CreateSubscriptionResult(Result):
    """
    Result of create subscription api

    Members:
        sub_id (:class:`str`): subscription id
    """

    __slots__ = '_sub_id'

    def __init__(self, sub_id):
        self._sub_id = sub_id

    @property
    def sub_id(self):
        return self._sub_id

    @sub_id.setter
    def sub_id(self, value):
        self._sub_id = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        return cls(content['SubId'])

    def to_json(self):
        return {
            'SubId': self._sub_id
        }


class GetSubscriptionResult(Subscription, Result):
    """
    Result of get subscription api

    Members:
        comment (:class:`str`): comment

        create_time (:class:`int`): create time

        is_owner (:class:`bool`): owner or not

        last_modify_time (:class:`int`): last modify time

        state (:class:`str`): state

        update_time (:class:`int`): update time

        record_time (:class:`int`): record time

        discard_count (:class:`int`): discard count
    """

    __slots__ = ('_comment', '_create_time', '_is_owner', '_last_modify_time',
                 '_state', '_sub_id', '_topic_name', '_type')

    def __init__(self, comment, create_time, is_owner, last_modify_time,
                 state, sub_id, topic_name, sub_type):
        super(GetSubscriptionResult, self).__init__(comment, create_time, is_owner, last_modify_time,
                                                    state, sub_id, topic_name, sub_type)

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        return GetSubscriptionResult.from_dict(content)

    def to_json(self):
        return super(GetSubscriptionResult, self).to_json()


class ListSubscriptionResult(Result):
    """
    Result of query subscription api
    """

    __slots__ = ('_total_count', '_subscriptions')

    def __init__(self, total_count, subscriptions):
        self._total_count = total_count
        self._subscriptions = subscriptions

    @property
    def subscriptions(self):
        return self._subscriptions

    @subscriptions.setter
    def subscriptions(self, value):
        self._subscriptions = value

    @property
    def total_count(self):
        return self._total_count

    @total_count.setter
    def total_count(self, value):
        self._total_count = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        subscriptions = [Subscription.from_dict(item) for item in content['Subscriptions']]
        return cls(content['TotalCount'], subscriptions)

    def to_json(self):
        return {
            'TotalCount': self._total_count,
            'Subscriptions': [subscription.to_json() for subscription in self._subscriptions]
        }
