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
    ConnectorShardStatus, ShardStatusEntry
from .record import FailedRecord, BlobRecord, TupleRecord, RecordType
from .schema import RecordSchema
from .shard import Shard, ShardBase, ShardContext
from .subscription import Subscription, OffsetWithBatchIndex
from ..batch.batch_serializer import BatchSerializer
from ..batch.utils import SchemaObject
from ..proto.datahub_record_proto_pb import GetRecordsResponse, PutRecordsResponse, GetBinaryRecordsResponse
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

    __slots__ = '_shards', '_protocol', '_interval'

    def __init__(self, shards, protocol, interval):
        self._shards = shards
        self._protocol = protocol
        self._interval = interval

    @property
    def shards(self):
        return self._shards

    @shards.setter
    def shards(self, value):
        self._shards = value

    @property
    def protocol(self):
        return self._protocol

    @protocol.setter
    def protocol(self, value):
        self._protocol = value

    @property
    def interval(self):
        return self._interval

    @interval.setter
    def interval(self, value):
        self._interval = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        shards = [Shard.from_dict(item) for item in content['Shards']]
        protocol = content['Protocol']
        interval = content['Interval']
        return cls(shards, protocol, interval)

    def to_json(self):
        return {
            'Shards': [shard.to_json() for shard in self._shards],
            'Protocol': self._protocol,
            'Interval': self._interval
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
        return cls(content['Cursor'], content['RecordTime'], content['Sequence'])

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
            'FailedRecordCount': self._failed_record_count,
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
        for item in content['Records']:
            data = item['Data']
            if isinstance(data, six.string_types):
                record = BlobRecord(values=data)
            else:
                record_schema = kwargs['record_schema']
                record = TupleRecord(schema=record_schema, values=data)
            if 'Attributes' in item:
                record.attributes = item['Attributes']
            record.sequence = item['Sequence']
            record.system_time = item['SystemTime']
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
        for pb_record in pb_get_record_response.records:
            record_schema = kwargs['record_schema']
            if record_schema:
                values = [bp_field_data.value for bp_field_data in pb_record.data.data]
                record = TupleRecord(schema=record_schema)
                record._set_values(values)
            else:
                record = BlobRecord(blob_data=pb_record.data.data[0].value)
            record._attributes = {
                attribute.key: attribute.value for attribute in pb_record.attributes.attributes
            }
            record.system_time = pb_record.system_time
            record.sequence = pb_record.sequence
            records.append(record)
        return cls(next_cursor, record_count, start_sequence, records)


class GetBatchRecordsResult(GetRecordsResult):
    """
    Batch Result of get records api
    """

    @classmethod
    def parse_content(cls, content, **kwargs):
        crc, compute_crc, pb_str = unwrap_pb_frame(content)
        if crc != compute_crc:
            raise DatahubException('Parse pb response body fail, error: crc check error. crc: %s, compute crc: %s'
                                   % (crc, compute_crc))

        project_name = kwargs['project_name']
        topic_name = kwargs['topic_name']
        init_schema = kwargs['init_schema']
        schema_register = kwargs['schema_register']

        pb_get_record_response = GetBinaryRecordsResponse()
        pb_get_record_response.ParseFromString(pb_str)
        next_cursor = pb_get_record_response.next_cursor
        record_count = 0
        start_sequence = pb_get_record_response.start_sequence
        total_records_list = []
        schema_object = SchemaObject(project_name, topic_name, schema_register)
        for i in range(pb_get_record_response.record_count):
            pb_record = pb_get_record_response.records[i]
            byte_data = pb_record.data
            records_list = BatchSerializer.deserialize(init_schema, schema_object, byte_data)
            index, records_len = 0, len(records_list)
            for record in records_list:
                record.system_time = pb_record.system_time
                record.sequence = pb_record.sequence
                record.batch_size = records_len
                record.batch_index = index
                index += 1
            total_records_list += records_list
            record_count += records_len
        return cls(next_cursor, record_count, start_sequence, total_records_list)


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

    def __init__(self, connector_names, connector_ids):
        self._connector_names = connector_names
        self._connector_ids = connector_ids

    @property
    def connector_names(self):
        return self._connector_names

    @connector_names.setter
    def connector_names(self, value):
        self._connector_names = value

    @property
    def connector_ids(self):
        return self._connector_ids

    @connector_ids.setter
    def connector_ids(self, value):
        self._connector_ids = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        return cls(content['Connectors'], content['Connectors'])

    def to_json(self):
        return {
            'Connectors': self._connector_names
        }


class CreateConnectorResult(Result):
    """
    Result of create connector

    Members:
        connector_id (:class:`str`): connector id
    """

    __slots__ = '_connector_id'

    def __init__(self, connector_id):
        self._connector_id = connector_id

    @property
    def connector_id(self):
        return self._connector_id

    @connector_id.setter
    def connector_id(self, value):
        self._connector_id = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        return cls(content.get('ConnectorId', ''))

    def to_json(self):
        return {
            'ConnectorId': self._connector_id
        }


class GetConnectorResult(Result):
    """
    Result of get data connector

    Members:
        cluster_addr (:class:`str`): cluster address

        connector_id (:class:`str`): connector id

        type (:class:`datahub.models.ConnectorType`): connector type

        state (:class:`datahub.models.ConnectorState`): connector state

        creator (:class:`str`): creator

        owner (:class:`str`): owner

        create_time (:class:`int`): create time

        column_fields (:class:`list`): list of column fields

        config (:class:`datahub.models.OdpsConnectorConfig`): config

        extra_config (:class:`dict`): extra config

        shard_contexts (:class:`list`): list of :obj:`datahub.models.ShardContext`

        sub_id (:class:`str`): subscription id used by connector
    """

    __slots__ = (
        '_cluster_addr', '_connector_id', '_type', '_state', '_creator', '_owner', '_create_time', '_column_fields',
        '_config', '_extra_config', '_shard_contexts', '_sub_id')

    def __init__(self, cluster_addr, connector_id, connector_type, state, creator, owner, create_time, column_fields,
                 config, extra_config, shard_contexts, sub_id):
        self._cluster_addr = cluster_addr
        self._connector_id = connector_id
        self._type = connector_type
        self._state = state
        self._creator = creator
        self._create_time = create_time
        self._column_fields = column_fields
        self._owner = owner
        self._config = config
        self._extra_config = extra_config
        self._shard_contexts = shard_contexts
        self._sub_id = sub_id

    @property
    def cluster_addr(self):
        return self._cluster_addr

    @cluster_addr.setter
    def cluster_addr(self, value):
        self._cluster_addr = value

    @property
    def connector_id(self):
        return self._connector_id

    @connector_id.setter
    def connector_id(self, value):
        self._connector_id = value

    @property
    def create_time(self):
        return self._create_time

    @create_time.setter
    def create_time(self, value):
        self._create_time = value

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
    def extra_config(self):
        return self._extra_config

    @extra_config.setter
    def extra_config(self, value):
        self._extra_config = value

    @property
    def shard_contexts(self):
        return self._shard_contexts

    @shard_contexts.setter
    def shard_contexts(self, value):
        self._shard_contexts = value

    @property
    def sub_id(self):
        return self._sub_id

    @sub_id.setter
    def sub_id(self, value):
        self._sub_id = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        cluster_addr = content.get('ClusterAddress', '')
        connector_id = content.get('ConnectorId', '')
        connector_type = ConnectorType(content['Type'])
        state = ConnectorState(content['State'])
        creator = content.get('Creator', '')
        owner = content.get('Owner', '')
        create_time = content.get('CreateTime', 0)
        column_fields = content.get('ColumnFields', [])
        connector_config = get_connector_builder_by_type(connector_type).from_dict(content['Config'])
        extra_config = content.get('ExtraInfo', {})
        shard_contexts = [ShardContext.from_dict(item) for item in content['ShardContexts']]  # deprecated
        sub_id = extra_config.get('SubscriptionId', '')
        return cls(cluster_addr, connector_id, connector_type, state, creator, owner, create_time, column_fields,
                   connector_config, extra_config, shard_contexts, sub_id)

    def to_json(self):
        return {
            'ClusterAddress': self._cluster_addr,
            'ConnectorId': self._connector_id,
            'Type': self._type.value,
            'State': self.state.value,
            'Creator': self._creator,
            'Owner': self._owner,
            'CreateTime': self._create_time,
            'ColumnFields': self._column_fields,
            'ConnectorConfig': self._config.to_json(),
            'ExtraInfo': self._extra_config,
        }


class GetConnectorShardStatusResult(Result):
    """
    Result of get data connector shard status

    Members:
        shard_status_infos (:class:`dict`): shard status entry map
    """

    __slots__ = '_shard_status_infos'

    def __init__(self, shard_status_infos):
        self._shard_status_infos = shard_status_infos

    @property
    def shard_status_infos(self):
        return self._shard_status_infos

    @shard_status_infos.setter
    def shard_status_infos(self, value):
        self._shard_status_infos = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        shard_status_infos = {}
        if 'ShardStatusInfos' in content:
            for (k, v) in content.get('ShardStatusInfos', {}).items():
                shard_status_infos.update({
                    k: ShardStatusEntry.from_dict(v)
                })
        else:
            shard_status_infos[content['ShardId']] = ShardStatusEntry.from_dict(content)
        return cls(shard_status_infos)

    def to_json(self):
        shard_status_infos = {}
        for (k, v) in self._shard_status_infos.items():
            shard_status_infos.update({
                k: v.to_json()
            })
        return {
            'ShardStatusInfos': shard_status_infos
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
                k: OffsetWithBatchIndex.from_dict(v)
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
                k: OffsetWithBatchIndex.from_dict(v)
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

        time_zone (:class`str`): time zone

        time_window (:class`int`): time window
    """

    __slots__ = ('_done_time', '_time_zone', '_time_window')

    def __init__(self, done_time, time_zone, time_window):
        self._done_time = done_time
        self._time_zone = time_zone
        self._time_window = time_window

    @property
    def done_time(self):
        return self._done_time

    @done_time.setter
    def done_time(self, value):
        self._done_time = value

    @property
    def time_zone(self):
        return self._time_zone

    @time_zone.setter
    def time_zone(self, value):
        self._time_zone = value

    @property
    def time_window(self):
        return self._time_window

    @time_window.setter
    def time_window(self, value):
        self._time_window = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        done_time = content.get('DoneTime', 0)
        time_zone = content.get('TimeZone', '')
        time_window = content.get('TimeWindow', 0)
        return cls(done_time, time_zone, time_window)

    def to_json(self):
        return {
            'DoneTime': self._done_time,
            'TimeZone': self._time_zone,
            'TimeWindow': self._time_window
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


class ListTopicSchemaResult(Result):
    """
    Result of list topic schema
    """

    __slots__ = '_page_number', '_page_size', '_page_count', '_total_count', '_record_schema_list'

    def __init__(self, page_number, page_size, page_count, total_count, record_schema_list):
        self._page_number = page_number
        self._page_size = page_size
        self._page_count = page_count
        self._total_count = total_count
        self._record_schema_list = record_schema_list

    @property
    def page_number(self):
        return self._page_number

    @page_number.setter
    def page_number(self, value):
        self._page_number = value

    @property
    def page_size(self):
        return self._page_size

    @page_size.setter
    def page_size(self, value):
        self._page_size = value

    @property
    def page_count(self):
        return self._page_count

    @page_count.setter
    def page_count(self, value):
        self._page_count = value

    @property
    def total_count(self):
        return self._total_count

    @total_count.setter
    def total_count(self, value):
        self._total_count = value

    @property
    def record_schema_list(self):
        return self._record_schema_list

    @record_schema_list.setter
    def record_schema_list(self, value):
        self._record_schema_list = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        page_number = content.get("PageNumber", 0)
        page_size = content.get("PageSize", 0)
        page_count = content.get("PageCount", 0)
        total_count = content.get("TotalCount", 0)
        record_schema_list = content.get("RecordSchemaList", [])
        return cls(page_number, page_size, page_count, total_count, record_schema_list)

    def to_json(self):
        return {
            "PageNumber": self._page_number,
            "PageSize": self._page_size,
            "PageCount": self._page_count,
            "TotalCount": self._total_count,
            "RecordSchemaList": self._record_schema_list
        }


class GetTopicSchemaResult(Result):
    """
    Result of get topic schema
    """

    __slots__ = '_version_id', '_create_time', '_creator', '_record_schema'

    def __init__(self, version_id, create_time, creator, record_schema):
        self._version_id = version_id
        self._create_time = create_time
        self._creator = creator
        self._record_schema = record_schema

    @property
    def version_id(self):
        return self._version_id

    @version_id.setter
    def version_id(self, value):
        self._version_id = value

    @property
    def create_time(self):
        return self._create_time

    @create_time.setter
    def create_time(self, value):
        self._create_time = value

    @property
    def creator(self):
        return self._creator

    @creator.setter
    def creator(self, value):
        self._creator = value

    @property
    def record_schema(self):
        return self._record_schema

    @record_schema.setter
    def record_schema(self, value):
        self._record_schema = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        version_id = content.get("VersionId", 0)
        create_time = content.get("CreateTime", 0)
        creator = content.get("Creator", 0)
        record_schema = content.get("RecordSchema", 0)
        return cls(version_id, create_time, creator, record_schema)

    def to_json(self):
        return {
            "VersionId": self._version_id,
            "CreateTime": self._create_time,
            "Creator": self._creator,
            "RecordSchema": self._record_schema
        }


class RegisterTopicSchemaResult(Result):
    """
    Result of register topic schema
    """

    __slots__ = '_version_id'

    def __init__(self, version_id):
        self._version_id = version_id

    @property
    def version_id(self):
        return self._version_id

    @version_id.setter
    def version_id(self, value):
        self._version_id = value

    @classmethod
    def parse_content(cls, content, **kwargs):
        content = json.loads(to_text(content))
        version_id = content.get("VersionId", 0)
        return cls(version_id)

    def to_json(self):
        return {
            "VersionId": self._version_id,
        }


class DeleteTopicSchemaResult(Result):
    """
    Result of delete topic schema
    """

    def __init__(self):
        super(DeleteTopicSchemaResult, self).__init__()

    @classmethod
    def parse_content(cls, content, **kwargs):
        return cls()

    def to_json(self):
        return {}
