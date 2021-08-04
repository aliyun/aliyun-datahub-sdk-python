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

from .utils import type_assert
from .implement import DataHubJson, DataHubPB
from .models import CompressFormat, RecordSchema, FieldType, CursorType, ConnectorType, ConnectorConfig,\
    ConnectorState, ConnectorOffset, SubscriptionState


class DataHub(object):
    """
    Main entrance to DataHub.

    Convenient operations on DataHub objects are provided.
    Please refer to `DataHub docs <https://datahub.console.aliyun.com/intro/index.html>`_
    to see the details.

    Generally, basic operations such as ``create``, ``list``, ``delete``, ``update`` are provided for each DataHub object.
    Take the ``project`` as an example.

    To create an DataHub instance, access_id and access_key is required, and should ensure correctness,
    or ``SignatureNotMatch`` error will throw.

    :param access_id: Aliyun Access ID
    :param secret_access_key: Aliyun Access Key
    :param endpoint: Rest service URL
    :param enable_pb: enable protobuf when put/get records, default value is False in version <= 2.11, default value will be True in version >= 2.12
    :param compress_format: compress format
    :type compress_format: :class:`datahub.models.compress.CompressFormat`

    :Example:

    >>> datahub = DataHub('**your access id**', '**your access key**', '**endpoint**')
    >>> datahub_pb = DataHub('**your access id**', '**your access key**', '**endpoint**', enable_pb=True)
    >>> datahub_lz4 = DataHub('**your access id**', '**your access key**', '**endpoint**', compress_format=CompressFormat.LZ4)
    >>>
    >>> project_result = datahub.get_project('datahub_test')
    >>>
    >>> print(project_result is None)
    >>>
    """

    def __init__(self, access_id, access_key, endpoint=None, enable_pb=True,
                 compress_format=CompressFormat.NONE, **kwargs):
        if enable_pb:
            self._datahub_impl = DataHubPB(access_id, access_key, endpoint, compress_format, **kwargs)
        else:
            self._datahub_impl = DataHubJson(access_id, access_key, endpoint, compress_format, **kwargs)

    def list_project(self):
        """
        List all project names

        :return: projects in datahub server
        :rtype: :class:`datahub.models.results.ListProjectResult`
        """
        return self._datahub_impl.list_project()

    @type_assert(object, str, str)
    def create_project(self, project_name, comment):
        """
        Create a new project by given name and comment

        :param project_name: project name
        :param comment: description of project
        :return: none
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name is not valid
        :raise: :class:`datahub.exceptions.ResourceExistException` if project is already existed
        """
        self._datahub_impl.create_project(project_name, comment)

    @type_assert(object, str)
    def get_project(self, project_name):
        """
        Get a project by given name

        :param project_name: project name
        :return: the right project
        :rtype: :class:`datahub.models.GetProjectResult`
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if project not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name is empty

        .. see also:: :class:`datahub.models.Project`
        """
        return self._datahub_impl.get_project(project_name)

    @type_assert(object, str, str)
    def update_project(self, project_name, comment):
        """
        Update project comment

        :param project_name: project name
        :param comment: new description of project
        :return: none
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if project not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name is empty or comment is invalid
        """
        return self._datahub_impl.update_project(project_name, comment)

    @type_assert(object, str)
    def delete_project(self, project_name):
        """
        Delete the project by given name

        :param project_name: project name
        :return: none
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name is empty
        """
        self._datahub_impl.delete_project(project_name)

    @type_assert(object, str)
    def list_topic(self, project_name):
        """
        Get all topics of a project

        :param project_name: project name
        :return: all topics of the project
        :rtype: :class:`datahub.models.ListTopicResult`
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name is empty
        """
        return self._datahub_impl.list_topic(project_name)

    @type_assert(object, str, str, int, int, str, bool)
    def create_blob_topic(self, project_name, topic_name, shard_count, life_cycle, comment, extend_mode=None):
        """
        Create blob topic

        :param project_name: project name
        :param topic_name: topic name
        :param shard_count: shard count
        :param life_cycle: life cycle
        :param comment: comment
        :param extend_mode: use expansion method to increase shard
        :return: none
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name is empty; topic_name is not valid; life_cycle is not positive; record_schema is wrong type
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if project not existed
        :raise: :class:`datahub.exceptions.ResourceExistException` if topic is already existed
        """
        self._datahub_impl.create_blob_topic(project_name, topic_name, shard_count, life_cycle, extend_mode, comment)

    @type_assert(object, str, str, int, int, RecordSchema, str, bool)
    def create_tuple_topic(self, project_name, topic_name, shard_count, life_cycle, record_schema, comment, extend_mode=None):
        """
        Create tuple topic

        :param project_name: project name
        :param topic_name: topic name
        :param shard_count: shard count
        :param life_cycle: life cycle
        :param record_schema: record schema for tuple record type
        :type record_schema: :class:`datahub.models.RecordSchema`
        :param comment: comment
        :param extend_mode: use expansion method to increase shard
        :return: none
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name is empty; topic_name is not valid; life_cycle is not positive; record_schema is wrong type
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if project not existed
        :raise: :class:`datahub.exceptions.ResourceExistException` if topic is already existed
        """
        self._datahub_impl.create_tuple_topic(project_name, topic_name, shard_count, life_cycle, record_schema, extend_mode, comment)

    @type_assert(object, str, str)
    def get_topic(self, project_name, topic_name):
        """
        Get a topic

        :param topic_name: topic name
        :param project_name: project name
        :return: topic info
        :rtype: :class:`datahub.models.GetTopicResult`
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project or topic not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if the project name or topic name is empty
        """
        return self._datahub_impl.get_topic(project_name, topic_name)

    @type_assert(object, str, str, int, str)
    def update_topic(self, project_name, topic_name, life_cycle, comment):
        """
        Update topic info, only life cycle and comment can be modified.

        :param topic_name: topic name
        :param project_name: project name
        :param life_cycle: life cycle of topic
        :param comment: topic comment
        :return: none
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project or topic not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if the project name or topic name is empty; life_cycle is not positive
        """
        self._datahub_impl.update_topic(project_name, topic_name, life_cycle, comment)

    @type_assert(object, str, str)
    def delete_topic(self, project_name, topic_name):
        """
        Delete a topic

        :param topic_name: topic name
        :param project_name: project name
        :return: none
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project or topic not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if the project name or topic name is empty
        """
        self._datahub_impl.delete_topic(project_name, topic_name)

    @type_assert(object, str, str, str, FieldType)
    def append_field(self, project_name, topic_name, field_name, field_type):
        """
        Append field to a tuple topic

        :param project_name: project name
        :param topic_name: topic name
        :param field_name: field name
        :param field_type: field type
        :type field_type: :class:`datahub.models.FieldType`
        :return: none
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name, topic_name or field_name is empty; field_type is wrong type
        """
        self._datahub_impl.append_field(project_name, topic_name, field_name, field_type)

    @type_assert(object, str, str, int)
    def wait_shards_ready(self, project_name, topic_name, timeout=30):
        """
        Wait all shard state in ``active`` or ``closed``.
        It always be invoked when create a topic, and will be blocked and until all
        shards state in ``active`` or ``closed`` or timeout .

        :param project_name: project name
        :param topic_name: topic name
        :param timeout: -1 means it will be blocked until all shards state in ``active`` or ``closed``, else will be wait timeout seconds
        :return: none
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project or topic not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if the project name or topic name is empty; timeout < 0
        :raise: :class:`datahub.exceptions.DatahubException` if timeout
        """
        self._datahub_impl.wait_shards_ready(project_name, topic_name, timeout)

    @type_assert(object, str, str)
    def list_shard(self, project_name, topic_name):
        """
        List all shards of a topic

        :param project_name: project name
        :param topic_name: topic name
        :return: shards info
        :rtype: :class:`datahub.models.ListTopicResult`
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project or topic not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if the project name or topic name is empty
        """
        return self._datahub_impl.list_shard(project_name, topic_name)

    @type_assert(object, str, str, str, str)
    def merge_shard(self, project_name, topic_name, shard_id, adj_shard_id):
        """
        Merge shards

        :param project_name: project name
        :param topic_name: topic name
        :param shard_id: shard id
        :param adj_shard_id: adjacent shard id
        :return: shards info after merged
        :rtype: :class:`datahub.models.MergeShardResult`
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project, topic or shard not exists
        :raise: :class:`datahub.exceptions.InvalidOperationException` if the shard is not active
        :raise: :class:`datahub.exceptions.InvalidParameterException` if the shards not adjacent; project name, topic name, shard id or adjacent shard id is empty
        :raise: :class:`datahub.exceptions.LimitExceededException` if merge shard operation limit exceeded
        """
        return self._datahub_impl.merge_shard(project_name, topic_name, shard_id, adj_shard_id)

    @type_assert(object, str, str, str, str)
    def split_shard(self, project_name, topic_name, shard_id, split_key=''):
        """
        Split shard

        :param project_name: project name
        :param topic_name: topic name
        :param shard_id: split shard id
        :param split_key: split key, if not given, choose the median
        :return: shards info after split
        :rtype: :class:`datahub.models.SplitShardResult`
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project, topic or shard not exists
        :raise: :class:`datahub.exceptions.InvalidOperationException` if the shard is not active
        :raise: :class:`datahub.exceptions.InvalidParameterException` if the key range is invalid; project name, topic name or shard id is empty
        :raise: :class:`datahub.exceptions.LimitExceededException` if split shard operation limit exceeded
        """
        return self._datahub_impl.split_shard(project_name, topic_name, shard_id, split_key)

    @type_assert(object, str, str, str, CursorType, int)
    def get_cursor(self, project_name, topic_name, shard_id, cursor_type, param=-1):
        """
        Get cursor.
        When you invoke get_blob_records/get_tuple_records, you must be invoke it to get a cursor first

        :param project_name: project name
        :param topic_name: topic name
        :param shard_id: shard id
        :param cursor_type: cursor type
        :type cursor_type: :class:`datahub.models.CursorType`
        :param param: param is system time if cursor_type == CursorType.SYSTEM_TIME while sequence if cursor_type==CursorType.SEQUENCE
        :return: cursor info
        :rtype: :class:`datahub.models.GetCursorResult`
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project, topic or shard not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if the param is invalid; project_name, topic_name or shard_id is empty; cursor_type is wrong type; param is missing
        """
        return self._datahub_impl.get_cursor(project_name, topic_name, shard_id, cursor_type, param)

    @type_assert(object, str, str, list)
    def put_records(self, project_name, topic_name, record_list):
        """
        Put records to a topic

        :param project_name: project name
        :param topic_name: topic name
        :param record_list: record list
        :type record_list: :class:`list`
        :return: failed records info
        :rtype: :class:`datahub.models.PutRecordsResult`
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project or topic not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if the record is not well-formed; project_name or topic_name is empty
        :raise: :class:`datahub.exceptions.InvalidOperationException` if the shard is not active
        :raise: :class:`datahub.exceptions.LimitExceededException` if query rate or throughput rate limit exceeded
        :raise: :class:`datahub.exceptions.DatahubException` if crc is wrong in pb mode

        .. see also:: :class:`datahub.models.Record`
        """
        return self._datahub_impl.put_records(project_name, topic_name, record_list)

    @type_assert(object, str, str, str, list)
    def put_records_by_shard(self, project_name, topic_name, shard_id, record_list):
        """
        Put records to specific shard of topic

        :param project_name: project name
        :param topic_name: topic name
        :param shard_id: shard id
        :param record_list: record list
        :type record_list: :class:`list`
        :return: failed records info
        :rtype: :class:`datahub.models.PutRecordsResult`
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project or topic not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if the record is not well-formed; project_name, topic_name or shard_id is empty
        :raise: :class:`datahub.exceptions.InvalidOperationException` if the shard is not active
        :raise: :class:`datahub.exceptions.LimitExceededException` if query rate or throughput rate limit exceeded
        :raise: :class:`datahub.exceptions.DatahubException` if crc is wrong in pb mode

        .. see also:: :class:`datahub.models.Record`
        """
        return self._datahub_impl.put_records_by_shard(project_name, topic_name, shard_id, record_list)

    @type_assert(object, str, str, str, str, int)
    def get_blob_records(self, project_name, topic_name, shard_id, cursor, limit_num):
        """
        Get records from a topic

        :param project_name: project name
        :param topic_name: topic name
        :param shard_id: shard id
        :param cursor: the cursor
        :param limit_num: record number need to read
        :return: result include record list, start sequence, record num and next cursor
        :rtype: :class:`datahub.models.GetRecordsResult`
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project or topic or shard not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if the cursor is invalid; project_name, topic_name, shard_id, or cursor is empty
        :raise: :class:`datahub.exceptions.DatahubException` if crc is wrong in pb mode
        """
        return self._datahub_impl.get_blob_records(project_name, topic_name, shard_id, cursor, limit_num)

    @type_assert(object, str, str, str, RecordSchema, str, int)
    def get_tuple_records(self, project_name, topic_name, shard_id, record_schema, cursor, limit_num):
        """
        Get records from a topic

        :param project_name: project name
        :param topic_name: topic name
        :param shard_id: shard id
        :param record_schema: tuple record schema
        :param filter_: filter
        :type record_schema: :class:`datahub.models.RecordSchema`
        :param cursor: the cursor
        :param limit_num: record number need to read
        :return: result include record list, start sequence, record num and next cursor
        :rtype: :class:`datahub.models.GetRecordsResult`
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project or topic or shard not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if the cursor is invalid; project_name, topic_name, shard_id, or cursor is empty
        :raise: :class:`datahub.exceptions.DatahubException` if crc is wrong in pb mode
        """
        return self._datahub_impl.get_tuple_records(project_name, topic_name, shard_id, record_schema, cursor,
                                                    limit_num)

    @type_assert(object, str, str, str)
    def get_metering_info(self, project_name, topic_name, shard_id):
        """
        Get a shard metering info

        :param project_name: project name
        :param topic_name: topic name
        :param shard_id: shard id
        :return: the shard metering info
        :rtype: :class:`datahub.models.GetMeteringInfoResult`
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project, topic or shard not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if the project_name, topic_name or shard_id is empty
        """
        return self._datahub_impl.get_metering_info(project_name, topic_name, shard_id)

    @type_assert(object, str, str)
    def list_connector(self, project_name, topic_name):
        """
        Create a data connector

        :param project_name: project name
        :param topic_name: topic name
        :return: data connector names list
        :rtype: :class:`datahub.models.ListConnectorResult`
        :raise: :class:`datahub.exceptions.InvalidParameterException` if the project_name or topic_name is empty
        """
        return self._datahub_impl.list_connector(project_name, topic_name)

    @type_assert(object, str, str, ConnectorType, list, ConnectorConfig, start_time=int)
    def create_connector(self, project_name, topic_name, connector_type, column_fields, config, start_time=-1):
        """
        Create a data connector

        :param project_name: project name
        :param topic_name: topic name
        :param connector_type: connector type
        :type connector_type: :class:`datahub.models.ConnectorType`
        :param column_fields: column fields
        :type column_fields: :class:`list`
        :param config: connector config
        :type config: :class:`datahub.models.ConnectorConfig`
        :param start_time: start timestamp in milliseconds
        :type start_time: :class:`int`
        :return: connector id
        :rtype: :class:`datahub.models.CreateConnectorResult`
        :raise: :class:`datahub.exceptions.ResourceExistException` if connector is already existed
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project or topic not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if the column field or config is invalid; project_name or topic_name is empty; connector_type or config is wrong type
        """
        return self._datahub_impl.create_connector(project_name, topic_name, connector_type, column_fields, config, start_time)

    @type_assert(object, str, str, (ConnectorType, str), ConnectorConfig)
    def update_connector(self, project_name, topic_name, connector_id, config):
        """

        :param project_name: project name
        :param topic_name: topic name
        :param connector_id: connector id, compatible for connector type
        :type connector_id: :class:`str` or :class:`datahub.models.ConnectorType`
        :param config: connector config
        :type config: :class:`datahub.models.ConnectorConfig`
        :return: none
        """
        self._datahub_impl.update_connector(project_name, topic_name, connector_id.value if isinstance(connector_id, ConnectorType) else connector_id, config)

    @type_assert(object, str, str, (ConnectorType, str))
    def get_connector(self, project_name, topic_name, connector_id):
        """
        Get a data connector

        :param project_name: project name
        :param topic_name: topic name
        :param connector_id: connector id, compatible for connector type
        :type connector_id: :class:`str` or :class:`datahub.models.ConnectorType`
        :return: data connector info
        :rtype: :class:`datahub.models.GetConnectorResult`
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project, topic or connector not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name or topic_name is empty; connector_type or config is wrong type
        """
        return self._datahub_impl.get_connector(project_name, topic_name, connector_id.value if isinstance(connector_id, ConnectorType) else connector_id)

    @type_assert(object, str, str, (ConnectorType, str))
    def delete_connector(self, project_name, topic_name, connector_id):
        """
        Delete a data connector

        :param project_name: project name
        :param topic_name: topic name
        :param connector_id: connector id, compatible for connector type
        :type connector_id: :class:`str` or :class:`datahub.models.ConnectorType`
        :return: none
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project, topic or connector not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name or topic_name is empty; connector_type is wrong type
        """
        self._datahub_impl.delete_connector(project_name, topic_name, connector_id.value if isinstance(connector_id, ConnectorType) else connector_id)

    @type_assert(object, str, str, (ConnectorType, str), str)
    def get_connector_shard_status(self, project_name, topic_name, connector_id, shard_id=''):
        """
        Get data connector shard status

        :param project_name: project name
        :param topic_name: topic name
        :param connector_id: connector id, compatible for connector type
        :type connector_id: :class:`str` or :class:`datahub.models.ConnectorType`
        :param shard_id: shard id
        :return: data connector shard status
        :rtype: :class:`datahub.models.results.GetConnectorShardStatusResult`
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project, topic, shard or connector not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name, topic_name or shard_id is empty; connector_type is wrong type
        """
        return self._datahub_impl.get_connector_shard_status(project_name, topic_name, connector_id.value if isinstance(connector_id, ConnectorType) else connector_id, shard_id)

    @type_assert(object, str, str, (ConnectorType, str), str)
    def reload_connector(self, project_name, topic_name, connector_id, shard_id=''):
        """
        Reload data connector by given shard id or reload all shards without any shard id given

        :param project_name: project name
        :param topic_name: topic name
        :param connector_id: connector id, compatible for connector type
        :type connector_id: :class:`str` or :class:`datahub.models.ConnectorType`
        :param shard_id: shard id
        :return: none
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project, topic or connector not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name, topic_name or filed_name is empty; connector_type is wrong type
        """
        self._datahub_impl.reload_connector(project_name, topic_name, connector_id.value if isinstance(connector_id, ConnectorType) else connector_id, shard_id)

    @type_assert(object, str, str, (ConnectorType, str), str)
    def append_connector_field(self, project_name, topic_name, connector_id, field_name):
        """
        Append field to a connector

        :param project_name: project name
        :param topic_name: topic name
        :param connector_id: connector id, compatible for connector type
        :type connector_id: :class:`str` or :class:`datahub.models.ConnectorType`
        :param field_name: field name
        :return: none
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project, topic or connector not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name, topic_name or filed_name is empty; connector_type is wrong type
        """
        self._datahub_impl.append_connector_field(project_name, topic_name, connector_id.value if isinstance(connector_id, ConnectorType) else connector_id, field_name)

    @type_assert(object, str, str, (ConnectorType, str))
    def get_connector_done_time(self, project_name, topic_name, connector_id):
        """
        Get connector done time

        :param project_name: project name
        :param topic_name: topic name
        :param connector_id: connector id, compatible for connector type
        :type connector_id: :class:`str` or :class:`datahub.models.ConnectorType`
        """
        return self._datahub_impl.get_connector_done_time(project_name, topic_name, connector_id.value if isinstance(connector_id, ConnectorType) else connector_id)

    @type_assert(object, str, str, (ConnectorType, str), ConnectorState)
    def update_connector_state(self, project_name, topic_name, connector_id, connector_state):
        """
        Update data connector state

        :param project_name: project name
        :param topic_name: topic name
        :param connector_id: connector id, compatible for connector type
        :type connector_id: :class:`str` or :class:`datahub.models.ConnectorType`
        :param connector_state: connector state
        :type connector_state: :class:`datahub.models.ConnectorState`
        :return: none
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project, topic or connector not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name or topic_name is empty; connector_type or connector_state is wrong type
        """
        self._datahub_impl.update_connector_state(project_name, topic_name, connector_id.value if isinstance(connector_id, ConnectorType) else connector_id, connector_state)

    @type_assert(object, str, str, (ConnectorType, str), str, ConnectorOffset)
    def update_connector_offset(self, project_name, topic_name, connector_id, shard_id, connector_offset):
        """
        Update data connector offset

        :param project_name: project name
        :param topic_name: topic name
        :param connector_id: connector id, compatible for connector type
        :type connector_id: :class:`str` or :class:`datahub.models.ConnectorType`
        :param shard_id: shard id
        :param connector_offset: current sequence
        :type connector_offset: :class:`datahub.models.ConnectorOffset`
        :return: none
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project, topic or connector not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name or topic_name is empty; connector_type or connector_state is wrong type
        """
        self._datahub_impl.update_connector_offset(project_name, topic_name, connector_id.value if isinstance(connector_id, ConnectorType) else connector_id, shard_id, connector_offset)

    @type_assert(object, str, str, str)
    def init_and_get_subscription_offset(self, project_name, topic_name, sub_id, shard_ids):
        """
        Open subscription offset session

        :param project_name: project name
        :param topic_name: topic name
        :param sub_id: subscription id
        :param shard_ids: shard ids
        :return: offset info
        :rtype :class:`datahub.models.InitAndGetSubscriptionOffsetResult`
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project, topic or subscription not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name, topic_name, sub_id or shard_id is empty
        """
        return self._datahub_impl.init_and_get_subscription_offset(project_name, topic_name, sub_id, shard_ids)

    @type_assert(object, str, str, str)
    def get_subscription_offset(self, project_name, topic_name, sub_id, shard_ids=None):
        """
        Get subscription offset

        :param project_name: project name
        :param topic_name: topic name
        :param sub_id: subscription id
        :param shard_ids: shard ids
        :return: offset info
        :rtype: :class:`datahub.models.results.GetSubscriptionOffsetResult`
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project, topic or subscription not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name, topic_name or sub_id is empty
        """
        return self._datahub_impl.get_subscription_offset(project_name, topic_name, sub_id, shard_ids)

    @type_assert(object, str, str, str, dict)
    def update_subscription_offset(self, project_name, topic_name, sub_id, offsets):
        """
        Update subscription offset

        :param project_name: project name
        :param topic_name: topic name
        :param sub_id: subscription id
        :param offsets: offsets
        :type offsets: :class:`dict`
        :return: none
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project, topic or subscription not exists
        :raise: :class:`datahub.exceptions.InvalidOperationException` if the offset session closed or offset version changed
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name, topic_name or sub_id is empty; offsets is wrong type
        """
        self._datahub_impl.update_subscription_offset(project_name, topic_name, sub_id, offsets)

    @type_assert(object, str, str, str)
    def create_subscription(self, project_name, topic_name, comment):
        """
        Create subscription to a topic

        :param project_name: project name
        :param topic_name: topic name
        :param comment: comment for subscription
        :return: create result contains subscription id
        :rtype: :class:`datahub.models.results.CreateSubscriptionResult`
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project or topic not exists
        :raise: :class:`datahub.exceptions.LimitExceededException` if limit of subscription number is exceeded
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name or topic_name is empty
        """
        return self._datahub_impl.create_subscription(project_name, topic_name, comment)

    @type_assert(object, str, str, str)
    def delete_subscription(self, project_name, topic_name, sub_id):
        """
        Delete subscription by subscription id

        :param project_name: project name
        :param topic_name: topic name
        :param sub_id: subscription id
        :return: none
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project, topic or subscription not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name, topic_name or sub_id is empty
        """
        self._datahub_impl.delete_subscription(project_name, topic_name, sub_id)

    @type_assert(object, str, str, str)
    def get_subscription(self, project_name, topic_name, sub_id):
        """
        Get subscription

        :param project_name: project name
        :param topic_name: topic name
        :param sub_id: subscription id
        :return: subscription info
        :rtype: :class:`datahub.models.results.GetSubscriptionResult`
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project, topic or subscription not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name, topic_name or sub_id is empty
        """
        return self._datahub_impl.get_subscription(project_name, topic_name, sub_id)

    @type_assert(object, str, str, str, str)
    def update_subscription(self, project_name, topic_name, sub_id, comment):
        """
        Update subscription

        :param project_name: project name
        :param topic_name: topic name
        :param sub_id: subscription id
        :param comment: new comment
        :return: none
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project, topic or subscription not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name, topic_name or sub_id is empty
        """
        self._datahub_impl.update_subscription(project_name, topic_name, sub_id, comment)

    @type_assert(object, str, str, str, SubscriptionState)
    def update_subscription_state(self, project_name, topic_name, sub_id, state):
        """
        Update subscription state

        :param project_name: project name
        :param topic_name: topic name
        :param sub_id: subscription id
        :param state: new state
        :type state: :class:`datahub.models.SubscriptionState`
        :return: none
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project, topic or subscription not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name, topic_name or sub_id is empty; state is wrong type
        """
        self._datahub_impl.update_subscription_state(project_name, topic_name, sub_id, state)

    @type_assert(object, str, str, str, int, int)
    def list_subscription(self, project_name, topic_name, query_key, page_index, page_size):
        """
        Query subscription in range [start, end)

        start = (page_index - 1) * page_size + 1

        end = start + page_size

        :param project_name: project name
        :param topic_name: topic name
        :param query_key: query key for search
        :param page_index: page index
        :param page_size: page size
        :return: subscription info list
        :rtype: :class:`datahub.models.results.ListSubscriptionResult`
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name or topic_name is empty; page_index <= 0 or page_size < 0
        """
        return self._datahub_impl.list_subscription(project_name, topic_name, query_key, page_index, page_size)

    @type_assert(object, str, str, str, dict)
    def reset_subscription_offset(self, project_name, topic_name, sub_id, offsets):
        """
        Update subscription offset

        :param project_name: project name
        :param topic_name: topic name
        :param sub_id: subscription id
        :param offsets: offsets
        :type: :class:`dict`
        :return: none
        :raise: :class:`datahub.exceptions.ResourceNotFoundException` if the project, topic or subscription not exists
        :raise: :class:`datahub.exceptions.InvalidParameterException` if project_name, topic_name or sub_id is empty; offsets is wrong type
        """
        self._datahub_impl.reset_subscription_offset(project_name, topic_name, sub_id, offsets)
