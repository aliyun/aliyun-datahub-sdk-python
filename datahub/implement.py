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

import time

from .models.params import *
from .models.results import *
from .auth import AliyunAccount
from .exceptions import InvalidParameterException
from .models import ShardState, OffsetBase, SubscriptionState, FieldType
from .rest import Path
from .rest import RestClient
from .utils import check_project_name_valid, check_topic_name_valid, check_type, check_positive, \
    to_text, ErrorMessage, check_empty, check_negative


class DataHubJson(object):
    """
    Datahub json client
    """

    MAX_WAITING_MILLISECOND = 120

    def __init__(self, access_id, access_key, endpoint=None, compress_format=None, **kwargs):
        self._account = kwargs.pop('account', None)
        if self._account is None:
            security_token = kwargs.pop('security_token', '')
            self._account = AliyunAccount(access_id=access_id, access_key=access_key, security_token=security_token)
        self._endpoint = endpoint
        self._compress_format = compress_format
        self._rest_client = RestClient(self._account, self._endpoint, **kwargs)

    def list_project(self):
        url = Path.PROJECTS

        content = self._rest_client.get(url)

        result = ListProjectResult.parse_content(content)
        return result

    def create_project(self, project_name, comment):
        if not check_project_name_valid(project_name):
            raise InvalidParameterException(ErrorMessage.INVALID_PROJECT_NAME)

        url = Path.PROJECT % project_name
        request_param = CreateProjectRequestParams(comment)

        self._rest_client.post(url, data=request_param.content())

    def get_project(self, project_name):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')

        url = Path.PROJECT % project_name

        content = self._rest_client.get(url)
        result = GetProjectResult.parse_content(to_text(content), project_name=project_name)
        return result

    def update_project(self, project_name, comment):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')

        url = Path.PROJECT % project_name
        request_param = UpdateProjectRequestParams(comment)

        self._rest_client.put(url, data=request_param.content())

    def delete_project(self, project_name):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')

        url = Path.PROJECT % project_name

        self._rest_client.delete(url)

    def list_topic(self, project_name):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')

        url = Path.TOPICS % project_name

        content = self._rest_client.get(url)

        result = ListTopicResult.parse_content(content)
        return result

    def create_blob_topic(self, project_name, topic_name, shard_count, life_cycle, comment):
        self.__create_topic(project_name, topic_name, shard_count, life_cycle, RecordType.BLOB, comment)

    def create_tuple_topic(self, project_name, topic_name, shard_count, life_cycle, record_schema, comment):
        self.__create_topic(project_name, topic_name, shard_count, life_cycle, RecordType.TUPLE, comment, record_schema)

    def get_topic(self, project_name, topic_name):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')

        url = Path.TOPIC % (project_name, topic_name)

        content = self._rest_client.get(url)

        result = GetTopicResult.parse_content(to_text(content), project_name=project_name,
                                              topic_name=topic_name)
        return result

    def update_topic(self, project_name, topic_name, life_cycle, comment):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')
        if not check_positive(life_cycle):
            raise InvalidParameterException(ErrorMessage.PARAMETER_NOT_POSITIVE % 'life_cycle')

        url = Path.TOPIC % (project_name, topic_name)
        request_param = UpdateTopicRequestParams(life_cycle, comment)

        self._rest_client.put(url, data=request_param.content())

    def delete_topic(self, project_name, topic_name):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')

        url = Path.TOPIC % (project_name, topic_name)

        self._rest_client.delete(url)

    def append_field(self, project_name, topic_name, field_name, field_type):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')
        if check_empty(field_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'field_name')
        if not check_type(field_type, FieldType):
            raise InvalidParameterException(ErrorMessage.INVALID_TYPE %
                                            ('field_type', FieldType.__name__))

        url = Path.TOPIC % (project_name, topic_name)
        request_param = AppendFieldParams(field_name, field_type)

        self._rest_client.post(url, data=request_param.content())

    def wait_shards_ready(self, project_name, topic_name, timeout=30):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')
        if check_negative(timeout):
            raise InvalidParameterException(ErrorMessage.PARAMETER_NEGATIVE % 'timeout')

        current_time = time.time()
        end_time = current_time + timeout

        while current_time <= end_time:
            if self.__is_shard_load_completed(project_name, topic_name):
                return
            time.sleep(1)
            current_time = time.time()

        raise DatahubException(ErrorMessage.WAIT_SHARD_TIMEOUT)

    def list_shard(self, project_name, topic_name):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')

        url = Path.SHARDS % (project_name, topic_name)

        content = self._rest_client.get(url)

        result = ListShardResult.parse_content(content)
        return result

    def merge_shard(self, project_name, topic_name, shard_id, adj_shard_id):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')
        if check_empty(shard_id):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'shard_id')
        if check_empty(adj_shard_id):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'adj_shard_id')

        url = Path.SHARDS % (project_name, topic_name)
        request_param = MergeShardRequestParams(shard_id, adj_shard_id)

        content = self._rest_client.post(url, data=request_param.content())

        result = MergeShardResult.parse_content(content)
        return result

    def split_shard(self, project_name, topic_name, shard_id, split_key=''):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')
        if check_empty(shard_id):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'shard_id')
        if check_empty(split_key):
            shard_info = self.__get_shard(project_name, topic_name, shard_id)
            if not check_empty(shard_info):
                begin_hash_key = int(shard_info.begin_hash_key, 16)
                end_hash_key = int(shard_info.end_hash_key, 16)
                split_key = hex(int((begin_hash_key + end_hash_key) // 2))[2:34]

        url = Path.SHARDS % (project_name, topic_name)
        request_param = SplitShardRequestParams(shard_id, split_key)

        content = self._rest_client.post(url, data=request_param.content())

        result = SplitShardResult.parse_content(content)
        return result

    def get_cursor(self, project_name, topic_name, shard_id, cursor_type, param=-1):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')
        if check_empty(shard_id):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'shard_id')
        if not check_type(cursor_type, CursorType):
            raise InvalidParameterException(ErrorMessage.INVALID_TYPE % ('cursor_type', CursorType.__name__))
        if CursorType.SYSTEM_TIME == cursor_type and param < 0:
            raise InvalidParameterException(ErrorMessage.MISSING_SYSTEM_TIME)
        elif CursorType.SEQUENCE == cursor_type and param < 0:
            raise InvalidParameterException(ErrorMessage.MISSING_SEQUENCE)

        url = Path.SHARD % (project_name, topic_name, shard_id)
        request_param = GetCursorRequestParams(cursor_type, param)

        content = self._rest_client.post(url, data=request_param.content())

        result = GetCursorResult.parse_content(content)
        return result

    def put_records(self, project_name, topic_name, record_list):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')

        url = Path.SHARDS % (project_name, topic_name)

        request_param = PutRecordsRequestParams(record_list)

        content = self._rest_client.post(url, data=request_param.content(), headers=request_param.extra_headers(),
                                         compress_format=self._compress_format)

        result = PutRecordsResult.parse_content(content)

        return result

    def put_records_by_shard(self, project_name, topic_name, shard_id, record_list):
        raise DatahubException('put_records_by_shard api only support pb mode')

    def get_blob_records(self, project_name, topic_name, shard_id, cursor, limit_num):
        return self.__get_records(project_name, topic_name, shard_id, cursor, limit_num)

    def get_tuple_records(self, project_name, topic_name, shard_id, record_schema, cursor, limit_num):
        return self.__get_records(project_name, topic_name, shard_id, cursor, limit_num, record_schema)

    def get_metering_info(self, project_name, topic_name, shard_id):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')
        if check_empty(shard_id):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'shard_id')

        url = Path.SHARD % (project_name, topic_name, shard_id)
        request_param = GetMeteringInfoRequestParams()

        content = self._rest_client.post(url, data=request_param.content())

        result = GetMeteringInfoResult.parse_content(content)
        return result

    def list_connector(self, project_name, topic_name):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')

        url = Path.CONNECTORS % (project_name, topic_name) + "?mode=id"

        content = self._rest_client.get(url)

        result = ListConnectorResult.parse_content(content)
        return result

    def create_connector(self, project_name, topic_name, connector_type, column_fields, config, start_time):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')

        url = Path.CONNECTOR % (project_name, topic_name, connector_type.value)
        request_param = CreateConnectorParams(column_fields, config, start_time)

        content = self._rest_client.post(url, data=request_param.content())

        result = CreateConnectorResult.parse_content(content)
        return result

    def update_connector(self, project_name, topic_name, connector_id, config):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')

        url = Path.CONNECTOR % (project_name, topic_name, connector_id)
        request_param = UpdateConnectorParams(config)

        self._rest_client.post(url, data=request_param.content())

    def get_connector(self, project_name, topic_name, connector_id):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')

        url = Path.CONNECTOR % (project_name, topic_name, connector_id)

        content = self._rest_client.get(url)

        result = GetConnectorResult.parse_content(content)
        return result

    def delete_connector(self, project_name, topic_name, connector_id):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')

        url = Path.CONNECTOR % (project_name, topic_name, connector_id)

        self._rest_client.delete(url)

    def get_connector_shard_status(self, project_name, topic_name, connector_id, shard_id=''):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')

        url = Path.CONNECTOR % (project_name, topic_name, connector_id)
        request_param = GetConnectorShardStatusParams(shard_id)

        content = self._rest_client.post(url, data=request_param.content())

        result = GetConnectorShardStatusResult.parse_content(content)
        return result

    def reload_connector(self, project_name, topic_name, connector_id, shard_id=''):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')

        url = Path.CONNECTOR % (project_name, topic_name, connector_id)
        request_param = ReloadConnectorParams(shard_id)

        self._rest_client.post(url, data=request_param.content())

    def append_connector_field(self, project_name, topic_name, connector_id, field_name):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')
        if check_empty(field_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'field_name')

        url = Path.CONNECTOR % (project_name, topic_name, connector_id)
        request_param = AppendConnectorFieldParams(field_name)

        self._rest_client.post(url, data=request_param.content())

    def get_connector_done_time(self, project_name, topic_name, connector_id):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')

        url = Path.DONE_TIME % (project_name, topic_name, connector_id)

        content = self._rest_client.get(url)

        result = GetConnectorDoneTimeResult.parse_content(content)
        return result

    def update_connector_state(self, project_name, topic_name, connector_id, connector_state):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')

        url = Path.CONNECTOR % (project_name, topic_name, connector_id)
        request_param = UpdateConnectorStateParams(connector_state)

        self._rest_client.post(url, data=request_param.content())

    def update_connector_offset(self, project_name, topic_name, connector_id, shard_id, connector_offset):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')

        url = Path.CONNECTOR % (project_name, topic_name, connector_id)
        request_param = UpdateConnectorOffsetParams(shard_id, connector_offset)

        self._rest_client.post(url, data=request_param.content())

    def init_and_get_subscription_offset(self, project_name, topic_name, sub_id, shard_ids):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')
        if check_empty(sub_id):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'sub_id')
        if check_empty(shard_ids):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'shard_ids')

        if isinstance(shard_ids, six.string_types):
            shard_ids = [shard_ids]
        if not check_type(shard_ids, list):
            raise InvalidParameterException(ErrorMessage.INVALID_TYPE % ('shard_ids', list.__name__))

        url = Path.OFFSETS % (project_name, topic_name, sub_id)
        request_param = InitAndGetSubscriptionOffsetParams(shard_ids)

        content = self._rest_client.post(url, data=request_param.content())

        result = InitAndGetSubscriptionOffsetResult.parse_content(content)
        return result

    def get_subscription_offset(self, project_name, topic_name, sub_id, shard_ids=None):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')
        if check_empty(sub_id):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'sub_id')

        if isinstance(shard_ids, six.string_types):
            shard_ids = [shard_ids]

        url = Path.OFFSETS % (project_name, topic_name, sub_id)
        request_param = GetSubscriptionOffsetParams(shard_ids)

        content = self._rest_client.post(url, data=request_param.content())

        result = GetSubscriptionOffsetResult.parse_content(content)
        return result

    def update_subscription_offset(self, project_name, topic_name, sub_id, offsets):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')
        if check_empty(sub_id):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'sub_id')
        if not check_type(offsets, dict):
            raise InvalidParameterException(ErrorMessage.INVALID_TYPE % ('offsets', dict.__name__))

        for (k, v) in offsets.items():
            if isinstance(v, dict):
                offsets[k] = OffsetWithSession.from_dict(v)

        url = Path.OFFSETS % (project_name, topic_name, sub_id)
        request_param = UpdateSubscriptionOffsetParams(offsets)

        self._rest_client.put(url, data=request_param.content())

    # =======================================================
    # internal api
    # =======================================================

    def create_subscription(self, project_name, topic_name, comment):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')

        url = Path.SUBSCRIPTIONS % (project_name, topic_name)
        request_param = CreateSubscriptionParams(comment)

        content = self._rest_client.post(url, data=request_param.content())

        result = CreateSubscriptionResult.parse_content(content)
        return result

    def delete_subscription(self, project_name, topic_name, sub_id):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')
        if check_empty(sub_id):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'sub_id')

        url = Path.SUBSCRIPTION % (project_name, topic_name, sub_id)

        self._rest_client.delete(url)

    def get_subscription(self, project_name, topic_name, sub_id):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')
        if check_empty(sub_id):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'sub_id')

        url = Path.SUBSCRIPTION % (project_name, topic_name, sub_id)

        content = self._rest_client.get(url)

        result = GetSubscriptionResult.parse_content(content)
        return result

    def update_subscription(self, project_name, topic_name, sub_id, comment):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')
        if check_empty(sub_id):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'sub_id')

        url = Path.SUBSCRIPTION % (project_name, topic_name, sub_id)
        request_param = UpdateSubscriptionParams(comment)

        self._rest_client.put(url, data=request_param.content())

    def update_subscription_state(self, project_name, topic_name, sub_id, state):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')
        if check_empty(sub_id):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'sub_id')
        if not check_type(state, SubscriptionState):
            raise InvalidParameterException(ErrorMessage.INVALID_TYPE %
                                            ('state', SubscriptionState.__name__))

        url = Path.SUBSCRIPTION % (project_name, topic_name, sub_id)
        request_param = UpdateSubscriptionStateParams(state)

        self._rest_client.put(url, data=request_param.content())

    def list_subscription(self, project_name, topic_name, query_key, page_index, page_size):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')
        if not check_positive(page_index):
            raise InvalidParameterException(ErrorMessage.PARAMETER_NOT_POSITIVE % 'page_index')
        if check_negative(page_size):
            raise InvalidParameterException(ErrorMessage.PARAMETER_NEGATIVE % 'page_size')

        url = Path.SUBSCRIPTIONS % (project_name, topic_name)
        request_param = ListSubscriptionParams(query_key, page_index, page_size)

        content = self._rest_client.post(url, data=request_param.content())
        result = ListSubscriptionResult.parse_content(content)
        return result

    def reset_subscription_offset(self, project_name, topic_name, sub_id, offsets):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')
        if check_empty(sub_id):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'sub_id')
        if check_empty(offsets):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'offsets')
        if not check_type(offsets, dict):
            raise InvalidParameterException(ErrorMessage.INVALID_TYPE % ('offsets', dict.__name__))

        for (k, v) in offsets.items():
            if isinstance(v, dict):
                offsets[k] = OffsetBase.from_dict(v)

        url = Path.OFFSETS % (project_name, topic_name, sub_id)
        request_param = ResetSubscriptionOffsetParams(offsets)

        self._rest_client.put(url, data=request_param.content())

    # =======================================================
    # private function
    # =======================================================

    def __is_shard_load_completed(self, project_name, topic_name):
        shards = self.list_shard(project_name, topic_name)
        for shard in shards.shards:
            if shard.state not in (ShardState.ACTIVE, ShardState.CLOSED):
                return False
        return True

    def __get_shard(self, project_name, topic_name, shard_id):
        shards = self.list_shard(project_name, topic_name)
        for shard in shards.shards:
            if shard.shard_id == shard_id:
                return shard

    def __create_topic(self, project_name, topic_name, shard_count, life_cycle, record_type, comment,
                       record_schema=None):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if not check_topic_name_valid(topic_name):
            raise InvalidParameterException(ErrorMessage.INVALID_TOPIC_NAME)
        if not check_positive(life_cycle):
            raise InvalidParameterException(ErrorMessage.PARAMETER_NOT_POSITIVE % 'life_cycle')
        if not check_type(record_type, RecordType):
            raise InvalidParameterException(ErrorMessage.INVALID_TYPE % ('record_type', RecordType.__name__))
        if record_type == RecordType.TUPLE:
            if not check_type(record_schema, RecordSchema):
                raise InvalidParameterException(ErrorMessage.INVALID_RECORD_SCHEMA_TYPE)

        url = Path.TOPIC % (project_name, topic_name)
        request_param = CreateTopicRequestParams(shard_count, life_cycle, record_type, record_schema, comment)

        self._rest_client.post(url, data=request_param.content())

    def __get_records(self, project_name, topic_name, shard_id, cursor, limit_num, record_schema=None):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')
        if check_empty(shard_id):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'shard_id')
        if check_empty(cursor):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'cursor')
        if check_type(cursor, GetCursorResult):
            raise InvalidParameterException(ErrorMessage.INVALID_TYPE % ('cursor', 'str'))

        url = Path.SHARD % (project_name, topic_name, shard_id)

        request_param = GetRecordsRequestParams(cursor, limit_num)

        content = self._rest_client.post(url, data=request_param.content(), headers=request_param.extra_headers(),
                                         compress_format=self._compress_format)

        result = GetRecordsResult.parse_content(content, record_schema=record_schema)

        return result


class DataHubPB(DataHubJson):
    """
    DataHub protobuf client
    """

    def put_records(self, project_name, topic_name, record_list):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')

        url = Path.SHARDS % (project_name, topic_name)

        request_param = PutPBRecordsRequestParams(record_list)

        content = self._rest_client.post(url, data=request_param.content(), headers=request_param.extra_headers())

        result = PutPBRecordsResult.parse_content(content)

        return result

    def put_records_by_shard(self, project_name, topic_name, shard_id, record_list):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')
        if check_empty(shard_id):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'shard_id')

        url = Path.SHARD % (project_name, topic_name, shard_id)

        request_param = PutPBRecordsRequestParams(record_list)

        self._rest_client.post(url, data=request_param.content(), headers=request_param.extra_headers())

    def get_blob_records(self, project_name, topic_name, shard_id, cursor, limit_num):
        return self.__get_records(project_name, topic_name, shard_id, cursor, limit_num)

    def get_tuple_records(self, project_name, topic_name, shard_id, record_schema, cursor, limit_num):
        return self.__get_records(project_name, topic_name, shard_id, cursor, limit_num, record_schema)

    def __get_records(self, project_name, topic_name, shard_id, cursor, limit_num, record_schema=None):
        if check_empty(project_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'project_name')
        if check_empty(topic_name):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'topic_name')
        if check_empty(shard_id):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'shard_id')
        if check_empty(cursor):
            raise InvalidParameterException(ErrorMessage.PARAMETER_EMPTY % 'cursor')

        url = Path.SHARD % (project_name, topic_name, shard_id)
        request_param = GetPBRecordsRequestParams(cursor, limit_num)

        content = self._rest_client.post(url, data=request_param.content(), headers=request_param.extra_headers(),
                                         compress_format=self._compress_format)

        result = GetPBRecordsResult.parse_content(content, record_schema=record_schema)

        return result
