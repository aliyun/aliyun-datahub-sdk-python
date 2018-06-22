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

import json
import os

from httmock import HTTMock, urlmatch, response

from datahub import DataHub
from datahub.exceptions import ResourceNotFoundException, InvalidParameterException
from datahub.models import OdpsConnectorConfig, ConnectorType, PartitionMode

_TESTS_PATH = os.path.abspath(os.path.dirname(__file__))
_FIXTURE_PATH = os.path.join(_TESTS_PATH, '../fixtures')

dh = DataHub('access_id', 'access_key', 'http://endpoint')


@urlmatch(netloc=r'(.*\.)?endpoint')
def datahub_api_mock(url, request):
    path = url.path.replace('/', '.')[1:]
    res_file = os.path.join(_FIXTURE_PATH, '%s.json' % path)
    status_code = 200
    content = {
    }
    headers = {
        'Content-Type': 'application/json',
        'x-datahub-request-id': 0
    }
    try:
        with open(res_file, 'rb') as f:
            content = json.loads(f.read().decode('utf-8'))
            if 'ErrorCode' in content:
                status_code = 500
    except (IOError, ValueError) as e:
        content['ErrorMessage'] = 'Loads fixture %s failed, error: %s' % (res_file, e)
    return response(status_code, content, headers, request=request)


class TestConnector:

    def test_list_connector_success(self):
        project_name = 'success'
        topic_name = 'success'
        with HTTMock(datahub_api_mock):
            result = dh.list_connector(project_name, topic_name)
        print(result)
        assert 'connector_1' in result.connector_names

    def test_list_connector_with_empty_project_name(self):
        project_name = ''
        topic_name = 'valid'

        try:
            result = dh.list_connector(project_name, topic_name)
        except InvalidParameterException:
            pass
        else:
            raise Exception('list data connector success with empty project name!')

    def test_list_connector_with_empty_topic_name(self):
        project_name = 'valid'
        topic_name = ''

        try:
            result = dh.list_connector(project_name, topic_name)
        except InvalidParameterException:
            pass
        else:
            raise Exception('list data connector success with empty topic name!')

    def test_create_connector_success(self):
        project_name = 'success'
        topic_name = 'success'
        column_fields = ['field_a', 'field_b']
        connector_config = OdpsConnectorConfig('onnector_project_name', 'connector_table_name', 'dps_endpoint',
                                               'tunnel_endpoint', 'connector_access_id', 'connector_access_key',
                                               PartitionMode.SYSTEM_TIME, 0, {})
        with HTTMock(datahub_api_mock):
            dh.create_connector(project_name, topic_name, ConnectorType.SINK_ODPS, column_fields, connector_config)

    def test_create_connector_with_empty_project_name(self):
        project_name = ''
        topic_name = 'valid'
        column_fields = ['field_a', 'field_b']
        connector_config = OdpsConnectorConfig('onnector_project_name', 'connector_table_name', 'dps_endpoint',
                                               'tunnel_endpoint', 'connector_access_id', 'connector_access_key',
                                               PartitionMode.SYSTEM_TIME, 0, {})

        try:
            dh.create_connector(project_name, topic_name, ConnectorType.SINK_ODPS, column_fields, connector_config)
        except InvalidParameterException:
            pass
        else:
            raise Exception('create data connector success with empty project name!')

    def test_create_connector_with_empty_topic_name(self):
        project_name = 'valid'
        topic_name = ''
        column_fields = ['field_a', 'field_b']
        connector_config = OdpsConnectorConfig('onnector_project_name', 'connector_table_name', 'dps_endpoint',
                                               'tunnel_endpoint', 'connector_access_id', 'connector_access_key',
                                               PartitionMode.SYSTEM_TIME, 0, {})

        try:
            dh.create_connector(project_name, topic_name, ConnectorType.SINK_ODPS, column_fields, connector_config)
        except InvalidParameterException:
            pass
        else:
            raise Exception('create data connector success with empty topic name!')

    def test_create_connector_with_invalid_connector_type(self):
        project_name = 'valid'
        topic_name = 'valid'
        column_fields = ['field_a', 'field_b']

        try:
            dh.create_connector(project_name, topic_name, 'sink_odps', column_fields, 'config')
        except InvalidParameterException:
            pass
        else:
            raise Exception('create data connector success with invalid connector type!')

    def test_create_connector_with_invalid_config_type(self):
        project_name = 'valid'
        topic_name = 'valid'
        column_fields = ['field_a', 'field_b']

        try:
            dh.create_connector(project_name, topic_name, ConnectorType.SINK_ODPS, column_fields, 'config')
        except InvalidParameterException:
            pass
        else:
            raise Exception('create data connector success with invalid config type!')

    def test_create_connector_with_unexisted_project_name(self):
        project_name = 'unexisted'
        topic_name = 'valid'
        column_fields = ['field_a', 'field_b']
        connector_config = OdpsConnectorConfig('onnector_project_name', 'connector_table_name', 'dps_endpoint',
                                               'tunnel_endpoint', 'connector_access_id', 'connector_access_key',
                                               PartitionMode.SYSTEM_TIME, 0, {})

        try:
            with HTTMock(datahub_api_mock):
                dh.create_connector(project_name, topic_name, ConnectorType.SINK_ODPS,
                                    column_fields, connector_config)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('create data connector success with unexisted project name!')

    def test_create_connector_with_unexisted_topic_name(self):
        project_name = 'valid'
        topic_name = 'unexisted'
        column_fields = ['field_a', 'field_b']
        connector_config = OdpsConnectorConfig('onnector_project_name', 'connector_table_name', 'dps_endpoint',
                                               'tunnel_endpoint', 'connector_access_id', 'connector_access_key',
                                               PartitionMode.SYSTEM_TIME, 0, {})

        try:
            with HTTMock(datahub_api_mock):
                dh.create_connector(project_name, topic_name, ConnectorType.SINK_ODPS,
                                    column_fields, connector_config)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('create data connector success with unexisted project name!')

    def test_create_connector_with_invalid_parameter(self):
        project_name = 'valid'
        topic_name = 'valid'
        column_fields = ['field_a', 'field_b', 'invalid']
        connector_config = OdpsConnectorConfig('onnector_project_name', 'connector_table_name_invalid', 'dps_endpoint',
                                               'tunnel_endpoint', 'connector_access_id', 'connector_access_key',
                                               PartitionMode.SYSTEM_TIME, 0, {})

        try:
            with HTTMock(datahub_api_mock):
                dh.create_connector(project_name, topic_name, ConnectorType.SINK_ODPS,
                                    column_fields, connector_config)
        except InvalidParameterException:
            pass
        else:
            raise Exception('create data connector success with unexisted project name!')


if __name__ == '__main__':
    test = TestConnector()
    test.test_list_connector_success()
    test.test_list_connector_with_empty_project_name()
    test.test_list_connector_with_empty_topic_name()
    test.test_create_connector_success()
    test.test_create_connector_with_empty_project_name()
    test.test_create_connector_with_empty_topic_name()
    test.test_create_connector_with_invalid_connector_type()
    test.test_create_connector_with_invalid_config_type()
    test.test_create_connector_with_unexisted_project_name()
    test.test_create_connector_with_unexisted_topic_name()
    test.test_create_connector_with_invalid_parameter()
