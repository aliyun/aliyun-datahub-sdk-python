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
import sys

sys.path.append('./')
from collections import OrderedDict

from httmock import HTTMock

from datahub import DataHub
from datahub.exceptions import ResourceNotFoundException, InvalidParameterException
from datahub.models import OdpsConnectorConfig, ConnectorType, PartitionMode
from .unittest_util import gen_mock_api

dh = DataHub('access_id', 'access_key', 'http://endpoint')


class TestConnector:

    def test_list_connector_success(self):
        project_name = 'success'
        topic_name = 'success'

        def check(request):
            assert request.method == 'GET'
            assert request.url == 'http://endpoint/projects/success/topics/success/connectors?mode=id'

        with HTTMock(gen_mock_api(check)):
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
        partition_config = OrderedDict([("ds", "%Y%m%d"), ("hh", "%H"), ("mm", "%M")])
        connector_config = OdpsConnectorConfig('connector_project_name', 'connector_table_name', 'dps_endpoint',
                                               'tunnel_endpoint', 'connector_access_id', 'connector_access_key',
                                               PartitionMode.SYSTEM_TIME, 0, partition_config)

        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/success/topics/success/connectors/sink_odps'
            content = json.loads(request.body)
            print(request.body)
            assert content['Action'] == 'Create'
            assert content['ColumnFields'] == column_fields
            assert content['SinkStartTime'] == -1
            assert content['Config']['PartitionMode'] == PartitionMode.SYSTEM_TIME.value
            assert content['Config']['Project'] == 'connector_project_name'
            assert content['Config']['TunnelEndpoint'] == 'tunnel_endpoint'
            assert content['Config']['Table'] == 'connector_table_name'
            assert content['Config']['OdpsEndpoint'] == 'dps_endpoint'
            assert content['Config']['AccessId'] == 'connector_access_id'
            assert content['Config']['AccessKey'] == 'connector_access_key'
            assert content['Config']['PartitionConfig']['ds'] == '%Y%m%d'
            assert content['Config']['PartitionConfig']['hh'] == '%H'
            assert content['Config']['PartitionConfig']['mm'] == '%M'

        with HTTMock(gen_mock_api(check)):
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
        connector_config = OdpsConnectorConfig('connector_project_name', 'connector_table_name', 'dps_endpoint',
                                               'tunnel_endpoint', 'connector_access_id', 'connector_access_key',
                                               PartitionMode.SYSTEM_TIME, 0, {})

        try:
            def check(request):
                assert request.method == 'POST'
                assert request.url == 'http://endpoint/projects/unexisted/topics/valid/connectors/sink_odps'
                content = json.loads(request.body)
                assert content['Action'] == 'Create'
                assert content['ColumnFields'] == column_fields
                assert content['SinkStartTime'] == -1
                assert content['Config']['PartitionMode'] == PartitionMode.SYSTEM_TIME.value
                assert content['Config']['Project'] == 'connector_project_name'
                assert content['Config']['TunnelEndpoint'] == 'tunnel_endpoint'
                assert content['Config']['Table'] == 'connector_table_name'
                assert content['Config']['OdpsEndpoint'] == 'dps_endpoint'
                assert content['Config']['AccessId'] == 'connector_access_id'
                assert content['Config']['AccessKey'] == 'connector_access_key'

            with HTTMock(gen_mock_api(check)):
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
        connector_config = OdpsConnectorConfig('connector_project_name', 'connector_table_name', 'dps_endpoint',
                                               'tunnel_endpoint', 'connector_access_id', 'connector_access_key',
                                               PartitionMode.SYSTEM_TIME, 0, {})

        try:
            def check(request):
                assert request.method == 'POST'
                assert request.url == 'http://endpoint/projects/valid/topics/unexisted/connectors/sink_odps'
                content = json.loads(request.body)
                assert content['Action'] == 'Create'
                assert content['ColumnFields'] == column_fields
                assert content['SinkStartTime'] == -1
                assert content['Config']['PartitionMode'] == PartitionMode.SYSTEM_TIME.value
                assert content['Config']['Project'] == 'connector_project_name'
                assert content['Config']['TunnelEndpoint'] == 'tunnel_endpoint'
                assert content['Config']['Table'] == 'connector_table_name'
                assert content['Config']['OdpsEndpoint'] == 'dps_endpoint'
                assert content['Config']['AccessId'] == 'connector_access_id'
                assert content['Config']['AccessKey'] == 'connector_access_key'

            with HTTMock(gen_mock_api(check)):
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
        connector_config = OdpsConnectorConfig('connector_project_name', 'connector_table_name_invalid', 'dps_endpoint',
                                               'tunnel_endpoint', 'connector_access_id', 'connector_access_key',
                                               PartitionMode.SYSTEM_TIME, 0, {})

        try:
            def check(request):
                assert request.method == 'POST'
                assert request.url == 'http://endpoint/projects/valid/topics/valid/connectors/sink_odps'
                content = json.loads(request.body)
                assert content['Action'] == 'Create'
                assert content['ColumnFields'] == column_fields
                assert content['SinkStartTime'] == -1
                assert content['Config']['PartitionMode'] == PartitionMode.SYSTEM_TIME.value
                assert content['Config']['Project'] == 'connector_project_name'
                assert content['Config']['TunnelEndpoint'] == 'tunnel_endpoint'
                assert content['Config']['Table'] == 'connector_table_name_invalid'
                assert content['Config']['OdpsEndpoint'] == 'dps_endpoint'
                assert content['Config']['AccessId'] == 'connector_access_id'
                assert content['Config']['AccessKey'] == 'connector_access_key'

            with HTTMock(gen_mock_api(check)):
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
