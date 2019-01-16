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
import sys
import time
from collections import OrderedDict

import pytest
from six.moves import configparser

from datahub import DataHub
from datahub.exceptions import ResourceExistException, InvalidOperationException
from datahub.models import OdpsConnectorConfig, ConnectorType, ConnectorState, PartitionMode
from datahub.models.connector import DatabaseConnectorConfig, EsConnectorConfig, FcConnectorConfig, AuthMode, \
    OssConnectorConfig, OtsConnectorConfig, ConnectorShardStatus

current_path = os.path.split(os.path.realpath(__file__))[0]
root_path = os.path.join(current_path, '../..')

configer = configparser.ConfigParser()
configer.read(os.path.join(current_path, '../datahub.ini'))

access_id = configer.get('datahub', 'access_id')
access_key = configer.get('datahub', 'access_key')
endpoint = configer.get('datahub', 'endpoint')

connector_test_project_name = configer.get('datahub', 'connector_test_project_name')
system_time_topic_name = configer.get('datahub', 'system_time_topic_name')
event_time_topic_name = configer.get('datahub', 'event_time_topic_name')
user_define_topic_name = configer.get('datahub', 'user_define_topic_name')
ads_test_topic_name = configer.get('datahub', 'ads_test_topic_name')
es_test_topic_name = configer.get('datahub', 'es_test_topic_name')
fc_test_topic_name = configer.get('datahub', 'fc_test_topic_name')
mysql_test_topic_name = configer.get('datahub', 'mysql_test_topic_name')
oss_test_topic_name = configer.get('datahub', 'oss_test_topic_name')
ots_test_topic_name = configer.get('datahub', 'ots_test_topic_name')

odps_project_name = configer.get('odps', 'project_name')
system_time_table_name = configer.get('odps', 'system_time_table_name')
event_time_table_name = configer.get('odps', 'event_time_table_name')
user_define_table_name = configer.get('odps', 'user_define_table_name')

odps_endpoint = configer.get('odps', 'odps_endpoint')
tunnel_endpoint = configer.get('odps', 'tunnel_endpoint')
odps_access_id = configer.get('odps', 'access_id')
odps_access_key = configer.get('odps', 'access_key')

odps_connector_column_fields = json.loads(configer.get('odps', 'column_fields'))
user_define_fields = json.loads(configer.get('odps', 'user_define_fields'))

ads_host = configer.get('ads', 'host')
ads_port = int(configer.get('ads', 'port'))
ads_user = configer.get('ads', 'user')
ads_password = configer.get('ads', 'password')
ads_database = configer.get('ads', 'database')
ads_table = configer.get('ads', 'table')
ads_connector_column_fields = json.loads(configer.get('ads', 'column_fields'))

es_endpoint = configer.get('es', 'endpoint')
es_user = configer.get('es', 'user')
es_password = configer.get('es', 'password')
es_id_fields = json.loads(configer.get('es', 'id_fields'))
es_type_fields = json.loads(configer.get('es', 'type_fields'))
es_connector_column_fields = json.loads(configer.get('es', 'column_fields'))

fc_endpoint = configer.get('fc', 'endpoint')
fc_service = configer.get('fc', 'service')
fc_function = configer.get('fc', 'function')
fc_start_position = configer.get('fc', 'start_position')
fc_column_fields = json.loads(configer.get('fc', 'column_fields'))

mysql_host = configer.get('mysql', 'host')
mysql_port = int(configer.get('mysql', 'port'))
mysql_user = configer.get('mysql', 'user')
mysql_password = configer.get('mysql', 'password')
mysql_database = configer.get('mysql', 'database')
mysql_table = configer.get('mysql', 'table')
mysql_connector_column_fields = json.loads(configer.get('mysql', 'column_fields'))

oss_endpoint = configer.get('oss', 'endpoint')
oss_bucket = configer.get('oss', 'bucket')
oss_access_id = configer.get('oss', 'access_id')
oss_access_key = configer.get('oss', 'access_key')
oss_connector_column_fields = json.loads(configer.get('oss', 'column_fields'))

ots_endpoint = configer.get('ots', 'endpoint')
ots_instance = configer.get('ots', 'instance')
ots_table = configer.get('ots', 'table')
ots_access_id = configer.get('ots', 'access_id')
ots_access_key = configer.get('ots', 'access_key')
ots_connector_column_fields = json.loads(configer.get('ots', 'column_fields'))

print("=======================================")
print("access_id: %s" % access_id)
print("access_key: %s" % access_key)
print("endpoint: %s" % endpoint)
print("=======================================\n\n")

if not access_id or not access_key or not endpoint:
    print("[access_id, access_key, endpoint] must be set in datahub.ini!")
    sys.exit(-1)

dh = DataHub(access_id, access_key, endpoint)


def clean_topic(datahub_client, project_name, force=False):
    topic_names = datahub_client.list_topic(project_name).topic_names
    for topic_name in topic_names:
        if force:
            clean_subscription(datahub_client, project_name, topic_name)
        datahub_client.delete_topic(project_name, topic_name)


def clean_project(datahub_client, force=False):
    project_names = datahub_client.list_project().project_names
    for project_name in project_names:
        if force:
            clean_topic(datahub_client, project_name)
        try:
            datahub_client.delete_project(project_name)
        except InvalidOperationException:
            pass


def clean_subscription(datahub_client, project_name, topic_name):
    subscriptions = datahub_client.list_subscription(project_name, topic_name, '', 1, 100).subscriptions
    for subscription in subscriptions:
        datahub_client.delete_subscription(project_name, topic_name, subscription.sub_id)


class TestConnector:

    def test_list_connector(self):
        result = dh.list_connector(connector_test_project_name, system_time_topic_name)
        print(result)
        assert len(result.connector_names) >= 0

    @pytest.mark.skipif(
        not (connector_test_project_name and system_time_topic_name and odps_project_name and system_time_table_name
             and odps_endpoint and tunnel_endpoint and tunnel_endpoint and odps_access_id and odps_access_key
             and odps_connector_column_fields and user_define_fields),
        reason="odps connector test config isn\'t set")
    def test_create_odps_connector(self):
        partition_config = OrderedDict([
            ("ds", "%Y%m%d"),
            ("hh", "%H"),
            ("mm", "%M")
        ])
        # system time mode
        connector_config_0 = OdpsConnectorConfig(odps_project_name, system_time_table_name, odps_endpoint,
                                                 tunnel_endpoint, odps_access_id, odps_access_key,
                                                 PartitionMode.SYSTEM_TIME, 15, partition_config)
        try:
            dh.create_connector(connector_test_project_name, system_time_topic_name, ConnectorType.SINK_ODPS,
                                odps_connector_column_fields, connector_config_0)
        except ResourceExistException:
            pass

        dh.delete_connector(connector_test_project_name, system_time_topic_name, ConnectorType.SINK_ODPS)

        # event time mode
        connector_config_1 = OdpsConnectorConfig(odps_project_name, event_time_table_name, odps_endpoint,
                                                 tunnel_endpoint, odps_access_id, odps_access_key,
                                                 PartitionMode.EVENT_TIME, 15, partition_config)
        try:
            dh.create_connector(connector_test_project_name, event_time_topic_name, ConnectorType.SINK_ODPS,
                                odps_connector_column_fields, connector_config_1)
        except ResourceExistException:
            pass

        dh.delete_connector(connector_test_project_name, event_time_topic_name, ConnectorType.SINK_ODPS)

        # user define mode
        connector_config_2 = OdpsConnectorConfig(odps_project_name, user_define_table_name, odps_endpoint,
                                                 tunnel_endpoint, odps_access_id, odps_access_key,
                                                 PartitionMode.USER_DEFINE, 15, {})
        try:
            dh.create_connector(connector_test_project_name, user_define_topic_name, ConnectorType.SINK_ODPS,
                                odps_connector_column_fields + user_define_fields, connector_config_2)
        except ResourceExistException:
            pass

        dh.delete_connector(connector_test_project_name, user_define_topic_name, ConnectorType.SINK_ODPS)

    @pytest.mark.skipif(
        not (connector_test_project_name and ads_test_topic_name and ads_host and ads_port
             and ads_user and ads_password and ads_database and ads_table and ads_connector_column_fields),
        reason="ads connector test config isn\'t set")
    def test_create_ads_connector(self):
        connector_config = DatabaseConnectorConfig(ads_host, ads_port, ads_database, ads_user, ads_password,
                                                   ads_table, 10, True)
        try:
            dh.create_connector(connector_test_project_name, ads_test_topic_name, ConnectorType.SINK_ADS,
                                ads_connector_column_fields, connector_config)
        except ResourceExistException:
            pass

        result = dh.get_connector(connector_test_project_name, ads_test_topic_name, ConnectorType.SINK_ADS)
        print(result)
        print(result.config)

        dh.delete_connector(connector_test_project_name, ads_test_topic_name, ConnectorType.SINK_ADS)

    @pytest.mark.skipif(
        not (connector_test_project_name and es_test_topic_name and es_endpoint and es_user and es_password
             and es_id_fields and es_type_fields and es_connector_column_fields),
        reason="es connector test config isn\'t set")
    def test_create_es_connector(self):
        connector_config = EsConnectorConfig("index", es_endpoint, es_user, es_password,
                                             es_id_fields, es_type_fields, 10, True)
        try:
            dh.create_connector(connector_test_project_name, es_test_topic_name, ConnectorType.SINK_ES,
                                es_connector_column_fields, connector_config)
        except ResourceExistException:
            pass

        result = dh.get_connector(connector_test_project_name, es_test_topic_name, ConnectorType.SINK_ES)
        print(result)
        print(result.config)

        dh.delete_connector(connector_test_project_name, es_test_topic_name, ConnectorType.SINK_ES)

    @pytest.mark.skipif(
        not (connector_test_project_name and fc_test_topic_name and fc_endpoint and fc_service
             and fc_function and fc_start_position and fc_column_fields),
        reason="fc connector test config isn\'t set")
    def test_create_fc_connector(self):
        invocation_role = ""
        batch_size = 10
        auth_mode = AuthMode.STS
        connector_config = FcConnectorConfig(fc_endpoint, fc_service, fc_function, invocation_role, batch_size,
                                             auth_mode, fc_start_position)
        try:
            dh.create_connector(connector_test_project_name, fc_test_topic_name, ConnectorType.SINK_FC,
                                fc_column_fields, connector_config)
        except ResourceExistException:
            pass

        result = dh.get_connector(connector_test_project_name, fc_test_topic_name, ConnectorType.SINK_FC)
        print(result)
        print(result.config)

        dh.delete_connector(connector_test_project_name, fc_test_topic_name, ConnectorType.SINK_FC)

    @pytest.mark.skipif(
        not (connector_test_project_name and mysql_test_topic_name and mysql_host and mysql_port
             and mysql_user and mysql_password and mysql_database and mysql_table and mysql_connector_column_fields),
        reason="mysql connector test config isn\'t set")
    def test_create_mysql_connector(self):
        connector_config = DatabaseConnectorConfig(mysql_host, mysql_port, mysql_database, mysql_user, mysql_password,
                                                   mysql_table, 10, True)
        try:
            dh.create_connector(connector_test_project_name, mysql_test_topic_name, ConnectorType.SINK_MYSQL,
                                mysql_connector_column_fields, connector_config)
        except ResourceExistException:
            pass

        result = dh.get_connector(connector_test_project_name, mysql_test_topic_name, ConnectorType.SINK_MYSQL)
        print(result)
        print(result.config)

        dh.delete_connector(connector_test_project_name, mysql_test_topic_name, ConnectorType.SINK_MYSQL)

    @pytest.mark.skipif(
        not (connector_test_project_name and oss_test_topic_name and oss_endpoint and oss_bucket
             and oss_access_id and oss_access_key and oss_connector_column_fields),
        reason="oss connector test config isn\'t set")
    def test_create_oss_connector(self):
        prefix = connector_test_project_name + '/' + oss_test_topic_name
        time_format = '%Y%m%d%H%M'
        time_range = 5
        auth_mode = AuthMode.AK
        connector_config = OssConnectorConfig(oss_endpoint, oss_bucket, prefix, time_format, time_range,
                                              auth_mode, oss_access_id, oss_access_key)
        try:
            dh.create_connector(connector_test_project_name, oss_test_topic_name, ConnectorType.SINK_OSS,
                                oss_connector_column_fields, connector_config)
        except ResourceExistException:
            pass

        result = dh.get_connector(connector_test_project_name, oss_test_topic_name, ConnectorType.SINK_OSS)
        print(result)
        print(result.config)

        dh.delete_connector(connector_test_project_name, oss_test_topic_name, ConnectorType.SINK_OSS)

    @pytest.mark.skipif(
        not (connector_test_project_name and ots_test_topic_name and ots_endpoint and ots_instance
             and ots_table and ots_access_id and ots_access_key and ots_connector_column_fields),
        reason="ots connector test config isn\'t set")
    def test_create_ots_connector(self):
        auth_mode = AuthMode.AK
        connector_config = OtsConnectorConfig(ots_endpoint, ots_instance, ots_table,
                                              auth_mode, ots_access_id, ots_access_key)
        try:
            dh.create_connector(connector_test_project_name, ots_test_topic_name, ConnectorType.SINK_OTS,
                                ots_connector_column_fields, connector_config)
        except ResourceExistException:
            pass

        result = dh.get_connector(connector_test_project_name, ots_test_topic_name, ConnectorType.SINK_OTS)
        print(result)
        print(result.config)

        dh.delete_connector(connector_test_project_name, ots_test_topic_name, ConnectorType.SINK_OTS)

    def test_get_odps_connector(self):
        partition_config = OrderedDict([("ds", "%Y%m%d"), ("hh", "%H"), ("mm", "%M")])

        connector_config = OdpsConnectorConfig(odps_project_name, system_time_table_name, odps_endpoint,
                                               tunnel_endpoint, odps_access_id, odps_access_key,
                                               PartitionMode.SYSTEM_TIME, 15, partition_config)
        try:
            dh.create_connector(connector_test_project_name, system_time_topic_name, ConnectorType.SINK_ODPS,
                                odps_connector_column_fields, connector_config)
        except ResourceExistException:
            pass

        result = dh.get_connector(connector_test_project_name, system_time_topic_name, ConnectorType.SINK_ODPS)
        print(result)
        print(result.config)
        print(result.config.partition_config)
        assert result.type == ConnectorType.SINK_ODPS
        assert result.column_fields == odps_connector_column_fields
        assert result.state == ConnectorState.CONNECTOR_RUNNING
        assert result.config.project_name == odps_project_name
        assert result.config.table_name == system_time_table_name
        assert result.config.odps_endpoint == odps_endpoint
        assert result.config.tunnel_endpoint == tunnel_endpoint
        for shard_context in result.shard_contexts:
            assert shard_context.shard_id != ''
            assert shard_context.current_sequence >= -1

        dh.delete_connector(connector_test_project_name, system_time_topic_name, ConnectorType.SINK_ODPS)

    def test_delete_connector(self):
        partition_config = OrderedDict([("ds", "%Y%m%d"), ("hh", "%H"), ("mm", "%M")])

        connector_config = OdpsConnectorConfig(odps_project_name, system_time_table_name, odps_endpoint,
                                               tunnel_endpoint, odps_access_id, odps_access_key,
                                               PartitionMode.SYSTEM_TIME, 15, partition_config)
        try:
            dh.create_connector(connector_test_project_name, system_time_topic_name, ConnectorType.SINK_ODPS,
                                odps_connector_column_fields, connector_config)
        except ResourceExistException:
            pass

        dh.delete_connector(connector_test_project_name, system_time_topic_name, ConnectorType.SINK_ODPS)

    def test_get_connector_shard_status(self):
        partition_config = OrderedDict([("ds", "%Y%m%d"), ("hh", "%H"), ("mm", "%M")])

        connector_config = OdpsConnectorConfig(odps_project_name, system_time_table_name, odps_endpoint,
                                               tunnel_endpoint, odps_access_id, odps_access_key,
                                               PartitionMode.SYSTEM_TIME, 15, partition_config)
        try:
            dh.create_connector(connector_test_project_name, system_time_topic_name, ConnectorType.SINK_ODPS,
                                odps_connector_column_fields, connector_config)
        except ResourceExistException:
            pass

        result = dh.get_connector_shard_status(connector_test_project_name, system_time_topic_name,
                                               ConnectorType.SINK_ODPS, '0')
        print(result)
        assert result.start_sequence >= 0
        assert result.end_sequence > 0
        assert result.current_sequence >= -1
        assert result.record_time >= 0
        assert result.update_time > 0
        assert result.state == ConnectorShardStatus.CONTEXT_PLANNED

        dh.delete_connector(connector_test_project_name, system_time_topic_name, ConnectorType.SINK_ODPS)

    def test_reload_connector(self):
        partition_config = OrderedDict([("ds", "%Y%m%d"), ("hh", "%H"), ("mm", "%M")])

        connector_config = OdpsConnectorConfig(odps_project_name, system_time_table_name, odps_endpoint,
                                               tunnel_endpoint, odps_access_id, odps_access_key,
                                               PartitionMode.SYSTEM_TIME, 15, partition_config)
        try:
            dh.create_connector(connector_test_project_name, system_time_topic_name, ConnectorType.SINK_ODPS,
                                odps_connector_column_fields, connector_config)
        except ResourceExistException:
            pass

        dh.reload_connector(connector_test_project_name, system_time_topic_name, ConnectorType.SINK_ODPS, '0')

        dh.reload_connector(connector_test_project_name, system_time_topic_name, ConnectorType.SINK_ODPS)

        dh.delete_connector(connector_test_project_name, system_time_topic_name, ConnectorType.SINK_ODPS)

    def test_append_connector_field(self):
        connector_config = OdpsConnectorConfig(odps_project_name, user_define_table_name, odps_endpoint,
                                               tunnel_endpoint, odps_access_id, odps_access_key,
                                               PartitionMode.USER_DEFINE, 15, {})
        try:
            dh.create_connector(connector_test_project_name, user_define_topic_name, ConnectorType.SINK_ODPS,
                                user_define_fields, connector_config)
        except ResourceExistException:
            pass

        dh.append_connector_field(connector_test_project_name, user_define_topic_name, ConnectorType.SINK_ODPS,
                                  odps_connector_column_fields[1])

        dh.delete_connector(connector_test_project_name, user_define_topic_name, ConnectorType.SINK_ODPS)

    def test_update_connector_state(self):
        partition_config = OrderedDict([("ds", "%Y%m%d"), ("hh", "%H"), ("mm", "%M")])

        connector_config = OdpsConnectorConfig(odps_project_name, system_time_table_name, odps_endpoint,
                                               tunnel_endpoint, odps_access_id, odps_access_key,
                                               PartitionMode.SYSTEM_TIME, 15, partition_config)
        try:
            dh.create_connector(connector_test_project_name, system_time_topic_name, ConnectorType.SINK_ODPS,
                                odps_connector_column_fields, connector_config)
        except ResourceExistException:
            pass

        states = [ConnectorState.CONNECTOR_RUNNING, ConnectorState.CONNECTOR_PAUSED]
        for state in states:
            dh.update_connector_state(connector_test_project_name, system_time_topic_name, ConnectorType.SINK_ODPS,
                                      state)
            time.sleep(5)

        dh.delete_connector(connector_test_project_name, system_time_topic_name, ConnectorType.SINK_ODPS)

    def test_get_connector_done_time(self):
        partition_config = OrderedDict([("ds", "%Y%m%d"), ("hh", "%H"), ("mm", "%M")])

        connector_config = OdpsConnectorConfig(odps_project_name, system_time_table_name, odps_endpoint,
                                               tunnel_endpoint, odps_access_id, odps_access_key,
                                               PartitionMode.SYSTEM_TIME, 15, partition_config)
        try:
            dh.create_connector(connector_test_project_name, system_time_topic_name, ConnectorType.SINK_ODPS,
                                odps_connector_column_fields, connector_config)
        except ResourceExistException:
            pass

        time.sleep(1)
        result = dh.get_connector_done_time(connector_test_project_name,
                                            system_time_topic_name, ConnectorType.SINK_ODPS)
        print(result)
        assert result.done_time >= 0

        dh.delete_connector(connector_test_project_name, system_time_topic_name, ConnectorType.SINK_ODPS)

    def test_update_connector(self):
        partition_config = OrderedDict([("ds", "%Y%m%d"), ("hh", "%H"), ("mm", "%M")])

        connector_config = OdpsConnectorConfig(odps_project_name, system_time_table_name, odps_endpoint,
                                               tunnel_endpoint, odps_access_id, odps_access_key,
                                               PartitionMode.SYSTEM_TIME, 15, partition_config)
        try:
            dh.create_connector(connector_test_project_name, system_time_topic_name, ConnectorType.SINK_ODPS,
                                odps_connector_column_fields, connector_config)
        except ResourceExistException:
            pass

        time.sleep(1)

        new_odps_project_name = "1"
        new_system_time_table_name = "2"
        new_odps_endpoint = "3"
        new_tunnel_endpoint = "4"
        new_odps_access_id = "5"
        new_odps_access_key = "6"

        new_partition_config = OrderedDict([("pt", "%Y%m%d"), ("ct", "%H%M")])
        new_connector_config = OdpsConnectorConfig(new_odps_project_name, new_system_time_table_name, new_odps_endpoint,
                                                   new_tunnel_endpoint, new_odps_access_id, new_odps_access_key,
                                                   PartitionMode.USER_DEFINE, 30, new_partition_config)

        dh.update_connector(connector_test_project_name,
                            system_time_topic_name, ConnectorType.SINK_ODPS, new_connector_config)

        config = dh.get_connector(connector_test_project_name, system_time_topic_name,
                                  ConnectorType.SINK_ODPS).config
        assert config.project_name == new_odps_project_name
        assert config.table_name == new_system_time_table_name
        assert config.odps_endpoint == new_odps_endpoint
        assert config.tunnel_endpoint == new_tunnel_endpoint
        assert config.partition_mode == PartitionMode.USER_DEFINE
        for k, v in new_partition_config.items():
            assert config.partition_config.get(k) == v
        dh.delete_connector(connector_test_project_name, system_time_topic_name, ConnectorType.SINK_ODPS)

    def test_update_connector_2(self):
        partition_config = OrderedDict([("ds", "%Y%m%d"), ("hh", "%H"), ("mm", "%M")])

        connector_config = OdpsConnectorConfig(odps_project_name, system_time_table_name, odps_endpoint,
                                               tunnel_endpoint, odps_access_id, odps_access_key,
                                               PartitionMode.SYSTEM_TIME, 15, partition_config)
        try:
            dh.create_connector(connector_test_project_name, system_time_topic_name, ConnectorType.SINK_ODPS,
                                odps_connector_column_fields, connector_config)
        except ResourceExistException:
            pass

        time.sleep(1)

        new_connector_config = dh.get_connector(connector_test_project_name, system_time_topic_name,
                                                ConnectorType.SINK_ODPS).config

        new_connector_config.project_name = "1"
        new_connector_config.table_name = "2"
        new_connector_config.odps_endpoint = "3"
        new_connector_config.tunnel_endpoint = "4"
        new_connector_config.access_id = "5"
        new_connector_config.access_key = "6"

        dh.update_connector(connector_test_project_name,
                            system_time_topic_name, ConnectorType.SINK_ODPS, new_connector_config)

        config = dh.get_connector(connector_test_project_name, system_time_topic_name,
                                  ConnectorType.SINK_ODPS).config
        assert config.project_name == new_connector_config.project_name
        assert config.table_name == new_connector_config.table_name
        assert config.odps_endpoint == new_connector_config.odps_endpoint
        assert config.tunnel_endpoint == new_connector_config.tunnel_endpoint
        assert config.partition_mode == PartitionMode.SYSTEM_TIME
        for k, v in new_connector_config.partition_config.items():
            assert config.partition_config.get(k) == v
        dh.delete_connector(connector_test_project_name, system_time_topic_name, ConnectorType.SINK_ODPS)


# run directly
if __name__ == '__main__':
    test = TestConnector()
    test.test_list_connector()
    test.test_create_odps_connector()
    test.test_create_ads_connector()
    test.test_create_es_connector()
    test.test_create_fc_connector()
    test.test_create_mysql_connector()
    test.test_create_oss_connector()
    test.test_create_ots_connector()
    test.test_get_odps_connector()
    test.test_delete_connector()
    test.test_get_connector_shard_status()
    test.test_reload_connector()
    test.test_append_connector_field()
    test.test_update_connector_state()
    test.test_get_connector_done_time()
    test.test_update_connector()
    test.test_update_connector_2()
