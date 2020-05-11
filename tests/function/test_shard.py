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

import os
import sys
import time

from six.moves import configparser

from datahub import DataHub
from datahub.exceptions import ResourceExistException, InvalidOperationException
from datahub.models import RecordSchema, FieldType, ShardState

current_path = os.path.split(os.path.realpath(__file__))[0]
root_path = os.path.join(current_path, '../..')

configer = configparser.ConfigParser()
configer.read(os.path.join(current_path, '../datahub.ini'))
access_id = configer.get('datahub', 'access_id')
access_key = configer.get('datahub', 'access_key')
endpoint = configer.get('datahub', 'endpoint')

print('=======================================')
print('access_id: %s' % access_id)
print('access_key: %s' % access_key)
print('endpoint: %s' % endpoint)
print('=======================================\n\n')

if not access_id or not access_key or not endpoint:
    print('[access_id, access_key, endpoint] must be set in datahub.ini!')
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


class TestShard:

    def test_list_shard(self):
        project_name = "shard_test_p"
        topic_name_0 = 'shard_test_t%d_0' % int(time.time())
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'event_time1'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        try:
            dh.create_project(project_name, '')
        except ResourceExistException:
            pass

        # make sure project wil be deleted
        try:
            try:
                dh.create_tuple_topic(project_name, topic_name_0, 3, 7, record_schema, '1')
            except ResourceExistException:
                pass
            # ======================= list shard =======================
            shard_results = dh.list_shard(project_name, topic_name_0)
            print(shard_results)
            assert len(shard_results.shards) == 3
            assert shard_results.shards[0].left_shard_id != ''
            assert shard_results.shards[0].right_shard_id != ''
        finally:
            clean_topic(dh, project_name)
            dh.delete_project(project_name)

    def test_split_merge_shard(self):
        project_name = "shard_test_p"
        topic_name_1 = 'shard_test_t%d_1' % int(time.time())
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'event_time1'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        try:
            dh.create_project(project_name, '')
        except ResourceExistException:
            pass

        # make sure project wil be deleted
        try:
            try:
                dh.create_tuple_topic(project_name, topic_name_1, 1, 7, record_schema, '1')
            except ResourceExistException:
                pass

            time.sleep(5)
            # ======================= split shard =======================
            new_shards = dh.split_shard(project_name, topic_name_1, '0',
                                        '66666666666666666666666666666666').new_shards
            assert new_shards[0].shard_id == '1'
            assert new_shards[1].shard_id == '2'
            assert new_shards[0].end_hash_key == '66666666666666666666666666666666'
            assert new_shards[1].begin_hash_key == '66666666666666666666666666666666'

            dh.wait_shards_ready(project_name, topic_name_1)
            shard_list = dh.list_shard(project_name, topic_name_1).shards
            print(shard_list)

            for shard in shard_list:
                if shard.shard_id == '0':
                    assert shard.state == ShardState.CLOSED
                else:
                    assert shard.state == ShardState.ACTIVE

            time.sleep(5)
            # ======================= merge shard =======================
            merge_result = dh.merge_shard(project_name, topic_name_1, '1', '2')
            print(merge_result)
            assert merge_result.shard_id == '3'

            dh.wait_shards_ready(project_name, topic_name_1)
            shard_list = dh.list_shard(project_name, topic_name_1).shards
            print(shard_list)

            for shard in shard_list:
                if shard.shard_id in ('0', '1', '2'):
                    assert shard.state == ShardState.CLOSED
                else:
                    assert shard.state == ShardState.ACTIVE
        finally:
            clean_topic(dh, project_name)
            dh.delete_project(project_name)


# run directly
if __name__ == '__main__':
    test = TestShard()
    test.test_list_shard()
    test.test_split_merge_shard()
