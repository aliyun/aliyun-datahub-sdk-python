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
import os
import sys
import time

from six.moves import configparser

from datahub import DataHub
from datahub.exceptions import ResourceExistException, InvalidOperationException
from datahub.models import OffsetWithSession, OffsetBase, FieldType, RecordSchema, RecordType

current_path = os.path.split(os.path.realpath(__file__))[0]
root_path = os.path.join(current_path, '../..')

configer = configparser.ConfigParser()
configer.read(os.path.join(current_path, '../datahub.ini'))
access_id = configer.get('datahub', 'access_id')
access_key = configer.get('datahub', 'access_key')
endpoint = configer.get('datahub', 'endpoint')

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


class TestOffset:

    def test_init_and_get_subscription_offset(self):
        project_name = "offset_test_p"
        topic_name = "offset_test_t%d_0" % int(time.time())

        shard_count = 3
        life_cycle = 7

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
                dh.create_tuple_topic(project_name, topic_name, shard_count, life_cycle, record_schema, '')
            except ResourceExistException:
                pass

            create_result = dh.create_subscription(project_name, topic_name, 'comment')
            print(create_result)
            sub_id = create_result.sub_id

            # ======================= init and get subscription offset =======================
            result = dh.init_and_get_subscription_offset(project_name, topic_name, sub_id, '0')
            print(result)
            assert result.offsets['0'].sequence == -1
            assert result.offsets['0'].timestamp == -1
            assert result.offsets['0'].version >= 0
            assert result.offsets['0'].session_id >= 0

            result = dh.init_and_get_subscription_offset(project_name, topic_name, sub_id, ['0'])
            print(result)
            assert result.offsets['0'].sequence == -1
            assert result.offsets['0'].timestamp == -1
            assert result.offsets['0'].version >= 0
            assert result.offsets['0'].session_id >= 0

            dh.delete_subscription(project_name, topic_name, create_result.sub_id)
        finally:
            clean_topic(dh, project_name, True)
            dh.delete_project(project_name)

    def test_get_subscription_offset(self):
        project_name = "offset_test_p"
        topic_name = "offset_test_t%d_1" % int(time.time())

        shard_count = 3
        life_cycle = 7

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
                dh.create_tuple_topic(project_name, topic_name, shard_count, life_cycle, record_schema, '')
            except ResourceExistException:
                pass

            create_result = dh.create_subscription(project_name, topic_name, 'comment')
            print(create_result)
            sub_id = create_result.sub_id

            # ======================= get subscription =======================
            dh.init_and_get_subscription_offset(project_name, topic_name, sub_id, '0')
            result = dh.get_subscription_offset(project_name, topic_name, sub_id, '0')
            print(result)
            assert result.offsets['0'].sequence == -1
            assert result.offsets['0'].timestamp == -1
            assert result.offsets['0'].version >= 0

            result = dh.get_subscription_offset(project_name, topic_name, sub_id, ['0'])
            print(result)
            assert result.offsets['0'].sequence == -1
            assert result.offsets['0'].timestamp == -1
            assert result.offsets['0'].version >= 0

            result = dh.get_subscription_offset(project_name, topic_name, sub_id)
            print(result)
            assert result.offsets['0'].sequence == -1
            assert result.offsets['0'].timestamp == -1
            assert result.offsets['0'].version >= 0

            result = dh.get_subscription_offset(project_name, topic_name, sub_id, [])
            print(result)
            assert result.offsets['0'].sequence == -1
            assert result.offsets['0'].timestamp == -1
            assert result.offsets['0'].version >= 0

            dh.delete_subscription(project_name, topic_name, create_result.sub_id)
        finally:
            clean_topic(dh, project_name, True)
            dh.delete_project(project_name)

    def test_update_subscription_offset(self):
        project_name = "offset_test_p"
        topic_name = "offset_test_t%d_2" % int(time.time())

        shard_count = 3
        life_cycle = 7

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
                dh.create_tuple_topic(project_name, topic_name, shard_count, life_cycle, record_schema, '')
            except ResourceExistException:
                pass

            create_result = dh.create_subscription(project_name, topic_name, 'comment')
            print(create_result)
            sub_id = create_result.sub_id

            # ======================= update subscription offset =======================
            result = dh.init_and_get_subscription_offset(project_name, topic_name, sub_id, '0')
            print(result)
            offsets = result.offsets
            offsets['0'].sequence += 1
            offsets['0'].timestamp += 1
            dh.update_subscription_offset(project_name, topic_name, sub_id, offsets)

            offsets2 = {
                '0': OffsetWithSession(
                    offsets['0'].sequence + 1,
                    offsets['0'].timestamp + 1,
                    offsets['0'].version,
                    offsets['0'].session_id
                )
            }
            dh.update_subscription_offset(project_name, topic_name, sub_id, offsets2)

            offsets3 = {
                '0': {
                    'Sequence': 1,
                    'Timestamp': 1,
                    'Version': offsets['0'].version,
                    'SessionId': offsets['0'].session_id
                }
            }
            dh.update_subscription_offset(project_name, topic_name, sub_id, offsets3)

            dh.delete_subscription(project_name, topic_name, create_result.sub_id)
        finally:
            clean_topic(dh, project_name, True)
            dh.delete_project(project_name)

    def test_reset_subscription_offset(self):
        project_name = "offset_test_p"
        topic_name = "offset_test_t%d_3" % int(time.time())

        shard_count = 3
        life_cycle = 7
        record_type = RecordType.TUPLE
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
                dh.create_tuple_topic(project_name, topic_name, shard_count, life_cycle, record_schema, '')
            except ResourceExistException:
                pass

            create_result = dh.create_subscription(project_name, topic_name, 'comment')
            print(create_result)
            sub_id = create_result.sub_id

            # ======================= update subscription state =======================
            result = dh.init_and_get_subscription_offset(project_name, topic_name, sub_id, '0')
            print(result)
            offsets = result.offsets
            offsets['0'].sequence += 1
            offsets['0'].timestamp += 1
            dh.reset_subscription_offset(project_name, topic_name, sub_id, offsets)

            offsets2 = {
                '0': OffsetWithSession(
                    offsets['0'].sequence + 1,
                    offsets['0'].timestamp + 1,
                    offsets['0'].version,
                    offsets['0'].session_id
                )
            }
            dh.reset_subscription_offset(project_name, topic_name, sub_id, offsets2)

            offsets3 = {
                '0': {
                    'Sequence': 1,
                    'Timestamp': 1,
                    'Version': offsets['0'].version,
                    'SessionId': offsets['0'].session_id
                }
            }
            dh.reset_subscription_offset(project_name, topic_name, sub_id, offsets3)

            offsets4 = {
                '0': OffsetBase(
                    offsets['0'].sequence + 1,
                    offsets['0'].timestamp + 1,
                )
            }
            dh.reset_subscription_offset(project_name, topic_name, sub_id, offsets4)

            offsets5 = {
                '0': {
                    'Sequence': 2,
                    'Timestamp': 2,
                }
            }
            dh.reset_subscription_offset(project_name, topic_name, sub_id, offsets5)

            dh.delete_subscription(project_name, topic_name, create_result.sub_id)
        finally:
            clean_topic(dh, project_name, True)
            dh.delete_project(project_name)


# run directly
if __name__ == '__main__':
    test = TestOffset()
    test.test_init_and_get_subscription_offset()
    test.test_get_subscription_offset()
    test.test_update_subscription_offset()
    test.test_reset_subscription_offset()
