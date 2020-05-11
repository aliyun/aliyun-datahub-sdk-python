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
import random
import sys
import time

from six.moves import configparser

from datahub import DataHub
from datahub.exceptions import ResourceExistException, ResourceNotFoundException, InvalidOperationException, \
    LimitExceededException
from datahub.models import RecordType, RecordSchema, FieldType

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


class TestTopic:

    def test_list_topic(self):
        project_name = "topic_test_t"
        topic_name_0 = "topic_test_t%d_0" % int(time.time())

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
                dh.create_tuple_topic(project_name, topic_name_0, shard_count, life_cycle, record_schema, '')
            except ResourceExistException:
                pass

            # ======================= list topic =======================
            result = dh.list_topic(project_name)
            print(result)
            assert topic_name_0 in result.topic_names
        finally:
            clean_topic(dh, project_name)
            dh.delete_project(project_name)

    def test_create_and_delete_topic(self):
        project_name = "topic_test_t"
        topic_name_1 = "topic_test_t%d_1" % int(time.time())
        topic_name_2 = "topic_test_t%d_2" % int(time.time())

        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field', 'decimal_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP,
             FieldType.DECIMAL])

        try:
            dh.create_project(project_name, '')
        except ResourceExistException:
            pass

        # make sure project wil be deleted
        try:
            # ======================= create topic =======================
            try:
                dh.create_tuple_topic(project_name, topic_name_1, 3, 7, record_schema, '1')
            except ResourceExistException:
                pass

            try:
                dh.create_blob_topic(project_name, topic_name_2, 3, 7, '2')
            except ResourceExistException:
                pass
        finally:
            # ======================= delete topic =======================
            clean_topic(dh, project_name)
            dh.delete_project(project_name)

    def test_get_exist_topic(self):
        project_name = "topic_test_t"
        topic_name_1 = "topic_test_t%d_1" % int(time.time())
        topic_name_2 = "topic_test_t%d_2" % int(time.time())
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field', 'decimal_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP,
             FieldType.DECIMAL])

        try:
            dh.create_project(project_name, '')
        except ResourceExistException:
            pass

        # make sure project wil be deleted
        try:
            try:
                dh.create_tuple_topic(project_name, topic_name_1, 3, 7, record_schema, '1')
            except ResourceExistException:
                pass

            try:
                dh.create_blob_topic(project_name, topic_name_2, 3, 7, '2')
            except ResourceExistException:
                pass

            # ======================= get topic =======================
            topic_result_1 = dh.get_topic(project_name, topic_name_1)
            print(topic_result_1)
            assert topic_result_1.project_name == project_name
            assert topic_result_1.topic_name == topic_name_1
            assert topic_result_1.comment == '1'
            assert topic_result_1.life_cycle == 7
            assert topic_result_1.record_type == RecordType.TUPLE
            for index, field in enumerate(topic_result_1.record_schema.field_list):
                assert field.name == record_schema.field_list[index].name
                assert field.type == record_schema.field_list[index].type
                assert field.allow_null == record_schema.field_list[index].allow_null
            assert topic_result_1.shard_count == 3

            topic_result_2 = dh.get_topic(project_name, topic_name_2)
            print(topic_result_2)
            assert topic_result_2.topic_name == topic_name_2
            assert topic_result_2.project_name == project_name
            assert topic_result_2.comment == '2'
            assert topic_result_2.life_cycle == 7
            assert topic_result_2.record_type == RecordType.BLOB
            assert topic_result_2.shard_count == 3
        finally:
            clean_topic(dh, project_name)
            dh.delete_project(project_name)

    def test_get_unexist_topic(self):
        project_name = "topic_test_t"
        unexist_topic_name = "unexist_topic_test_t%d" % random.randint(1000, 9999)

        try:
            dh.create_project(project_name, '')
        except ResourceExistException:
            pass

        results = dh.list_topic(project_name)
        unexist = True

        # try to find an unexist topic name and test
        for i in range(0, 10):
            if unexist_topic_name in results.topic_names:
                unexist = False
                unexist_topic_name = "unexist_topic_test_p%d" % random.randint(1000, 9999)
            else:
                unexist = True
                break

        # make sure project wil be deleted
        try:
            # ======================= get unexisted topic =======================
            if unexist:
                try:
                    dh.get_topic(project_name, unexist_topic_name)
                except ResourceNotFoundException:
                    pass
                else:
                    raise Exception('Get topic success with unexisted topic name')
        finally:
            clean_topic(dh, project_name)
            dh.delete_project(project_name)

    def test_update_topic(self):
        project_name = "topic_test_t"
        topic_name_1 = "topic_test_t%d_1" % int(time.time())
        topic_name_2 = "topic_test_t%d_2" % int(time.time())

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
                dh.create_tuple_topic(project_name, topic_name_1, 3, 7, record_schema, '1')
            except ResourceExistException:
                pass

            try:
                dh.create_blob_topic(project_name, topic_name_2, 3, 7, '2')
            except ResourceExistException:
                pass

            time.sleep(1)
            # ======================= update topic =======================
            dh.update_topic(project_name, topic_name_1, 7, "2")
            topic_result_1 = dh.get_topic(project_name, topic_name_1)
            assert topic_result_1.life_cycle == 7
            assert topic_result_1.comment == "2"

            try:
                dh.update_topic(project_name, topic_name_2, 5, "new comment")
            except LimitExceededException:
                pass
            # topic_result_2 = dh.get_topic(project_name, topic_name_2)
            # assert topic_result_2.comment == "new comment"

        finally:
            clean_topic(dh, project_name)
            dh.delete_project(project_name)

    def test_append_field(self):
        project_name = "topic_test_t"
        topic_name_1 = "topic_test_t%d_1" % int(time.time())
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
                dh.create_tuple_topic(project_name, topic_name_1, 3, 7, record_schema, '1')
            except ResourceExistException:
                pass

            time.sleep(1)
            # ======================= append field =======================
            dh.append_field(project_name, topic_name_1, 'append', FieldType.BOOLEAN)

        finally:
            clean_topic(dh, project_name)
            dh.delete_project(project_name)


# run directly
if __name__ == '__main__':
    test = TestTopic()
    test.test_list_topic()
    test.test_create_and_delete_topic()
    test.test_get_exist_topic()
    test.test_get_unexist_topic()
    test.test_update_topic()
    test.test_append_field()
