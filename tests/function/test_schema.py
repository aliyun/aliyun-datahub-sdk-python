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
from datahub.models import RecordSchema, FieldType, Field, TupleRecord, CursorType

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


class TestSchema:

    def test_schema(self):
        project_name = "topic_test_p"
        topic_name_0 = "topic_test_t%d_0" % int(time.time())
        topic_name_1 = "topic_test_t%d_1" % int(time.time())

        shard_count = 3
        life_cycle = 7
        record_schema_0 = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'event_time1'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        record_schema_1 = RecordSchema()
        record_schema_1.add_field(Field('bigint_field', FieldType.BIGINT))
        record_schema_1.add_field(Field('string_field', FieldType.STRING, False))
        record_schema_1.add_field(Field('double_field', FieldType.DOUBLE, False))
        record_schema_1.add_field(Field('bool_field', FieldType.BOOLEAN))
        record_schema_1.add_field(Field('event_time1', FieldType.TIMESTAMP))
        record_schema_1.add_field(Field('float_field', FieldType.FLOAT))

        print(record_schema_0)
        print(RecordSchema())

        try:
            dh.create_project(project_name, '')
        except ResourceExistException:
            pass

        # make sure project wil be deleted
        try:
            try:
                dh.create_tuple_topic(project_name, topic_name_0, shard_count, life_cycle, record_schema_0, '')
            except ResourceExistException:
                pass
            result = dh.get_topic(project_name, topic_name_0)
            print(result)
            for index, field in enumerate(result.record_schema.field_list):
                assert field.name == record_schema_0.field_list[index].name
                assert field.type == record_schema_0.field_list[index].type
                assert field.allow_null == record_schema_0.field_list[index].allow_null

            try:
                dh.create_tuple_topic(project_name, topic_name_1, shard_count, life_cycle, record_schema_1, '')
            except ResourceExistException:
                pass
            result = dh.get_topic(project_name, topic_name_1)
            print(result)
            for index, field in enumerate(result.record_schema.field_list):
                assert field.name == record_schema_1.field_list[index].name
                assert field.type == record_schema_1.field_list[index].type
                assert field.allow_null == record_schema_1.field_list[index].allow_null

            record = TupleRecord(schema=record_schema_1, values=[1, 'yc1', 10.01, True, 1455869335000000, 2.2])
            put_result = dh.put_records_by_shard(project_name, topic_name_1, "0", [record])

            tuple_cursor_result = dh.get_cursor(project_name, topic_name_1, '0', CursorType.OLDEST)
            get_result = dh.get_tuple_records(project_name, topic_name_1, '0', record_schema_0,
                                              tuple_cursor_result.cursor, 1)

            print(get_result)
        finally:
            clean_topic(dh, project_name)
            dh.delete_project(project_name)


# run directly
if __name__ == '__main__':
    test = TestSchema()
    test.test_schema()
