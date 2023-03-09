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
import json
import sys

from datahub.batch.schema_registry_client import SchemaRegistryClient

sys.path.append('./')

from httmock import HTTMock

from datahub import DataHub, DatahubProtocolType
from datahub.exceptions import ResourceNotFoundException
from datahub.models import RecordSchema, FieldType
from unittest_util import gen_mock_api

dh = DataHub('access_id', 'access_key', 'http://endpoint', protocol_type=DatahubProtocolType.JSON)
schema_register = SchemaRegistryClient(dh)

class TestSchemaRegister:

    def test_register_schema(self):
        project_name = 'schema'
        topic_name = 'register'

        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/schema/topics/register'
            content = json.loads(request.body)
            assert content['Action'] == 'RegisterSchema'
            assert content['RecordSchema'] == record_schema.to_json_string()

        with HTTMock(gen_mock_api(check)):
            register_schema = dh.register_topic_schema(project_name, topic_name, record_schema)
            print("register schema: ", register_schema, type(register_schema))
        assert register_schema.version_id == 0

    def test_list_schema(self):
        project_name = 'schema'
        topic_name = 'list'

        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP],
        )

        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/schema/topics/list'
            content = json.loads(request.body)
            assert content['Action'] == 'ListSchema'
            assert content['PageNumber'] == -1
            assert content['PageSize'] == -1

        with HTTMock(gen_mock_api(check)):
            list_schema = dh.list_topic_schema(project_name, topic_name, page_number=-1, page_size=-1)
            print("list schema: ", list_schema, type(list_schema))
        assert list_schema.page_count == 1
        assert list_schema.page_number == 1
        assert list_schema.page_size == 1
        assert list_schema.total_count == 1
        assert list_schema.record_schema_list[0]["RecordSchema"] == record_schema.to_json_string()

    def test_get_schema(self):
        project_name = 'schema'
        topic_name = 'get'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP],
        )

        def check_version(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/schema/topics/get'
            content = json.loads(request.body)
            assert content['Action'] == 'GetSchema'
            assert content['VersionId'] == 0

        # ---------- get schema ---------
        with HTTMock(gen_mock_api(check_version)):
            get_schema_schema = dh.get_topic_schema(project_name, topic_name, version_id=0)
            print("get schema: ", get_schema_schema)
        assert get_schema_schema.version_id == 1
        assert get_schema_schema.record_schema == record_schema.to_json_string()

        def check_schema(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/schema/topics/get'
            content = json.loads(request.body)
            assert content['Action'] == 'GetSchema'
            assert content['RecordSchema'] == record_schema.to_json_string()

        # ---------- get version id ---------
        with HTTMock(gen_mock_api(check_schema)):
            get_schema_version = dh.get_topic_schema(project_name, topic_name, schema=record_schema)
            print("get version_id: ", get_schema_version)
        assert get_schema_version.version_id == 1
        assert get_schema_version.record_schema == record_schema.to_json_string()

    def test_delete_schema(self):
        project_name = 'schema'
        topic_name = 'delete'

        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/schema/topics/delete'
            content = json.loads(request.body)
            assert content['Action'] == 'DeleteSchema'
            assert content['VersionId'] == 0

        with HTTMock(gen_mock_api(check)):
            delete_schema = dh.delete_topic_schema(project_name, topic_name, version_id=0)
            print("delete schema: ", delete_schema)

    def test_schema_register_get_schema_success(self):
        project_name = 'schema'
        topic_name = 'list'
        version_id = 1
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        def check(request):
            content = json.loads(request.body)
            assert content['Action'] == 'ListSchema'
            assert request.url == 'http://endpoint/projects/schema/topics/list'

        with HTTMock(gen_mock_api(check)):
            get_schema = schema_register.get_schema(project_name, topic_name, version_id)
            print(get_schema)
            assert get_schema.to_json() == record_schema.to_json()


    def test_schema_register_get_version_id_success(self):
        project_name = 'schema'
        topic_name = 'list'
        version_id = 1
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        def check(request):
            content = json.loads(request.body)
            assert content['Action'] == 'ListSchema'
            assert request.url == 'http://endpoint/projects/schema/topics/list'

        with HTTMock(gen_mock_api(check)):
            get_version_id = schema_register.get_version_id(project_name, topic_name, record_schema)
            print(get_version_id)
            assert get_version_id == version_id


    def test_schema_register_get_schema_with_invalid_version_id(self):
        project_name = 'schema'
        topic_name = 'list'
        version_id = 2
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        try:
            def check(request):
                content = json.loads(request.body)
                assert content['Action'] == 'ListSchema'
                assert request.url == 'http://endpoint/projects/schema/topics/list'

            with HTTMock(gen_mock_api(check)):
                get_schema = schema_register.get_schema(project_name, topic_name, version_id)
                print(get_schema)
                assert get_schema.to_json() == record_schema.to_json()
        except ResourceNotFoundException:
            pass
        else:
            raise Exception("Get schema with invalid version_id!")


    def test_schema_register_get_version_id_with_invalid_schema(self):
        project_name = 'schema'
        topic_name = 'list'
        version_id = 1
        record_schema = RecordSchema.from_lists(
            ['bigint_field'],
            [FieldType.BIGINT])
        try:
            def check(request):
                content = json.loads(request.body)
                assert content['Action'] == 'ListSchema'
                assert request.url == 'http://endpoint/projects/schema/topics/list'

            with HTTMock(gen_mock_api(check)):
                get_version_id = schema_register.get_version_id(project_name, topic_name, record_schema)
                print(get_version_id)
                assert get_version_id == version_id
        except ResourceNotFoundException:
            pass
        else:
            raise Exception("Get version_id with invalid schema!")


if __name__ == '__main__':
    test = TestSchemaRegister()

    test.test_register_schema()
    test.test_list_schema()
    test.test_get_schema()
    test.test_delete_schema()

    test.test_schema_register_get_schema_success()
    test.test_schema_register_get_version_id_success()
    test.test_schema_register_get_schema_with_invalid_version_id()
    test.test_schema_register_get_version_id_with_invalid_schema()
