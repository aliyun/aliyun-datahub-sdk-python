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
from httmock import HTTMock

from datahub import DataHub
from datahub.exceptions import InvalidParameterException, ResourceNotFoundException, ResourceExistException
from datahub.models import RecordSchema, FieldType, RecordType
from .unittest_util import gen_mock_api

dh = DataHub('access_id', 'access_key', 'http://endpoint')


class TestTopic:

    def test_list_topic_success(self):
        project_name = 'success'

        def check(request):
            assert request.method == 'GET'
            assert request.url == 'http://endpoint/projects/success/topics'

        with HTTMock(gen_mock_api(check)):
            result = dh.list_topic(project_name)
        print(result)
        assert 'topic_name_1' in result.topic_names

    def test_list_topic_with_unexisted_project_name(self):
        project_name = 'unexisted'
        try:
            def check(request):
                assert request.method == 'GET'
                assert request.url == 'http://endpoint/projects/unexisted/topics'

            with HTTMock(gen_mock_api(check)):
                result = dh.list_topic(project_name)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('list success with unexisted project name!')

    def test_list_topic_with_empty_project_name(self):
        project_name = ''
        try:
            result = dh.list_topic(project_name)
        except InvalidParameterException:
            pass
        else:
            raise Exception('list success with unexisted project name!')

    def test_create_topic_success(self):
        project_name = 'success'
        topic_name = 'success'
        shard_count = 3
        life_cycle = 7
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'event_time1'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/success/topics/success'
            content = json.loads(request.body)
            assert content['Comment'] == 'comment'
            assert content['Lifecycle'] == 7
            assert content['ShardCount'] == 3
            if content['RecordType'] == 'TUPLE':
                schema = json.loads(content['RecordSchema'])
                assert len(schema['fields']) == 5
                assert schema['fields'][0]['type'] == 'bigint'
                assert schema['fields'][0]['name'] == 'bigint_field'
                assert schema['fields'][1]['type'] == 'string'
                assert schema['fields'][1]['name'] == 'string_field'
                assert schema['fields'][2]['type'] == 'double'
                assert schema['fields'][2]['name'] == 'double_field'
                assert schema['fields'][3]['type'] == 'boolean'
                assert schema['fields'][3]['name'] == 'bool_field'
                assert schema['fields'][4]['type'] == 'timestamp'
                assert schema['fields'][4]['name'] == 'event_time1'

        with HTTMock(gen_mock_api(check)):
            dh.create_tuple_topic(project_name, topic_name, shard_count, life_cycle, record_schema, 'comment')
            dh.create_blob_topic(project_name, topic_name, shard_count, life_cycle, 'comment')

    def test_create_topic_already_existed(self):
        project_name = 'existed'
        topic_name = 'existed'
        shard_count = 3
        life_cycle = 7
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'event_time1'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/existed/topics/existed'
            content = json.loads(request.body)
            assert content['Comment'] == 'comment'
            assert content['Lifecycle'] == 7
            assert content['ShardCount'] == 3
            if content['RecordType'] == 'TUPLE':
                schema = json.loads(content['RecordSchema'])
                assert len(schema['fields']) == 5
                assert schema['fields'][0]['type'] == 'bigint'
                assert schema['fields'][0]['name'] == 'bigint_field'
                assert schema['fields'][1]['type'] == 'string'
                assert schema['fields'][1]['name'] == 'string_field'
                assert schema['fields'][2]['type'] == 'double'
                assert schema['fields'][2]['name'] == 'double_field'
                assert schema['fields'][3]['type'] == 'boolean'
                assert schema['fields'][3]['name'] == 'bool_field'
                assert schema['fields'][4]['type'] == 'timestamp'
                assert schema['fields'][4]['name'] == 'event_time1'

        try:
            with HTTMock(gen_mock_api(check)):
                dh.create_tuple_topic(project_name, topic_name, shard_count, life_cycle, record_schema, 'comment')
        except ResourceExistException:
            pass
        else:
            raise Exception('create success with topic already existed!')

        try:
            with HTTMock(gen_mock_api(check)):
                dh.create_blob_topic(project_name, topic_name, shard_count, life_cycle, 'comment')
        except ResourceExistException:
            pass
        else:
            raise Exception('create success with topic already existed!')

    def test_create_topic_with_invalid_life_cycle(self):
        project_name = 'valid'
        topic_name = 'valid'
        shard_count = 3
        life_cycle = 0
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'event_time1'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])
        try:
            dh.create_tuple_topic(project_name, topic_name, shard_count, life_cycle, record_schema, 'comment')
        except InvalidParameterException:
            pass
        else:
            raise Exception('create success with invalid life cycle!')

        try:
            dh.create_blob_topic(project_name, topic_name, shard_count, life_cycle, 'comment')
        except InvalidParameterException:
            pass
        else:
            raise Exception('create success with invalid life cycle!')

    def test_create_topic_with_empty_project_name(self):
        project_name = ''
        topic_name = 'valid'
        shard_count = 3
        life_cycle = 7
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'event_time1'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])
        try:
            dh.create_tuple_topic(project_name, topic_name, shard_count, life_cycle, record_schema, 'comment')
        except InvalidParameterException:
            pass
        else:
            raise Exception('create success with empty project name!')

        try:
            dh.create_blob_topic(project_name, topic_name, shard_count, life_cycle, 'comment')
        except InvalidParameterException:
            pass
        else:
            raise Exception('create success with empty project name!')

    def test_create_topic_with_unexisted_project_name(self):
        project_name = 'unexisted'
        topic_name = 'valid'
        shard_count = 3
        life_cycle = 7
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'event_time1'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/unexisted/topics/valid'
            content = json.loads(request.body)
            assert content['Comment'] == 'comment'
            assert content['Lifecycle'] == 7
            assert content['ShardCount'] == 3
            if content['RecordType'] == 'TUPLE':
                schema = json.loads(content['RecordSchema'])
                assert len(schema['fields']) == 5
                assert schema['fields'][0]['type'] == 'bigint'
                assert schema['fields'][0]['name'] == 'bigint_field'
                assert schema['fields'][1]['type'] == 'string'
                assert schema['fields'][1]['name'] == 'string_field'
                assert schema['fields'][2]['type'] == 'double'
                assert schema['fields'][2]['name'] == 'double_field'
                assert schema['fields'][3]['type'] == 'boolean'
                assert schema['fields'][3]['name'] == 'bool_field'
                assert schema['fields'][4]['type'] == 'timestamp'
                assert schema['fields'][4]['name'] == 'event_time1'

        try:
            with HTTMock(gen_mock_api(check)):
                dh.create_tuple_topic(project_name, topic_name, shard_count, life_cycle, record_schema, 'comment')
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('create success with unexisted project name!')

        try:
            with HTTMock(gen_mock_api(check)):
                dh.create_blob_topic(project_name, topic_name, shard_count, life_cycle, 'comment')
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('create success with unexisted project name!')

    def test_create_topic_with_invalid_topic_name(self):
        project_name = 'valid'
        invalid_topic_names = ["", "1invalid", "_invalid", "!invalid",
                               "invalidinvalidinvalidinvalidinvalidinvalidinvalidinvalidinvalidinvalidinvalidinvalid"
                               "invalidinvalidinvalidinvalidinvalidinvalidinvalidinvalidinvalidinvalidinvalid"]
        shard_count = 3
        life_cycle = 7
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'event_time1'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        for invalid_topic_name in invalid_topic_names:
            try:
                dh.create_tuple_topic(project_name, invalid_topic_name, shard_count, life_cycle, record_schema,
                                      'comment')
            except InvalidParameterException:
                pass
            else:
                raise Exception('create success with invalid topic name!')

        for invalid_topic_name in invalid_topic_names:
            try:
                dh.create_blob_topic(project_name, invalid_topic_name, shard_count, life_cycle, 'comment')
            except InvalidParameterException:
                pass
            else:
                raise Exception('create success with invalid topic name!')

    def test_create_tuple_topic_without_schema(self):
        project_name = 'valid'
        topic_name = 'valid'
        shard_count = 3
        life_cycle = 7
        try:
            dh.create_tuple_topic(project_name, topic_name, shard_count, life_cycle, 'comment')
        except TypeError:
            pass
        else:
            raise Exception('create tuple topic success without schema!')

    def test_create_tuple_topic_with_invalid_schema_type(self):
        project_name = 'valid'
        topic_name = 'valid'
        shard_count = 3
        life_cycle = 7
        try:
            dh.create_tuple_topic(project_name, topic_name, shard_count, life_cycle, 'schema', 'comment')
        except InvalidParameterException:
            pass
        else:
            raise Exception('create success with wrong schema type!')

    def test_get_topic_success(self):
        project_name = 'success'
        topic_name = 'tuple'

        def check(request):
            assert request.method == 'GET'
            assert request.url == 'http://endpoint/projects/success/topics/tuple'

        with HTTMock(gen_mock_api(check)):
            tuple_topic_result = dh.get_topic(project_name, topic_name)
        assert tuple_topic_result.project_name == project_name
        assert tuple_topic_result.topic_name == topic_name
        assert tuple_topic_result.comment == 'tuple'
        assert tuple_topic_result.shard_count == 3
        assert tuple_topic_result.life_cycle == 7
        assert tuple_topic_result.record_type == RecordType.TUPLE
        assert tuple_topic_result.create_time == 1525312823
        assert tuple_topic_result.last_modify_time == 1525312823

        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'event_time1'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])
        for index in range(0, len(record_schema.field_list)):
            assert record_schema.field_list[index].name == tuple_topic_result.record_schema.field_list[index].name
            assert record_schema.field_list[index].type == tuple_topic_result.record_schema.field_list[index].type
            assert record_schema.field_list[index].allow_null == tuple_topic_result.record_schema.field_list[
                index].allow_null

        topic_name = 'blob'

        def check(request):
            assert request.method == 'GET'
            assert request.url == 'http://endpoint/projects/success/topics/blob'

        with HTTMock(gen_mock_api(check)):
            blob_topic_result = dh.get_topic(project_name, topic_name)
        assert blob_topic_result.project_name == project_name
        assert blob_topic_result.topic_name == topic_name
        assert blob_topic_result.comment == 'blob'
        assert blob_topic_result.shard_count == 3
        assert blob_topic_result.life_cycle == 7
        assert blob_topic_result.record_type == RecordType.BLOB
        assert blob_topic_result.create_time == 1525344044
        assert blob_topic_result.last_modify_time == 1525344044

    def test_get_topic_with_unexisted_project_name(self):
        project_name = 'unexisted'
        topic_name = 'valid'

        try:
            def check(request):
                assert request.method == 'GET'
                assert request.url == 'http://endpoint/projects/unexisted/topics/valid'

            with HTTMock(gen_mock_api(check)):
                get_result = dh.get_topic(project_name, topic_name)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('get topic success with unexisted project name')

    def test_get_topic_with_unexisted_topic_name(self):
        project_name = 'valid'
        topic_name = 'unexisted'

        try:
            def check(request):
                assert request.method == 'GET'
                assert request.url == 'http://endpoint/projects/valid/topics/unexisted'

            with HTTMock(gen_mock_api(check)):
                get_result = dh.get_topic(project_name, topic_name)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('get topic success with unexisted topic name')

    def test_get_topic_with_empty_project_name(self):
        project_name = ''
        topic_name = 'valid'

        try:
            get_result = dh.get_topic(project_name, topic_name)
        except InvalidParameterException:
            pass
        else:
            raise Exception('get topic success with empty project name')

    def test_get_topic_with_empty_topic_name(self):
        project_name = 'valid'
        topic_name = ''

        try:
            get_result = dh.get_topic(project_name, topic_name)
        except InvalidParameterException:
            pass
        else:
            raise Exception('get topic success with empty topic name')

    def test_update_topic_success(self):
        project_name = 'success'
        topic_name = 'success'
        new_life_cycle = 10

        def check(request):
            assert request.method == 'PUT'
            assert request.url == 'http://endpoint/projects/success/topics/success'
            content = json.loads(request.body)
            assert content['Comment'] == 'new comment'
            assert content['Lifecycle'] == 10

        with HTTMock(gen_mock_api(check)):
            dh.update_topic(project_name, topic_name, new_life_cycle, 'new comment')

    def test_update_topic_with_invalid_life_cycle(self):
        project_name = 'valid'
        topic_name = 'valid'
        shard_count = 3
        life_cycle = 0
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'event_time1'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])
        try:
            dh.create_tuple_topic(project_name, topic_name, shard_count, life_cycle, record_schema, 'comment')
        except InvalidParameterException:
            pass
        else:
            raise Exception('update success with invalid life cycle!')

        try:
            dh.create_blob_topic(project_name, topic_name, shard_count, life_cycle, 'comment')
        except InvalidParameterException:
            pass
        else:
            raise Exception('update success with invalid life cycle!')

    def test_update_topic_with_unexisted_project_name(self):
        project_name = 'unexisted'
        topic_name = 'valid'
        new_life_cycle = 10

        try:
            def check(request):
                assert request.method == 'PUT'
                assert request.url == 'http://endpoint/projects/unexisted/topics/valid'
                content = json.loads(request.body)
                assert content['Comment'] == 'new comment'
                assert content['Lifecycle'] == 10

            with HTTMock(gen_mock_api(check)):
                dh.update_topic(project_name, topic_name, new_life_cycle, 'new comment')
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('update topic success with unexisted project name')

    def test_update_topic_with_unexisted_topic_name(self):
        project_name = 'valid'
        topic_name = 'unexisted'
        new_life_cycle = 10

        try:
            def check(request):
                assert request.method == 'PUT'
                assert request.url == 'http://endpoint/projects/valid/topics/unexisted'
                content = json.loads(request.body)
                assert content['Comment'] == 'new comment'
                assert content['Lifecycle'] == 10

            with HTTMock(gen_mock_api(check)):
                dh.update_topic(project_name, topic_name, new_life_cycle, 'new comment')
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('update topic success with unexisted topic name')

    def test_update_topic_with_empty_project_name(self):
        project_name = ''
        topic_name = 'valid'
        new_life_cycle = 10

        try:
            dh.update_topic(project_name, topic_name, new_life_cycle, 'new comment')
        except InvalidParameterException:
            pass
        else:
            raise Exception('update topic success with empty project name')

    def test_update_topic_with_empty_topic_name(self):
        project_name = 'valid'
        topic_name = ''
        new_life_cycle = 10

        try:
            dh.update_topic(project_name, topic_name, new_life_cycle, 'new comment')
        except InvalidParameterException:
            pass
        else:
            raise Exception('update topic success with empty topic name')

    def test_delete_topic_success(self):
        project_name = 'success'
        topic_name = 'success'

        def check(request):
            assert request.method == 'DELETE'
            assert request.url == 'http://endpoint/projects/success/topics/success'

        with HTTMock(gen_mock_api(check)):
            dh.delete_topic(project_name, topic_name)

    def test_delete_topic_with_unexisted_project_name(self):
        project_name = 'unexisted'
        topic_name = 'valid'

        try:
            def check(request):
                assert request.method == 'DELETE'
                assert request.url == 'http://endpoint/projects/unexisted/topics/valid'

            with HTTMock(gen_mock_api(check)):
                dh.delete_topic(project_name, topic_name)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('delete topic success with unexisted project name')

    def test_delete_topic_with_unexisted_topic_name(self):
        project_name = 'valid'
        topic_name = 'unexisted'

        try:
            def check(request):
                assert request.method == 'DELETE'
                assert request.url == 'http://endpoint/projects/valid/topics/unexisted'

            with HTTMock(gen_mock_api(check)):
                dh.delete_topic(project_name, topic_name)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('delete topic success with unexisted topic name')

    def test_delete_topic_with_empty_project_name(self):
        project_name = ''
        topic_name = 'valid'

        try:
            dh.delete_topic(project_name, topic_name)
        except InvalidParameterException:
            pass
        else:
            raise Exception('delete topic success with empty project name')

    def test_delete_topic_with_empty_topic_name(self):
        project_name = 'valid'
        topic_name = ''

        try:
            dh.delete_topic(project_name, topic_name)
        except InvalidParameterException:
            pass
        else:
            raise Exception('delete topic success with empty topic name')

    def test_append_filed_success(self):
        project_name = 'success'
        topic_name = 'success'
        filed_name = 'double_field'

        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/success/topics/success'
            content = json.loads(request.body)
            assert content['FieldName'] == 'double_field'
            assert content['FieldType'] == 'double'
            assert content['Action'] == 'appendfield'

        with HTTMock(gen_mock_api(check)):
            dh.append_field(project_name, topic_name, filed_name, FieldType.DOUBLE)

    def test_append_field_with_unexisted_project_name(self):
        project_name = 'unexisted'
        topic_name = 'valid'
        filed_name = 'double_field'

        try:
            def check(request):
                assert request.method == 'POST'
                assert request.url == 'http://endpoint/projects/unexisted/topics/valid'
                content = json.loads(request.body)
                assert content['FieldName'] == 'double_field'
                assert content['FieldType'] == 'double'
                assert content['Action'] == 'appendfield'

            with HTTMock(gen_mock_api(check)):
                dh.append_field(project_name, topic_name, filed_name, FieldType.DOUBLE)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('append field success with unexisted project name')

    def test_append_field_with_unexisted_topic_name(self):
        project_name = 'valid'
        topic_name = 'unexisted'
        filed_name = 'double_field'

        try:
            def check(request):
                assert request.method == 'POST'
                assert request.url == 'http://endpoint/projects/valid/topics/unexisted'
                content = json.loads(request.body)
                assert content['FieldName'] == 'double_field'
                assert content['FieldType'] == 'double'
                assert content['Action'] == 'appendfield'

            with HTTMock(gen_mock_api(check)):
                dh.append_field(project_name, topic_name, filed_name, FieldType.DOUBLE)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('append field success with unexisted topic name')

    def test_append_field_with_empty_project_name(self):
        project_name = ''
        topic_name = 'valid'
        filed_name = 'bool_filed'

        try:
            dh.append_field(project_name, topic_name, filed_name, FieldType.DOUBLE)
        except InvalidParameterException:
            pass
        else:
            raise Exception('append field success with empty project name')

    def test_append_field_with_empty_topic_name(self):
        project_name = 'valid'
        topic_name = ''
        filed_name = 'bool_filed'

        try:
            dh.append_field(project_name, topic_name, filed_name, FieldType.DOUBLE)
        except InvalidParameterException:
            pass
        else:
            raise Exception('append field topic success with empty topic name')


if __name__ == '__main__':
    test = TestTopic()
    test.test_list_topic_success()
    test.test_list_topic_with_unexisted_project_name()
    test.test_list_topic_with_empty_project_name()
    test.test_create_topic_success()
    test.test_create_topic_already_existed()
    test.test_create_topic_with_invalid_life_cycle()
    test.test_create_topic_with_empty_project_name()
    test.test_create_topic_with_unexisted_project_name()
    test.test_create_topic_with_invalid_topic_name()
    test.test_create_tuple_topic_without_schema()
    test.test_create_tuple_topic_with_invalid_schema_type()
    test.test_get_topic_success()
    test.test_get_topic_with_unexisted_project_name()
    test.test_get_topic_with_unexisted_topic_name()
    test.test_get_topic_with_empty_project_name()
    test.test_get_topic_with_empty_topic_name()
    test.test_update_topic_success()
    test.test_update_topic_with_invalid_life_cycle()
    test.test_update_topic_with_unexisted_project_name()
    test.test_update_topic_with_unexisted_topic_name()
    test.test_update_topic_with_empty_project_name()
    test.test_update_topic_with_empty_topic_name()
    test.test_delete_topic_success()
    test.test_delete_topic_with_empty_project_name()
    test.test_delete_topic_with_empty_topic_name()
    test.test_delete_topic_with_unexisted_project_name()
    test.test_delete_topic_with_unexisted_topic_name()
    test.test_append_filed_success()
    test.test_append_field_with_unexisted_project_name()
    test.test_append_field_with_unexisted_topic_name()
    test.test_append_field_with_empty_project_name()
    test.test_append_field_with_empty_topic_name()
