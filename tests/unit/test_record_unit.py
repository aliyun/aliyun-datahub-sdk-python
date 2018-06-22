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
import os

from httmock import HTTMock, urlmatch, response

from datahub import DataHub
from datahub.exceptions import ResourceNotFoundException, InvalidOperationException, \
    InvalidParameterException, LimitExceededException
from datahub.models import RecordSchema, FieldType, BlobRecord, TupleRecord

_TESTS_PATH = os.path.abspath(os.path.dirname(__file__))
_FIXTURE_PATH = os.path.join(_TESTS_PATH, '../fixtures')

dh = DataHub('access_id', 'access_key', 'http://endpoint', enable_pb=False)
dh2 = DataHub('access_id', 'access_key', 'http://endpoint', enable_pb=True)


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
    except (IOError, InvalidParameterException) as e:
        content['ErrorMessage'] = 'Loads fixture %s failed, error: %s' % (res_file, e)
    return response(status_code, content, headers, request=request)


@urlmatch(netloc=r'(.*\.)?endpoint')
def datahub_pb_api_mock(url, request):
    path = url.path.replace('/', '.')[1:]
    res_file = os.path.join(_FIXTURE_PATH, '%s.bin' % path)
    status_code = 200
    content = {
    }
    headers = {
        'Content-Type': 'application/x-protobuf',
        'x-datahub-request-id': 0
    }
    try:
        with open(res_file, 'rb') as f:
            content = f.read()
    except (IOError, InvalidParameterException) as e:
        content['ErrorMessage'] = 'Loads fixture %s failed, error: %s' % (res_file, e)
    return response(status_code, content, headers, request=request)


class TestRecord:

    def test_build_tuple_record_allow_null(self):
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP],
            [False, True, False, True, True])

        try:
            record0 = TupleRecord(schema=record_schema, values=[1, 'yc1', None, True, 253402271999000000])
        except InvalidParameterException:
            pass
        else:
            raise Exception('build record success with none value of field not allowed null')

        record1 = TupleRecord(schema=record_schema)
        try:
            record1.set_value(0, None)
        except InvalidParameterException:
            pass
        else:
            raise Exception('set record success with none value of field not allowd null')

    def test_put_blob_record_success(self):
        project_name = 'put'
        topic_name = 'success'
        records = []
        data = None
        with open(os.path.join(_TESTS_PATH, '../resources/datahub.png'), 'rb') as f:
            data = f.read()
        record0 = BlobRecord(blob_data=data)
        record0.shard_id = '0'
        records.append(record0)

        record1 = BlobRecord(blob_data=data)
        record1.hash_key = '4FFFFFFFFFFFFFFD7FFFFFFFFFFFFFFD'
        records.append(record1)

        record2 = BlobRecord(blob_data=data)
        record2.partition_key = 'TestPartitionKey'
        records.append(record2)

        with HTTMock(datahub_api_mock):
            put_result = dh.put_records(project_name, topic_name, records)

        assert put_result.failed_record_count == 0
        assert put_result.failed_records == []

    def test_put_blob_record_pb_success(self):
        project_name = 'put'
        topic_name = 'success'
        records = []
        data = None
        with open(os.path.join(_TESTS_PATH, '../resources/datahub.png'), 'rb') as f:
            data = f.read()
        record0 = BlobRecord(blob_data=data)
        record0.shard_id = '0'
        records.append(record0)

        record1 = BlobRecord(blob_data=data)
        record1.hash_key = '4FFFFFFFFFFFFFFD7FFFFFFFFFFFFFFD'
        records.append(record1)

        record2 = BlobRecord(blob_data=data)
        record2.partition_key = 'TestPartitionKey'
        records.append(record2)

        with HTTMock(datahub_pb_api_mock):
            put_result = dh2.put_records(project_name, topic_name, records)

        assert put_result.failed_record_count == 0
        assert put_result.failed_records == []

    def test_put_tuple_record_success(self):
        project_name = 'put'
        topic_name = 'success'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        records = []
        record0 = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, 253402271999000000])
        record0.shard_id = '0'
        record0.shard_id = '0'
        records.append(record0)

        record1 = TupleRecord(schema=record_schema)
        record1.values = [-9223372036854775808, 'yc1', 10.01, True, -62135798400000000]
        record1.hash_key = '4FFFFFFFFFFFFFFD7FFFFFFFFFFFFFFD'
        records.append(record1)

        record2 = TupleRecord(schema=record_schema, values=[9223372036854775807, 'yc1', 10.01, True, 1455869335000000])
        record2.set_value(0, 9223372036854775807)
        record2.set_value('string_field', 'yc1')
        record2.partition_key = 'TestPartitionKey'
        records.append(record2)

        with HTTMock(datahub_api_mock):
            put_result = dh.put_records(project_name, topic_name, records)

        assert put_result.failed_record_count == 0
        assert put_result.failed_records == []

    def test_put_tuple_record_pb_success(self):
        project_name = 'put'
        topic_name = 'success'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        records = []
        record0 = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, 253402271999000000])
        record0.shard_id = '0'
        record0.shard_id = '0'
        records.append(record0)

        record1 = TupleRecord(schema=record_schema,
                              values=[-9223372036854775808, 'yc1', 10.01, True, -62135798400000000])
        record1.hash_key = '4FFFFFFFFFFFFFFD7FFFFFFFFFFFFFFD'
        records.append(record1)

        record2 = TupleRecord(schema=record_schema, values=[9223372036854775807, 'yc1', 10.01, True, 1455869335000000])
        record2.partition_key = 'TestPartitionKey'
        records.append(record2)

        with HTTMock(datahub_pb_api_mock):
            put_result = dh2.put_records(project_name, topic_name, records)

        assert put_result.failed_record_count == 0
        assert put_result.failed_records == []

    def test_put_malformed_tuple_record(self):
        project_name = 'put'
        topic_name = 'malformed'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        record = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
        record.shard_id = '0'
        try:
            with HTTMock(datahub_api_mock):
                put_result = dh.put_records(project_name, topic_name, [record])
        except InvalidParameterException:
            pass
        else:
            raise Exception('put malformed tuple record success!')

    def test_put_data_record_with_invalid_state(self):
        project_name = 'put'
        topic_name = 'invalid_state'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        record = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
        record.shard_id = '0'
        try:
            with HTTMock(datahub_api_mock):
                put_result = dh.put_records(project_name, topic_name, [record])
        except InvalidOperationException:
            pass
        else:
            raise Exception('put data record success with invalid shard state!')

    def test_put_data_record_with_limit_exceeded(self):
        project_name = 'put'
        topic_name = 'limit_exceeded'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        record = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
        record.shard_id = '0'
        try:
            with HTTMock(datahub_api_mock):
                put_result = dh.put_records(project_name, topic_name, [record])
        except LimitExceededException:
            pass
        else:
            raise Exception('put data record success with limit exceeded!')

    def test_put_data_record_with_empty_project_name(self):
        project_name = ''
        topic_name = 'valid'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        record = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
        record.shard_id = '0'
        try:
            put_result = dh.put_records(project_name, topic_name, [record])
        except InvalidParameterException:
            pass
        else:
            raise Exception('put data record success with empty project name!')

    def test_put_data_record_with_empty_topic_name(self):
        project_name = 'valid'
        topic_name = ''
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        record = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
        record.shard_id = '0'
        try:
            put_result = dh.put_records(project_name, topic_name, [record])
        except InvalidParameterException:
            pass
        else:
            raise Exception('put data record success with empty topic name!')

    def test_put_data_record_with_unexisted_project_name(self):
        project_name = 'unexisted'
        topic_name = 'valid'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        record = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
        record.shard_id = '0'
        try:
            with HTTMock(datahub_api_mock):
                put_result = dh.put_records(project_name, topic_name, [record])
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('put data record success with unexisted project name!')

    def test_put_data_record_with_unexisted_topic_name(self):
        project_name = 'valid'
        topic_name = 'unexisted'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        record = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
        record.shard_id = '0'
        try:
            with HTTMock(datahub_api_mock):
                put_result = dh.put_records(project_name, topic_name, [record])
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('put data record success with unexisted topic name!')

    def test_get_blob_record_success(self):
        project_name = 'get'
        topic_name = 'blob'
        shard_id = '0'
        limit_num = 10
        cursor = '20000000000000000000000000fb0021'

        with HTTMock(datahub_api_mock):
            get_result = dh.get_blob_records(project_name, topic_name, shard_id, cursor, limit_num)
        print(get_result)
        print(get_result.records[0])
        assert get_result.next_cursor == '20000000000000000000000000140001'
        assert get_result.record_count == 1
        assert get_result.start_seq == 0
        assert len(get_result.records) == 1
        assert get_result.records[0].system_time == 1526292424292
        assert get_result.records[0].values == 'iVBORw0KGgoAAAANSUhEUgAAB5FrTVeMB4wHjAeMBD3nAgEU'

    def test_get_blob_record_pb_success(self):
        project_name = 'get'
        topic_name = 'blob'
        shard_id = '0'
        limit_num = 10
        cursor = '20000000000000000000000000fb0021'

        with HTTMock(datahub_pb_api_mock):
            get_result = dh2.get_blob_records(project_name, topic_name, shard_id, cursor, limit_num)
        print(get_result)
        print(get_result.records[0])
        assert get_result.next_cursor == '20000000000000000000000000140001'
        assert get_result.record_count == 1
        assert get_result.start_seq == 0
        assert len(get_result.records) == 1
        assert get_result.records[0].system_time == 1527161646886
        assert get_result.records[0].values[:36] == 'iVBORw0KGgoAAAANSUhEUgAABRYAAAJYCAYA'

    def test_get_tuple_record_success(self):
        project_name = 'get'
        topic_name = 'tuple'
        shard_id = '0'
        limit_num = 10
        cursor = '20000000000000000000000000fb0021'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        with HTTMock(datahub_api_mock):
            get_result = dh.get_tuple_records(project_name, topic_name, shard_id, record_schema, cursor, limit_num)
        print(get_result)
        print(get_result.records[0])
        assert get_result.next_cursor == '20000000000000000000000000830010'
        assert get_result.record_count == 1
        assert get_result.start_seq == 0
        assert len(get_result.records) == 1
        assert get_result.records[0].system_time == 1526293795168
        assert get_result.records[0].values == (1, 'yc1', 10.01, True, 1455869335000000)
        assert get_result.records[0].attributes == {"string": "string"}

    def test_get_tuple_record_pb_success(self):
        project_name = 'get'
        topic_name = 'tuple'
        shard_id = '0'
        limit_num = 10
        cursor = '20000000000000000000000000fb0021'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        with HTTMock(datahub_pb_api_mock):
            get_result = dh2.get_tuple_records(project_name, topic_name, shard_id, record_schema, cursor, limit_num)
        print(get_result)
        print(get_result.records[0])
        assert get_result.next_cursor == '200000000000000000000000018c0030'
        assert get_result.record_count == 3
        assert get_result.start_seq == 0
        assert len(get_result.records) == 3
        assert get_result.records[0].system_time == 1527161792134
        assert get_result.records[0].values == (99, 'yc1', 10.01, True, 1455869335000000)
        assert get_result.records[0].attributes == {}

    def test_get_record_with_invalid_cursor(self):
        project_name = 'get'
        topic_name = 'invalid_cursor'
        shard_id = '0'
        limit_num = 10
        cursor = '20000000000000000000000000fb0021'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        try:
            with HTTMock(datahub_api_mock):
                get_result = dh.get_tuple_records(project_name, topic_name, shard_id, record_schema, cursor, limit_num)
        except InvalidParameterException:
            pass
        else:
            raise Exception('get data record success with invalid cursor!')

    def test_get_record_with_empty_project_name(self):
        project_name = ''
        topic_name = 'valid'
        shard_id = '0'
        limit_num = 10
        cursor = '20000000000000000000000000fb0021'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        try:
            get_result = dh.get_tuple_records(project_name, topic_name, shard_id, record_schema, cursor, limit_num)
        except InvalidParameterException:
            pass
        else:
            raise Exception('get data record success with empty project name!')

    def test_get_record_with_empty_topic_name(self):
        project_name = 'valid'
        topic_name = ''
        shard_id = '0'
        limit_num = 10
        cursor = '20000000000000000000000000fb0021'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        try:
            get_result = dh.get_tuple_records(project_name, topic_name, shard_id, record_schema, cursor, limit_num)
        except InvalidParameterException:
            pass
        else:
            raise Exception('get data record success with empty topic name!')

    def test_get_record_with_empty_shard_id(self):
        project_name = 'valid'
        topic_name = 'valid'
        shard_id = ''
        limit_num = 10
        cursor = '20000000000000000000000000fb0021'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        try:
            get_result = dh.get_tuple_records(project_name, topic_name, shard_id, record_schema, cursor, limit_num)
        except InvalidParameterException:
            pass
        else:
            raise Exception('get data record success with empty shard id!')

    def test_get_record_with_unexisted_project_name(self):
        project_name = 'unexisted'
        topic_name = 'valid'
        shard_id = '0'
        limit_num = 10
        cursor = '20000000000000000000000000fb0021'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        try:
            with HTTMock(datahub_api_mock):
                get_result = dh.get_tuple_records(project_name, topic_name, shard_id, record_schema, cursor, limit_num)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('get data record success with unexisted project name!')

    def test_get_record_with_unexisted_topic_name(self):
        project_name = 'valid'
        topic_name = 'unexisted'
        shard_id = '0'
        limit_num = 10
        cursor = '20000000000000000000000000fb0021'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        try:
            with HTTMock(datahub_api_mock):
                get_result = dh.get_tuple_records(project_name, topic_name, shard_id, record_schema, cursor, limit_num)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('get data record success with unexisted topic name!')

    def test_get_record_with_unexisted_shard_id(self):
        project_name = 'valid'
        topic_name = 'valid'
        shard_id = '0'
        limit_num = 10
        cursor = '20000000000000000000000000fb0021'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        try:
            with HTTMock(datahub_api_mock):
                get_result = dh.get_tuple_records(project_name, topic_name, shard_id, record_schema, cursor, limit_num)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('get data record success with unexisted shard id!')

    def test_build_record_with_invalid_value(self):
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        try:
            record = TupleRecord(schema=record_schema, values=['a', 'yc1', 10.01, True, 1455869335000000])
        except InvalidParameterException:
            pass
        else:
            raise Exception('build record success with invalid value!')

        try:
            record = TupleRecord(schema=record_schema,
                                 values=[-9223372036854775809, 'yc1', 10.01, True, 1455869335000000])
        except InvalidParameterException:
            pass
        else:
            raise Exception('build record success with invalid value!')

        try:
            record = TupleRecord(schema=record_schema,
                                 values=[9223372036854775808, 'yc1', 10.01, True, 1455869335000000])
        except InvalidParameterException:
            pass
        else:
            raise Exception('build record success with invalid value!')

        try:
            record = TupleRecord(schema=record_schema, values=['1', 'yc1', 'a', True, 1455869335000000])
        except InvalidParameterException:
            pass
        else:
            raise Exception('build record success with invalid value!')

        try:
            record = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, 2, 1455869335000000])
        except InvalidParameterException:
            pass
        else:
            raise Exception('build record success with invalid value!')

        try:
            record = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, -62135798400000001])
        except InvalidParameterException:
            pass
        else:
            raise Exception('build record success with invalid value!')

        try:
            record = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, -253402271999000001])
        except InvalidParameterException:
            pass
        else:
            raise Exception('build record success with invalid value!')


if __name__ == '__main__':
    test = TestRecord()
    test.test_build_tuple_record_allow_null()
    test.test_put_blob_record_success()
    test.test_put_tuple_record_success()
    test.test_put_malformed_tuple_record()
    test.test_put_data_record_with_invalid_state()
    test.test_put_data_record_with_limit_exceeded()
    test.test_put_data_record_with_empty_project_name()
    test.test_put_data_record_with_empty_topic_name()
    test.test_put_data_record_with_unexisted_project_name()
    test.test_put_data_record_with_unexisted_topic_name()
    test.test_get_blob_record_success()
    test.test_get_tuple_record_success()
    test.test_get_record_with_invalid_cursor()
    test.test_get_record_with_empty_project_name()
    test.test_get_record_with_empty_topic_name()
    test.test_get_record_with_empty_shard_id()
    test.test_get_record_with_unexisted_project_name()
    test.test_get_record_with_unexisted_topic_name()
    test.test_get_record_with_unexisted_shard_id()
    test.test_build_record_with_invalid_value()
    test.test_put_blob_record_pb_success()
    test.test_get_blob_record_pb_success()
    test.test_put_tuple_record_pb_success()
    test.test_get_tuple_record_pb_success()
