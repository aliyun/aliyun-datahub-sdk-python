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
import sys

from datahub.batch.batch_serializer import BatchSerializer
from datahub.batch.schema_registry_client import SchemaRegistryClient
from datahub.batch.utils import SchemaObject

sys.path.append('./')

from httmock import HTTMock

from datahub import DataHub, DatahubProtocolType
from datahub.exceptions import ResourceNotFoundException, InvalidParameterException,\
    LimitExceededException, ShardSealedException, InvalidCursorException
from datahub.models import RecordSchema, FieldType, BlobRecord, TupleRecord, CompressFormat
from datahub.proto.datahub_record_proto_pb import GetRecordsRequest, PutBinaryRecordsRequest
from datahub.utils import unwrap_pb_frame, to_binary
from unittest_util import gen_batch_mock_api, _TESTS_PATH

dh_batch = DataHub('access_id', 'access_key', 'http://endpoint', protocol_type=DatahubProtocolType.BATCH, compress_format=CompressFormat.NONE)
schema_register = SchemaRegistryClient(dh_batch)


class TestRecord:

    def test_put_blob_record_batch_success(self):
        project_name = 'put'
        topic_name = 'success'
        shard_id = '0'
        records = []
        data = []
        with open(os.path.join(_TESTS_PATH, '../resources/datahub.png'), 'rb') as f:
            data.append(f.read())
        record0 = BlobRecord(blob_data=data[0])
        record0.shard_id = '0'
        records.append(record0)

        data.append(b'abc')
        record1 = BlobRecord(blob_data=data[1])
        record1.hash_key = '4FFFFFFFFFFFFFFD7FFFFFFFFFFFFFFD'
        records.append(record1)

        data.append('abc')
        record2 = BlobRecord(blob_data=data[2])
        record2.partition_key = 'TestPartitionKey'
        records.append(record2)

        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/put/topics/success/shards/0'
            crc, compute_crc, pb_str = unwrap_pb_frame(request.body)
            assert crc == compute_crc
            pb_put_record_request = PutBinaryRecordsRequest()
            pb_put_record_request.ParseFromString(pb_str)
            pb_records = pb_put_record_request.records
            pb_byte_record = pb_records[0].data

            schema_object = SchemaObject(project_name, topic_name, None)
            record_list = BatchSerializer.deserialize(None, schema_object, pb_byte_record)
            j = 0
            for record in record_list:
                assert record.blob_data == to_binary(data[j])
                j += 1

        with HTTMock(gen_batch_mock_api(check)):
            dh_batch.put_records_by_shard(project_name, topic_name, shard_id, records)

    def test_put_tuple_record_batch_success(self):
        project_name = 'put'
        topic_name = 'success'
        shard_id = '0'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        records = []
        record0 = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, 253402271999000000])
        record0.shard_id = '0'
        record0.attributes = {"key": "value"}
        records.append(record0)

        record1 = TupleRecord(schema=record_schema,
                              values=[-9223372036854775808, 'yc1', 10.01, True, -62135798400000000])
        record1.hash_key = '4FFFFFFFFFFFFFFD7FFFFFFFFFFFFFFD'
        records.append(record1)

        record2 = TupleRecord(schema=record_schema, values=[9223372036854775807, 'yc1', 10.01, True, 1455869335000000])
        record2.partition_key = 'TestPartitionKey'
        records.append(record2)

        def check(request):
            assert request.method == 'POST'
            if not isinstance(request.body, bytes):
                content = json.loads(request.body)
                assert content['Action'] == 'ListSchema'
                assert request.url == 'http://endpoint/projects/put/topics/success'
            else:
                assert request.url == 'http://endpoint/projects/put/topics/success/shards/0'
                crc, compute_crc, pb_str = unwrap_pb_frame(request.body)
                assert crc == compute_crc
                pb_put_record_request = PutBinaryRecordsRequest()
                pb_put_record_request.ParseFromString(pb_str)
                pb_record_data = pb_put_record_request.records[0].data

                schema_object = SchemaObject(project_name, topic_name, schema_register)
                with HTTMock(gen_batch_mock_api(check)):
                    record_list = BatchSerializer.deserialize(record_schema, schema_object, pb_record_data)
                assert len(record_list) == 3

                assert len(record_list[0].values) == 5
                assert record_list[0].values[0] == 1
                assert record_list[0].values[1] == 'yc1'
                assert record_list[0].values[2] == 10.01
                assert record_list[0].values[3] == True
                assert record_list[0].values[4] == 253402271999000000

                assert len(record_list[1].values) == 5
                assert record_list[1].values[0] == -9223372036854775808
                assert record_list[1].values[1] == 'yc1'
                assert record_list[1].values[2] == 10.01
                assert record_list[1].values[3] == True
                assert record_list[1].values[4] == -62135798400000000

                assert len(record_list[2].values) == 5
                assert record_list[2].values[0] == 9223372036854775807
                assert record_list[2].values[1] == 'yc1'
                assert record_list[2].values[2] == 10.01
                assert record_list[2].values[3] == True
                assert record_list[2].values[4] == 1455869335000000

        with HTTMock(gen_batch_mock_api(check)):
            dh_batch.put_records_by_shard(project_name, topic_name, shard_id, records)

    def test_put_record_batch_with_malformed_record(self):
        project_name = 'put'
        topic_name = 'malformed_batch'
        shard_id = '0'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        record = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
        record.shard_id = '0'
        try:
            def check(request):
                assert request.method == 'POST'
                if not isinstance(request.body, bytes):
                    content = json.loads(request.body)
                    assert content['Action'] == 'ListSchema'
                    assert request.url == 'http://endpoint/projects/put/topics/malformed_batch'
                else:
                    assert request.url == 'http://endpoint/projects/put/topics/malformed_batch/shards/0'
                    crc, compute_crc, pb_str = unwrap_pb_frame(request.body)
                    assert crc == compute_crc
                    pb_put_record_request = PutBinaryRecordsRequest()
                    pb_put_record_request.ParseFromString(pb_str)
                    pb_record_data = pb_put_record_request.records[0].data

                    schema_object = SchemaObject(project_name, topic_name, schema_register)
                    with HTTMock(gen_batch_mock_api(check)):
                        record_list = BatchSerializer.deserialize(record_schema, schema_object, pb_record_data)
                    assert len(record_list) == 1
                    assert len(record_list[0].values) == 5
                    assert record_list[0].values[0] == 1
                    assert record_list[0].values[1] == 'yc1'
                    assert record_list[0].values[2] == 10.01
                    assert record_list[0].values[3] == True
                    assert record_list[0].values[4] == 1455869335000000

            with HTTMock(gen_batch_mock_api(check)):
                dh_batch.put_records_by_shard(project_name, topic_name, shard_id, [record])
        except InvalidParameterException:
            pass
        else:
            raise Exception('put data record success with malformed record!')

    def test_put_record_batch_with_invalid_state(self):
        project_name = 'put'
        topic_name = 'invalid_state_batch'
        shard_id = '0'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        record = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
        record.shard_id = '0'
        try:
            def check(request):
                assert request.method == 'POST'
                if not isinstance(request.body, bytes):
                    content = json.loads(request.body)
                    assert content['Action'] == 'ListSchema'
                    assert request.url == 'http://endpoint/projects/put/topics/invalid_state_batch'
                else:
                    assert request.url == 'http://endpoint/projects/put/topics/invalid_state_batch/shards/0'
                    crc, compute_crc, pb_str = unwrap_pb_frame(request.body)
                    assert crc == compute_crc
                    pb_put_record_request = PutBinaryRecordsRequest()
                    pb_put_record_request.ParseFromString(pb_str)
                    pb_record_data = pb_put_record_request.records[0].data

                    schema_object = SchemaObject(project_name, topic_name, schema_register)
                    with HTTMock(gen_batch_mock_api(check)):
                        record_list = BatchSerializer.deserialize(record_schema, schema_object, pb_record_data)
                    assert len(record_list) == 1
                    assert len(record_list[0].values) == 5
                    assert record_list[0].values[0] == 1
                    assert record_list[0].values[1] == 'yc1'
                    assert record_list[0].values[2] == 10.01
                    assert record_list[0].values[3] == True
                    assert record_list[0].values[4] == 1455869335000000

            with HTTMock(gen_batch_mock_api(check)):
                dh_batch.put_records_by_shard(project_name, topic_name, shard_id, [record])
        except ShardSealedException:
            pass
        else:
            raise Exception('put data record success with invalid state!')

    def test_put_record_batch_with_limit_exceeded(self):
        project_name = 'put'
        topic_name = 'limit_exceeded_batch'
        shard_id = '0'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        record = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
        record.shard_id = '0'
        try:
            def check(request):
                assert request.method == 'POST'
                if not isinstance(request.body, bytes):
                    content = json.loads(request.body)
                    assert content['Action'] == 'ListSchema'
                    assert request.url == 'http://endpoint/projects/put/topics/limit_exceeded_batch'
                else:
                    assert request.url == 'http://endpoint/projects/put/topics/limit_exceeded_batch/shards/0'
                    crc, compute_crc, pb_str = unwrap_pb_frame(request.body)
                    assert crc == compute_crc
                    pb_put_record_request = PutBinaryRecordsRequest()
                    pb_put_record_request.ParseFromString(pb_str)
                    pb_record_data = pb_put_record_request.records[0].data

                    schema_object = SchemaObject(project_name, topic_name, schema_register)
                    with HTTMock(gen_batch_mock_api(check)):
                        record_list = BatchSerializer.deserialize(record_schema, schema_object, pb_record_data)
                    assert len(record_list) == 1
                    assert len(record_list[0].values) == 5
                    assert record_list[0].values[0] == 1
                    assert record_list[0].values[1] == 'yc1'
                    assert record_list[0].values[2] == 10.01
                    assert record_list[0].values[3] == True
                    assert record_list[0].values[4] == 1455869335000000

            with HTTMock(gen_batch_mock_api(check)):
                dh_batch.put_records_by_shard(project_name, topic_name, shard_id, [record])
        except LimitExceededException:
            pass
        else:
            raise Exception('put data record success with limit exceeded!')

    def test_put_record_batch_with_empty_project_name(self):
        project_name = ''
        topic_name = 'success'
        shard_id = '0'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        record = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
        record.shard_id = '0'
        try:
            dh_batch.put_records_by_shard(project_name, topic_name, shard_id, [record])
        except InvalidParameterException as e:
            pass
        else:
            raise Exception('put data record success with empty project name!')

    def test_put_record_batch_with_empty_topic_name(self):
        project_name = 'success'
        topic_name = ''
        shard_id = '0'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        record = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
        record.shard_id = '0'
        try:
            dh_batch.put_records_by_shard(project_name, topic_name, shard_id, [record])
        except InvalidParameterException as e:
            pass
        else:
            raise Exception('put data record success with empty project name!')

    def test_put_record_batch_with_unexisted_project_name(self):
        project_name = 'unexisted'
        topic_name = 'valid_batch'
        shard_id = '0'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        record = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
        record.shard_id = '0'
        try:
            def check(request):
                assert request.method == 'POST'
                if not isinstance(request.body, bytes):
                    content = json.loads(request.body)
                    assert content['Action'] == 'ListSchema'
                    assert request.url == 'http://endpoint/projects/unexisted/topics/valid_batch'
                else:
                    assert request.url == 'http://endpoint/projects/unexisted/topics/valid_batch/shards/0'
                    crc, compute_crc, pb_str = unwrap_pb_frame(request.body)
                    assert crc == compute_crc
                    pb_put_record_request = PutBinaryRecordsRequest()
                    pb_put_record_request.ParseFromString(pb_str)
                    pb_record_data = pb_put_record_request.records[0].data

                    schema_object = SchemaObject(project_name, topic_name, schema_register)
                    with HTTMock(gen_batch_mock_api(check)):
                        record_list = BatchSerializer.deserialize(record_schema, schema_object, pb_record_data)
                    assert len(record_list) == 1
                    assert len(record_list[0].values) == 5
                    assert record_list[0].values[0] == 1
                    assert record_list[0].values[1] == 'yc1'
                    assert record_list[0].values[2] == 10.01
                    assert record_list[0].values[3] == True
                    assert record_list[0].values[4] == 1455869335000000

            with HTTMock(gen_batch_mock_api(check)):
                dh_batch.put_records_by_shard(project_name, topic_name, shard_id, [record])
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('put data record success with unexisted project name!')

    def test_put_record_batch_with_unexisted_topic_name(self):
        project_name = 'valid_batch'
        topic_name = 'unexisted'
        shard_id = '0'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        record = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
        record.shard_id = '0'
        try:
            def check(request):
                assert request.method == 'POST'
                if not isinstance(request.body, bytes):
                    content = json.loads(request.body)
                    assert content['Action'] == 'ListSchema'
                    assert request.url == 'http://endpoint/projects/valid_batch/topics/unexisted'
                else:
                    assert request.url == 'http://endpoint/projects/valid_batch/topics/unexisted/shards/0'
                    crc, compute_crc, pb_str = unwrap_pb_frame(request.body)
                    assert crc == compute_crc
                    pb_put_record_request = PutBinaryRecordsRequest()
                    pb_put_record_request.ParseFromString(pb_str)
                    pb_record_data = pb_put_record_request.records[0].data

                    schema_object = SchemaObject(project_name, topic_name, schema_register)
                    with HTTMock(gen_batch_mock_api(check)):
                        record_list = BatchSerializer.deserialize(record_schema, schema_object, pb_record_data)
                    assert len(record_list) == 1
                    assert len(record_list[0].values) == 5
                    assert record_list[0].values[0] == 1
                    assert record_list[0].values[1] == 'yc1'
                    assert record_list[0].values[2] == 10.01
                    assert record_list[0].values[3] == True
                    assert record_list[0].values[4] == 1455869335000000

            with HTTMock(gen_batch_mock_api(check)):
                dh_batch.put_records_by_shard(project_name, topic_name, shard_id, [record])
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('put data record success with unexisted topic name!')

    def test_get_blob_record_batch_success(self):
        project_name = 'get'
        topic_name = 'blob_batch'
        shard_id = '0'
        limit_num = 10
        cursor = '20000000000000000000000000fb0021'

        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/get/topics/blob_batch/shards/0'
            crc, compute_crc, pb_str = unwrap_pb_frame(request.body)
            assert crc == compute_crc
            pb_get_record_request = GetRecordsRequest()
            pb_get_record_request.ParseFromString(pb_str)
            assert pb_get_record_request.cursor == '20000000000000000000000000fb0021'
            assert pb_get_record_request.limit == 10

        with HTTMock(gen_batch_mock_api(check)):
            get_result = dh_batch.get_blob_records(project_name, topic_name, shard_id, cursor, limit_num)

        print("===== get blob record =====")
        print(get_result)
        print(get_result.records[0])

        assert get_result.next_cursor == '300062f4901900000000000000080001'
        assert get_result.record_count == 3
        assert get_result.start_seq == 8
        assert len(get_result.records) == 3
        assert get_result.records[0].system_time == 1660194841312
        assert get_result.records[0].blob_data == b'datatest-0'
        assert get_result.records[1].system_time == 1660194841312
        assert get_result.records[1].blob_data == b'datatest-1'
        assert get_result.records[2].system_time == 1660194841312
        assert get_result.records[2].blob_data == b'datatest-2'

    def test_get_tuple_record_batch_success(self):
        project_name = 'get'
        topic_name = 'tuple_batch'
        shard_id = '0'
        limit_num = 10
        cursor = '20000000000000000000000000fb0021'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        def check(request):
            assert request.method == 'POST'
            if not isinstance(request.body, bytes):
                content = json.loads(request.body)
                assert content['Action'] == 'ListSchema'
                assert request.url == 'http://endpoint/projects/get/topics/tuple_batch'
            else:
                assert request.url == 'http://endpoint/projects/get/topics/tuple_batch/shards/0'
                crc, compute_crc, pb_str = unwrap_pb_frame(request.body)
                assert crc == compute_crc
                pb_get_record_request = GetRecordsRequest()
                pb_get_record_request.ParseFromString(pb_str)
                assert pb_get_record_request.cursor == '20000000000000000000000000fb0021'
                assert pb_get_record_request.limit == 10

        with HTTMock(gen_batch_mock_api(check)):
            get_result = dh_batch.get_tuple_records(project_name, topic_name, shard_id, record_schema, cursor, limit_num)
        print("===== get tuple record =====")
        print(get_result)
        print(get_result.records[0])

        assert get_result.next_cursor == '300062f5b8bd00000000000000160001'
        assert get_result.record_count == 3
        assert get_result.start_seq == 22
        assert len(get_result.records) == 3
        assert get_result.records[0].system_time == 1660270781252
        assert get_result.records[0].values == (1, 'yc1', 10.01, True, 253402271999000000)
        assert get_result.records[0].attributes == {"key": "value"}

        assert get_result.records[1].system_time == 1660270781252
        assert get_result.records[1].values == (-9223372036854775808, 'yc1', 10.01, True, -62135798400000000)

        assert get_result.records[2].system_time == 1660270781252
        assert get_result.records[2].values == (9223372036854775807, 'yc1', 10.01, True, 1455869335000000)

    def test_get_record_with_invalid_cursor(self):
        project_name = 'get'
        topic_name = 'invalid_cursor_batch'
        shard_id = '0'
        limit_num = 10
        cursor = '20000000000000000000000000fb0021'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        try:
            def check(request):
                assert request.method == 'POST'
                if not isinstance(request.body, bytes):
                    content = json.loads(request.body)
                    assert content['Action'] == 'ListSchema'
                    assert request.url == 'http://endpoint/projects/get/topics/invalid_cursor_batch'
                else:
                    assert request.url == 'http://endpoint/projects/get/topics/invalid_cursor_batch/shards/0'
                    crc, compute_crc, pb_str = unwrap_pb_frame(request.body)
                    assert crc == compute_crc
                    pb_get_record_request = GetRecordsRequest()
                    pb_get_record_request.ParseFromString(pb_str)
                    assert pb_get_record_request.cursor == '20000000000000000000000000fb0021'
                    assert pb_get_record_request.limit == 10

            with HTTMock(gen_batch_mock_api(check)):
                get_result = dh_batch.get_tuple_records(project_name, topic_name, shard_id, record_schema, cursor,
                                                        limit_num)
        except InvalidCursorException:
            pass
        else:
            raise Exception('get data record success with invalid cursor!')

    def test_get_record_with_empty_project_name(self):
        project_name = ''
        topic_name = 'valid_batch'
        shard_id = '0'
        limit_num = 10
        cursor = '20000000000000000000000000fb0021'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        try:
            dh_batch.get_tuple_records(project_name, topic_name, shard_id, record_schema, cursor, limit_num)
        except InvalidParameterException:
            pass
        else:
            raise Exception('get data record success with empty project name!')

    def test_get_record_with_empty_topic_name(self):
        project_name = 'valid_batch'
        topic_name = ''
        shard_id = '0'
        limit_num = 10
        cursor = '20000000000000000000000000fb0021'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        try:
            dh_batch.get_tuple_records(project_name, topic_name, shard_id, record_schema, cursor, limit_num)
        except InvalidParameterException:
            pass
        else:
            raise Exception('get data record success with empty topic name!')

    def test_get_record_with_empty_shard_id(self):
        project_name = 'valid_batch'
        topic_name = 'valid_batch'
        shard_id = ''
        limit_num = 10
        cursor = '20000000000000000000000000fb0021'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        try:
            dh_batch.get_tuple_records(project_name, topic_name, shard_id, record_schema, cursor, limit_num)
        except InvalidParameterException:
            pass
        else:
            raise Exception('get data record success with empty shard id!')

    def test_get_record_with_unexisted_project_name(self):
        project_name = 'unexisted'
        topic_name = 'valid_batch'
        shard_id = '0'
        limit_num = 10
        cursor = '20000000000000000000000000fb0021'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        try:
            def check(request):
                assert request.method == 'POST'
                if not isinstance(request.body, bytes):
                    content = json.loads(request.body)
                    assert content['Action'] == 'ListSchema'
                    assert request.url == 'http://endpoint/projects/unexisted/topics/valid_batch'
                else:
                    assert request.url == 'http://endpoint/projects/unexisted/topics/valid_batch/shards/0'
                    crc, compute_crc, pb_str = unwrap_pb_frame(request.body)
                    assert crc == compute_crc
                    pb_get_record_request = GetRecordsRequest()
                    pb_get_record_request.ParseFromString(pb_str)
                    assert pb_get_record_request.cursor == '20000000000000000000000000fb0021'
                    assert pb_get_record_request.limit == 10

            with HTTMock(gen_batch_mock_api(check)):
                get_result = dh_batch.get_tuple_records(project_name, topic_name, shard_id, record_schema, cursor,
                                                        limit_num)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('get data record success with unexisted project name!')

    def test_get_record_with_unexisted_topic_name(self):
        project_name = 'valid_batch'
        topic_name = 'unexisted'
        shard_id = '0'
        limit_num = 10
        cursor = '20000000000000000000000000fb0021'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        try:
            def check(request):
                assert request.method == 'POST'
                if not isinstance(request.body, bytes):
                    content = json.loads(request.body)
                    assert content['Action'] == 'ListSchema'
                    assert request.url == 'http://endpoint/projects/valid_batch/topics/unexisted'
                else:
                    assert request.url == 'http://endpoint/projects/valid_batch/topics/unexisted/shards/0'
                    crc, compute_crc, pb_str = unwrap_pb_frame(request.body)
                    assert crc == compute_crc
                    pb_get_record_request = GetRecordsRequest()
                    pb_get_record_request.ParseFromString(pb_str)
                    assert pb_get_record_request.cursor == '20000000000000000000000000fb0021'
                    assert pb_get_record_request.limit == 10

            with HTTMock(gen_batch_mock_api(check)):
                get_result = dh_batch.get_tuple_records(project_name, topic_name, shard_id, record_schema, cursor,
                                                        limit_num)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('get data record success with unexisted topic name!')

    def test_get_record_with_unexisted_shard_id(self):
        project_name = 'valid_batch'
        topic_name = 'valid_batch'
        shard_id = '0'
        limit_num = 10
        cursor = '20000000000000000000000000fb0021'
        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

        try:
            def check(request):
                assert request.method == 'POST'
                if not isinstance(request.body, bytes):
                    content = json.loads(request.body)
                    assert content['Action'] == 'ListSchema'
                    assert request.url == 'http://endpoint/projects/valid_batch/topics/valid_batch'
                else:
                    assert request.url == 'http://endpoint/projects/valid_batch/topics/valid_batch/shards/0'
                    crc, compute_crc, pb_str = unwrap_pb_frame(request.body)
                    assert crc == compute_crc
                    pb_get_record_request = GetRecordsRequest()
                    pb_get_record_request.ParseFromString(pb_str)
                    assert pb_get_record_request.cursor == '20000000000000000000000000fb0021'
                    assert pb_get_record_request.limit == 10

            with HTTMock(gen_batch_mock_api(check)):
                get_result = dh_batch.get_tuple_records(project_name, topic_name, shard_id, record_schema, cursor,
                                                        limit_num)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('get data record success with unexisted shard id!')

if __name__ == '__main__':
    test = TestRecord()

    test.test_put_blob_record_batch_success()
    test.test_get_blob_record_batch_success()
    test.test_put_tuple_record_batch_success()
    test.test_get_tuple_record_batch_success()

    test.test_put_record_batch_with_malformed_record()
    test.test_put_record_batch_with_invalid_state()
    test.test_put_record_batch_with_limit_exceeded()
    test.test_put_record_batch_with_empty_project_name()
    test.test_put_record_batch_with_empty_topic_name()
    test.test_put_record_batch_with_unexisted_project_name()
    test.test_put_record_batch_with_unexisted_topic_name()

    test.test_get_record_with_invalid_cursor()
    test.test_get_record_with_empty_project_name()
    test.test_get_record_with_empty_topic_name()
    test.test_get_record_with_empty_shard_id()
    test.test_get_record_with_unexisted_project_name()
    test.test_get_record_with_unexisted_topic_name()
    test.test_get_record_with_unexisted_shard_id()
