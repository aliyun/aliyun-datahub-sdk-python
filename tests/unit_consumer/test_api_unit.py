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
from httmock import HTTMock

from datahub import DataHub, DatahubProtocolType
from datahub.models import ShardState, CursorType, RecordSchema, FieldType, RecordType, OffsetBase, CompressFormat
from datahub.models.compress import get_compressor
from datahub.rest import Headers
from datahub.utils import to_binary
from unit_consumer.unittest_util import gen_consumer_api

dh       = DataHub('access_id', 'access_key', 'http://endpoint',
                   protocol_type=DatahubProtocolType.JSON, compress_format=CompressFormat.NONE)

class TestConsumer:

    def test_get_topic_success(self):       # In TestTopic
        project_name = 'success'
        topic_name = 'get'

        def check(request):
            assert request.method == 'GET'
            assert request.url == 'http://endpoint/projects/success/topics/get'

        with HTTMock(gen_consumer_api(check)):
            result = dh.get_topic(project_name, topic_name)
        assert result.project_name == project_name
        assert result.topic_name == topic_name
        assert result.comment == 'consumer test'
        assert result.shard_count == 3
        assert result.life_cycle == 7
        assert result.record_type == RecordType.BLOB
        assert result.create_time == 1525344044
        assert result.last_modify_time == 1525344044

    def test_list_shard_success(self):      # In TestShard
        project_name = 'success'
        topic_name = 'list'

        def check(request):
            assert request.method == 'GET'
            assert request.url == 'http://endpoint/projects/success/topics/list/shards'

        with HTTMock(gen_consumer_api(check)):
            result = dh.list_shard(project_name, topic_name)
        shard_list = result.shards
        assert len(shard_list) == 3
        assert shard_list[0].shard_id == '0'
        assert shard_list[0].begin_hash_key == '00000000000000000000000000000000'
        assert shard_list[0].end_hash_key == '55555555555555555555555555555555'
        assert shard_list[0].left_shard_id == '4294967295'
        assert shard_list[0].right_shard_id == '1'
        assert shard_list[0].closed_time == ''
        assert shard_list[0].state == ShardState.ACTIVE
        assert shard_list[1].state == ShardState.ACTIVE
        assert shard_list[2].state == ShardState.ACTIVE
        assert shard_list[0].parent_shard_ids == ['10', '11']
        assert shard_list[1].parent_shard_ids == []
        assert shard_list[2].parent_shard_ids == []

    def test_join_group_success(self):
        project_name = 'success'
        topic_name = 'join'
        consumer_group = '166123931038000XXX'
        session_timeout = 60000

        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/success/topics/join/subscriptions/166123931038000XXX'
            content = json.loads(request.body)
            assert content['Action'] == 'joinGroup'
            assert content['SessionTimeout'] == 60000

        with HTTMock(gen_consumer_api(check)):
            result = dh.join_group(project_name, topic_name, consumer_group, session_timeout)
        assert result.consumer_id == '1661239310475U2M3J-5555abcd-6666abcd-111-222'
        assert result.version_id == 0
        assert result.session_timeout == 60000

    def test_heartbeat_success(self):
        project_name = 'success'
        topic_name = 'heartbeat'
        consumer_group = '166123931038000XXX'
        consumer_id = '1661239310475U2M3J-5555abcd-6666abcd-111-222'
        version_id = 0
        hold_shard_list = ['0']
        read_end_list = []

        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/success/topics/heartbeat/subscriptions/166123931038000XXX'
            content = json.loads(request.body)
            assert content['Action'] == 'heartBeat'
            assert content['ConsumerId'] == '1661239310475U2M3J-5555abcd-6666abcd-111-222'
            assert content['VersionId'] == 0
            assert content['HoldShardList'] == ['0']
            assert content['ReadEndShardList'] == []


        with HTTMock(gen_consumer_api(check)):
            result = dh.heart_beat(project_name, topic_name, consumer_group, consumer_id, version_id,
                                   hold_shard_list, read_end_list)
        assert result.plan_version == 0
        assert result.shard_list == ['0']
        assert result.total_plan == ''

    def test_sync_group_success(self):
        project_name = 'success'
        topic_name = 'sync'
        consumer_group = '166123931038000XXX'
        consumer_id = '1661239310475U2M3J-5555abcd-6666abcd-111-222'
        version_id = 0
        release_shard_list = ['0']
        read_end_list = []

        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/success/topics/sync/subscriptions/166123931038000XXX'
            content = json.loads(request.body)
            assert content['Action'] == 'syncGroup'
            assert content['ConsumerId'] == '1661239310475U2M3J-5555abcd-6666abcd-111-222'
            assert content['VersionId'] == 0
            assert content['ReleaseShardList'] == ['0']
            assert content['ReadEndShardList'] == []


        with HTTMock(gen_consumer_api(check)):
            result = dh.sync_group(project_name, topic_name, consumer_group, consumer_id, version_id,
                                   release_shard_list, read_end_list)

    def test_leave_group_success(self):
        project_name = 'success'
        topic_name = 'leave'
        consumer_group = '166123931038000XXX'
        consumer_id = '1661239310475U2M3J-5555abcd-6666abcd-111-222'
        version_id = 0

        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/success/topics/leave/subscriptions/166123931038000XXX'
            content = json.loads(request.body)
            assert content['Action'] == 'leaveGroup'
            assert content['ConsumerId'] == '1661239310475U2M3J-5555abcd-6666abcd-111-222'
            assert content['VersionId'] == 0


        with HTTMock(gen_consumer_api(check)):
            result = dh.leave_group(project_name, topic_name, consumer_group, consumer_id, version_id)

    def test_get_cursor_success(self):      # In TestCursor
        project_name = 'success'
        topic_name = 'cursor'
        shard_id = '0'

        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/success/topics/cursor/shards/0'
            content = json.loads(request.body)
            assert content['Action'] == 'cursor'
            assert content['Type'] == 'OLDEST'

        with HTTMock(gen_consumer_api(check)):
            cursor_oldest = dh.get_cursor(project_name, topic_name, shard_id, CursorType.OLDEST)

        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/success/topics/cursor/shards/0'
            content = json.loads(request.body)
            assert content['Action'] == 'cursor'
            assert content['Type'] == 'LATEST'

        with HTTMock(gen_consumer_api(check)):
            cursor_latest = dh.get_cursor(project_name, topic_name, shard_id, CursorType.LATEST)

        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/success/topics/cursor/shards/0'
            content = json.loads(request.body)
            assert content['Action'] == 'cursor'
            assert content['Type'] == 'SEQUENCE'
            assert content['Sequence'] == 0

        with HTTMock(gen_consumer_api(check)):
            cursor_sequence = dh.get_cursor(project_name, topic_name, shard_id, CursorType.SEQUENCE, 0)

        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/success/topics/cursor/shards/0'
            content = json.loads(request.body)
            assert content['Action'] == 'cursor'
            assert content['Type'] == 'SYSTEM_TIME'
            assert content['SystemTime'] == 0

        with HTTMock(gen_consumer_api(check)):
            cursor_system_time = dh.get_cursor(project_name, topic_name, shard_id, CursorType.SYSTEM_TIME, 0)

        assert cursor_oldest.cursor == '20000000000000000000000000000000'
        assert cursor_oldest.sequence == 0
        assert cursor_oldest.record_time == 0

        assert cursor_latest.cursor == '20000000000000000000000000000000'
        assert cursor_latest.sequence == 0
        assert cursor_latest.record_time == 0

        assert cursor_sequence.cursor == '20000000000000000000000000000000'
        assert cursor_sequence.sequence == 0
        assert cursor_sequence.record_time == 0

        assert cursor_system_time.cursor == '20000000000000000000000000000000'
        assert cursor_system_time.sequence == 0
        assert cursor_system_time.record_time == 0

    def test_list_topic_schema_success(self):
        project_name = 'success'
        topic_name = 'schema'

        record_schema = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP],
        )

        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/success/topics/schema'
            content = json.loads(request.body)
            assert content['Action'] == 'ListSchema'
            assert content['PageNumber'] == -1
            assert content['PageSize'] == -1

        with HTTMock(gen_consumer_api(check)):
            result = dh.list_topic_schema(project_name, topic_name, page_number=-1, page_size=-1)
        assert result.page_count == 1
        assert result.page_number == 1
        assert result.page_size == 1
        assert result.total_count == 1
        assert result.record_schema_list[0]["RecordSchema"] == record_schema.to_json_string()

    def test_get_record_success(self):
        project_name = 'success'
        topic_name = 'get'
        shard_id = '0'
        limit_num = 10
        cursor = '20000000000000000000000000fb0021'

        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/success/topics/get/shards/0'
            content = self.decompress_request(request)
            assert content['Limit'] == 10
            assert content['Action'] == 'sub'
            assert content['Cursor'] == '20000000000000000000000000fb0021'

        with HTTMock(gen_consumer_api(check)):
            result = dh.get_blob_records(project_name, topic_name, shard_id, cursor, limit_num)
        assert result.next_cursor == '20000000000000000000000000140001'
        assert result.record_count == 1
        assert result.start_seq == 0
        assert len(result.records) == 1
        assert result.records[0].system_time == 1526292424292
        assert result.records[0].values == 'iVBORw0KGgoAAAANSUhEUgAAB5FrTVeMB4wHjAeMBD3nAgEU'

    def test_init_and_get_subscription_offset_success(self):
        project_name = 'success'
        topic_name = 'init'
        sub_id = '166123931038000XXX'
        shard_ids = ['0']

        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/success/topics/init/subscriptions/166123931038000XXX/offsets'
            content = json.loads(request.body)
            assert content['Action'] == 'open'
            assert content['ShardIds'] == ['0']

        with HTTMock(gen_consumer_api(check)):
            result = dh.init_and_get_subscription_offset(project_name, topic_name, sub_id, shard_ids)
        offsets = result.offsets
        assert len(offsets) == 1
        assert offsets['0'].sequence == -1
        assert offsets['0'].timestamp == -1
        assert offsets['0'].version == 0
        assert offsets['0'].session_id == 0
        assert offsets['0'].batch_index == 0

    def test_update_subscription_offset_success(self):
        project_name = 'success'
        topic_name = 'update'
        sub_id = '166123931038000XXX'
        shard_ids = {'0': OffsetBase(-1, -1)}

        def check(request):
            assert request.method == 'PUT'
            assert request.url == 'http://endpoint/projects/success/topics/update/subscriptions/166123931038000XXX/offsets'
            content = json.loads(request.body)
            assert content['Action'] == 'commit'
            assert len(content['Offsets']) == 1
            assert len(content['Offsets'].get('0')) == 2
            assert content['Offsets'].get('0').get('Sequence') == -1
            assert content['Offsets'].get('0').get('Timestamp') == -1

        with HTTMock(gen_consumer_api(check)):
            result = dh.update_subscription_offset(project_name, topic_name, sub_id, shard_ids)

    def decompress_request(self, request):
        raw_size = int(request.headers.get(Headers.RAW_SIZE, '0'))
        compress = request.headers.get(Headers.CONTENT_ENCODING, '')
        compressor = get_compressor(compress)
        return json.loads(compressor.decompress(to_binary(request.body), raw_size))


if __name__ == "__main__":
    test = TestConsumer()

    test.test_get_topic_success()
    test.test_list_shard_success()
    test.test_get_cursor_success()
    test.test_list_topic_schema_success()
    test.test_get_record_success()
    test.test_init_and_get_subscription_offset_success()
    test.test_update_subscription_offset_success()

    test.test_join_group_success()
    test.test_heartbeat_success()
    test.test_sync_group_success()
    test.test_leave_group_success()
