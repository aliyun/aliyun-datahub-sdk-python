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

sys.path.append('./')
from httmock import HTTMock

from datahub import DataHub
from datahub.exceptions import ResourceNotFoundException, InvalidOperationException, InvalidParameterException, \
    LimitExceededException, DatahubException
from datahub.models import ShardState
from .unittest_util import gen_mock_api

dh = DataHub('access_id', 'access_key', 'http://endpoint')


class TestShard:

    def test_wait_shards_ready_success(self):
        project_name = 'wait'
        topic_name = 'ready'

        def check(request):
            assert request.method == 'GET'
            assert request.url == 'http://endpoint/projects/wait/topics/ready/shards'

        with HTTMock(gen_mock_api(check)):
            dh.wait_shards_ready(project_name, topic_name)

    def test_wait_shards_ready_timeout(self):
        project_name = 'wait'
        topic_name = 'unready'
        try:
            def check(request):
                assert request.method == 'GET'
                assert request.url == 'http://endpoint/projects/wait/topics/unready/shards'

            with HTTMock(gen_mock_api(check)):
                dh.wait_shards_ready(project_name, topic_name, 1)
        except DatahubException:
            pass
        else:
            raise Exception('wait shards ready success with unready shard status!')

    def test_wait_shards_ready_with_invalid_timeout(self):
        project_name = 'wait'
        topic_name = 'ready'
        try:
            dh.wait_shards_ready(project_name, topic_name, -12)
        except InvalidParameterException:
            pass
        else:
            raise Exception('wait shards ready success with invalid timeout!')

    def test_wait_shards_ready_with_empty_project_name(self):
        project_name = ''
        topic_name = 'valid'
        try:
            dh.wait_shards_ready(project_name, topic_name)
        except InvalidParameterException:
            pass
        else:
            raise Exception('wait shards ready success with empty project name!')

    def test_wait_shards_ready_with_empty_topic_name(self):
        project_name = 'valid'
        topic_name = ''
        try:
            dh.wait_shards_ready(project_name, topic_name)
        except InvalidParameterException:
            pass
        else:
            raise Exception('wait shards ready success with empty topic name!')

    def test_wait_shards_ready_with_unexisted_project_name(self):
        project_name = 'unexisted'
        topic_name = 'valid'
        try:
            def check(request):
                assert request.method == 'GET'
                assert request.url == 'http://endpoint/projects/unexisted/topics/valid/shards'

            with HTTMock(gen_mock_api(check)):
                dh.wait_shards_ready(project_name, topic_name)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('wait shards ready success with unexisted project name!')

    def test_wait_shards_ready_with_unexisted_topic_name(self):
        project_name = 'valid'
        topic_name = 'unexisted'
        try:
            def check(request):
                assert request.method == 'GET'
                assert request.url == 'http://endpoint/projects/valid/topics/unexisted/shards'

            with HTTMock(gen_mock_api(check)):
                dh.wait_shards_ready(project_name, topic_name)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('wait shards ready success with unexisted topic name!')

    def test_list_shard_success(self):
        project_name = 'success'
        topic_name = 'success'

        def check(request):
            assert request.method == 'GET'
            assert request.url == 'http://endpoint/projects/success/topics/success/shards'

        with HTTMock(gen_mock_api(check)):
            result = dh.list_shard(project_name, topic_name)
        print(result)
        shard_list = result.shards
        assert len(shard_list) == 4
        assert shard_list[0].shard_id == '0'
        assert shard_list[0].begin_hash_key == '00000000000000000000000000000000'
        assert shard_list[0].end_hash_key == '55555555555555555555555555555555'
        assert shard_list[0].left_shard_id == '4294967295'
        assert shard_list[0].right_shard_id == '1'
        assert shard_list[0].closed_time == ''
        assert shard_list[0].state == ShardState.ACTIVE
        assert shard_list[1].state == ShardState.CLOSED
        assert shard_list[2].state == ShardState.OPENING
        assert shard_list[3].state == ShardState.CLOSING
        assert shard_list[0].parent_shard_ids == ['10', '11']
        assert shard_list[1].parent_shard_ids == []

    def test_list_shard_with_empty_project_name(self):
        project_name = ''
        topic_name = 'valid'

        try:
            dh.list_shard(project_name, topic_name)
        except InvalidParameterException:
            pass
        else:
            raise Exception('list shard success with empty project name!')

    def test_list_shard_with_empty_topic_name(self):
        project_name = 'valid'
        topic_name = ''

        try:
            dh.list_shard(project_name, topic_name)
        except InvalidParameterException:
            pass
        else:
            raise Exception('list shard success with empty topic name!')

    def test_list_shard_with_unexisted_project_name(self):
        project_name = 'unexisted'
        topic_name = 'valid'

        try:
            def check(request):
                assert request.method == 'GET'
                assert request.url == 'http://endpoint/projects/unexisted/topics/valid/shards'

            with HTTMock(gen_mock_api(check)):
                dh.list_shard(project_name, topic_name)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('list shard success with unexisted project name!')

    def test_list_shard_with_unexisted_topic_name(self):
        project_name = 'valid'
        topic_name = 'unexisted'

        try:
            def check(request):
                assert request.method == 'GET'
                assert request.url == 'http://endpoint/projects/valid/topics/unexisted/shards'

            with HTTMock(gen_mock_api(check)):
                dh.list_shard(project_name, topic_name)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('list shard success with unexisted topic name!')

    def test_split_shard_success(self):
        project_name = 'split'
        topic_name = 'success'
        shard_id = '0'
        split_key = '16666666666666666666666666666666'

        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/split/topics/success/shards'
            content = json.loads(request.body)
            assert content['Action'] == 'split'
            assert content['SplitKey'] == "16666666666666666666666666666666"
            assert content['ShardId'] == '0'

        with HTTMock(gen_mock_api(check)):
            split_result = dh.split_shard(project_name, topic_name, shard_id, split_key)

        new_shards = split_result.new_shards
        assert len(new_shards) == 2
        assert new_shards[0].shard_id == '3'
        assert new_shards[0].begin_hash_key == '00000000000000000000000000000000'
        assert new_shards[0].end_hash_key == '16666666666666666666666666666666'

        assert new_shards[1].shard_id == '4'
        assert new_shards[1].begin_hash_key == '16666666666666666666666666666666'
        assert new_shards[1].end_hash_key == '55555555555555555555555555555555'

    def test_split_shard_success_without_split_key(self):
        project_name = 'split'
        topic_name = 'default'
        shard_id = '0'

        def check(request):
            assert request.url == 'http://endpoint/projects/split/topics/default/shards'
            if request.method == 'GET':
                pass # list shard
            else:
                assert request.method == 'POST'
                content = json.loads(request.body)
                assert content['Action'] == 'split'
                assert content['SplitKey'] == "2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                assert content['ShardId'] == '0'

        with HTTMock(gen_mock_api(check)):
            split_result = dh.split_shard(project_name, topic_name, shard_id)

    def test_split_shard_with_invalid_state(self):
        project_name = 'split'
        topic_name = 'invalid_state'
        shard_id = '0'
        split_key = '16666666666666666666666666666666'

        try:
            def check(request):
                assert request.method == 'POST'
                assert request.url == 'http://endpoint/projects/split/topics/invalid_state/shards'
                content = json.loads(request.body)
                assert content['Action'] == 'split'
                assert content['SplitKey'] == "16666666666666666666666666666666"
                assert content['ShardId'] == '0'

            with HTTMock(gen_mock_api(check)):
                split_result = dh.split_shard(project_name, topic_name, shard_id, split_key)
        except InvalidOperationException:
            pass
        else:
            raise Exception('split shard success with invalid shard state!')

    def test_split_shard_with_limit_exceeded(self):
        project_name = 'split'
        topic_name = 'limit_exceeded'
        shard_id = '0'
        split_key = '1'

        try:
            def check(request):
                assert request.method == 'POST'
                assert request.url == 'http://endpoint/projects/split/topics/limit_exceeded/shards'
                content = json.loads(request.body)
                assert content['Action'] == 'split'
                assert content['SplitKey'] == "1"
                assert content['ShardId'] == '0'

            with HTTMock(gen_mock_api(check)):
                split_result = dh.split_shard(project_name, topic_name, shard_id, split_key)
        except LimitExceededException:
            pass
        else:
            raise Exception('split shard success with limit exceeded!')

    def test_split_shard_with_invalid_key(self):
        project_name = 'split'
        topic_name = 'invalid_key'
        shard_id = '0'
        split_key = '16666666666666666666666666666666'

        try:
            def check(request):
                assert request.method == 'POST'
                assert request.url == 'http://endpoint/projects/split/topics/invalid_key/shards'
                content = json.loads(request.body)
                assert content['Action'] == 'split'
                assert content['SplitKey'] == "16666666666666666666666666666666"
                assert content['ShardId'] == '0'

            with HTTMock(gen_mock_api(check)):
                split_result = dh.split_shard(project_name, topic_name, shard_id, split_key)
        except InvalidParameterException:
            pass
        else:
            raise Exception('split shard success with invalid key range!')

    def test_split_shard_with_empty_project_name(self):
        project_name = ''
        topic_name = 'valid'
        shard_id = '0'
        split_key = '16666666666666666666666666666666'

        try:
            split_result = dh.split_shard(project_name, topic_name, shard_id, split_key)
        except InvalidParameterException:
            pass
        else:
            raise Exception('split shard success with empty project name!')

    def test_split_shard_with_empty_topic_name(self):
        project_name = 'valid'
        topic_name = ''
        shard_id = '0'
        split_key = '16666666666666666666666666666666'

        try:
            split_result = dh.split_shard(project_name, topic_name, shard_id, split_key)
        except InvalidParameterException:
            pass
        else:
            raise Exception('split shard success with empty topic name!')

    def test_split_shard_with_empty_shard_id(self):
        project_name = 'valid'
        topic_name = 'valid'
        shard_id = ''
        split_key = '16666666666666666666666666666666'

        try:
            split_result = dh.split_shard(project_name, topic_name, shard_id, split_key)
        except InvalidParameterException:
            pass
        else:
            raise Exception('split shard success with empty shard id!')

    def test_split_shard_with_unexisted_project_name(self):
        project_name = 'unexisted'
        topic_name = 'valid'
        shard_id = '0'
        split_key = '16666666666666666666666666666666'

        try:
            def check(request):
                assert request.method == 'POST'
                assert request.url == 'http://endpoint/projects/unexisted/topics/valid/shards'

            with HTTMock(gen_mock_api(check)):
                split_result = dh.split_shard(project_name, topic_name, shard_id, split_key)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('split shard success with unexisted project name!')

    def test_split_shard_with_unexisted_topic_name(self):
        project_name = 'valid'
        topic_name = 'unexisted'
        shard_id = '0'
        split_key = '16666666666666666666666666666666'

        try:
            def check(request):
                assert request.method == 'POST'
                assert request.url == 'http://endpoint/projects/valid/topics/unexisted/shards'

            with HTTMock(gen_mock_api(check)):
                split_result = dh.split_shard(project_name, topic_name, shard_id, split_key)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('split shard success with unexisted topic name!')

    def test_split_shard_with_unexisted_shard_id(self):
        project_name = 'valid'
        topic_name = 'valid'
        shard_id = '99'
        split_key = '16666666666666666666666666666666'

        try:
            def check(request):
                assert request.method == 'POST'
                assert request.url == 'http://endpoint/projects/valid/topics/valid/shards'
                content = json.loads(request.body)
                assert content['Action'] == 'split'
                assert content['SplitKey'] == "16666666666666666666666666666666"
                assert content['ShardId'] == '99'

            with HTTMock(gen_mock_api(check)):
                split_result = dh.split_shard(project_name, topic_name, shard_id, split_key)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('split shard success with unexisted shard id!')

    def test_merge_shard_success(self):
        project_name = 'merge'
        topic_name = 'success'
        shard_id = '0'
        adj_shard_id = '1'

        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/merge/topics/success/shards'
            content = json.loads(request.body)
            assert content['Action'] == 'merge'
            assert content['AdjacentShardId'] == "1"
            assert content['ShardId'] == '0'

        with HTTMock(gen_mock_api(check)):
            merge_result = dh.merge_shard(project_name, topic_name, shard_id, adj_shard_id)

        assert merge_result.shard_id == '2'
        assert merge_result.begin_hash_key == '00000000000000000000000000000000'
        assert merge_result.end_hash_key == '55555555555555555555555555555555'

    def test_merge_shard_with_invalid_state(self):
        project_name = 'merge'
        topic_name = 'invalid_state'
        shard_id = '0'
        adj_shard_id = '1'

        try:
            def check(request):
                assert request.method == 'POST'
                assert request.url == 'http://endpoint/projects/merge/topics/invalid_state/shards'
                content = json.loads(request.body)
                assert content['Action'] == 'merge'
                assert content['AdjacentShardId'] == "1"
                assert content['ShardId'] == '0'

            with HTTMock(gen_mock_api(check)):
                merge_result = dh.merge_shard(project_name, topic_name, shard_id, adj_shard_id)
        except InvalidOperationException:
            pass
        else:
            raise Exception('merge shard success with invalid shard state!')

    def test_merge_shard_with_limit_exceeded(self):
        project_name = 'merge'
        topic_name = 'limit_exceeded'
        shard_id = '0'
        adj_shard_id = '1'

        try:
            def check(request):
                assert request.method == 'POST'
                assert request.url == 'http://endpoint/projects/merge/topics/limit_exceeded/shards'
                content = json.loads(request.body)
                assert content['Action'] == 'merge'
                assert content['AdjacentShardId'] == "1"
                assert content['ShardId'] == '0'

            with HTTMock(gen_mock_api(check)):
                merge_result = dh.merge_shard(project_name, topic_name, shard_id, adj_shard_id)
        except LimitExceededException:
            pass
        else:
            raise Exception('merge shard success with limit exceeded!')

    def test_merge_shard_with_shards_not_adjacent(self):
        project_name = 'merge'
        topic_name = 'shards_not_adjacent'
        shard_id = '0'
        adj_shard_id = '2'

        try:
            def check(request):
                assert request.method == 'POST'
                assert request.url == 'http://endpoint/projects/merge/topics/shards_not_adjacent/shards'
                content = json.loads(request.body)
                assert content['Action'] == 'merge'
                assert content['AdjacentShardId'] == "2"
                assert content['ShardId'] == '0'

            with HTTMock(gen_mock_api(check)):
                merge_result = dh.merge_shard(project_name, topic_name, shard_id, adj_shard_id)
        except InvalidParameterException:
            pass
        else:
            raise Exception('merge shard success with shards not adjacent!')

    def test_merge_shard_with_empty_project_name(self):
        project_name = ''
        topic_name = 'valid'
        shard_id = '0'
        adj_shard_id = '1'

        try:
            merge_result = dh.merge_shard(project_name, topic_name, shard_id, adj_shard_id)
        except InvalidParameterException:
            pass
        else:
            raise Exception('merge shard success with empty project name!')

    def test_merge_shard_with_empty_topic_name(self):
        project_name = 'valid'
        topic_name = ''
        shard_id = '0'
        adj_shard_id = '1'

        try:
            merge_result = dh.merge_shard(project_name, topic_name, shard_id, adj_shard_id)
        except InvalidParameterException:
            pass
        else:
            raise Exception('merge shard success with empty topic name!')

    def test_merge_shard_with_empty_shard_id(self):
        project_name = 'valid'
        topic_name = 'valid'
        shard_id = ''
        adj_shard_id = '1'

        try:
            merge_result = dh.merge_shard(project_name, topic_name, shard_id, adj_shard_id)
        except InvalidParameterException:
            pass
        else:
            raise Exception('merge shard success with empty shard id!')

    def test_merge_shard_with_empty_adj_shard_id(self):
        project_name = 'valid'
        topic_name = 'valid'
        shard_id = '0'
        adj_shard_id = ''

        try:
            merge_result = dh.merge_shard(project_name, topic_name, shard_id, adj_shard_id)
        except InvalidParameterException:
            pass
        else:
            raise Exception('merge shard success with empty split key!')

    def test_merge_shard_with_unexisted_project_name(self):
        project_name = 'unexisted'
        topic_name = 'valid'
        shard_id = '0'
        adj_shard_id = '1'

        try:
            def check(request):
                assert request.method == 'POST'
                assert request.url == 'http://endpoint/projects/unexisted/topics/valid/shards'
                content = json.loads(request.body)
                assert content['Action'] == 'merge'
                assert content['AdjacentShardId'] == "1"
                assert content['ShardId'] == '0'

            with HTTMock(gen_mock_api(check)):
                merge_result = dh.merge_shard(project_name, topic_name, shard_id, adj_shard_id)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('merge shard success with unexisted project name!')

    def test_merge_shard_with_unexisted_topic_name(self):
        project_name = 'valid'
        topic_name = 'unexisted'
        shard_id = '0'
        adj_shard_id = '1'

        try:
            def check(request):
                assert request.method == 'POST'
                assert request.url == 'http://endpoint/projects/valid/topics/unexisted/shards'
                content = json.loads(request.body)
                assert content['Action'] == 'merge'
                assert content['AdjacentShardId'] == "1"
                assert content['ShardId'] == '0'

            with HTTMock(gen_mock_api(check)):
                merge_result = dh.merge_shard(project_name, topic_name, shard_id, adj_shard_id)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('merge shard success with unexisted topic name!')

    def test_merge_shard_with_unexisted_shard_id(self):
        project_name = 'valid'
        topic_name = 'valid'
        shard_id = '99'
        adj_shard_id = '1'

        try:
            def check(request):
                assert request.method == 'POST'
                assert request.url == 'http://endpoint/projects/valid/topics/valid/shards'
                content = json.loads(request.body)
                assert content['Action'] == 'merge'
                assert content['AdjacentShardId'] == "1"
                assert content['ShardId'] == '99'

            with HTTMock(gen_mock_api(check)):
                merge_result = dh.merge_shard(project_name, topic_name, shard_id, adj_shard_id)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('merge shard success with unexisted shard id!')

    def test_merge_shard_with_unexisted_adj_shard_id(self):
        project_name = 'valid'
        topic_name = 'valid'
        shard_id = '0'
        adj_shard_id = '99'

        try:
            def check(request):
                assert request.method == 'POST'
                assert request.url == 'http://endpoint/projects/valid/topics/valid/shards'
                content = json.loads(request.body)
                assert content['Action'] == 'merge'
                assert content['AdjacentShardId'] == "99"
                assert content['ShardId'] == '0'

            with HTTMock(gen_mock_api(check)):
                merge_result = dh.merge_shard(project_name, topic_name, shard_id, adj_shard_id)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('merge shard success with unexisted adjacent shard id!')


if __name__ == '__main__':
    test = TestShard()
    test.test_wait_shards_ready_success()
    test.test_wait_shards_ready_timeout()
    test.test_wait_shards_ready_with_invalid_timeout()
    test.test_wait_shards_ready_with_empty_project_name()
    test.test_wait_shards_ready_with_unexisted_project_name()
    test.test_wait_shards_ready_with_unexisted_topic_name()
    test.test_list_shard_success()
    test.test_list_shard_with_empty_project_name()
    test.test_list_shard_with_empty_topic_name()
    test.test_list_shard_with_unexisted_project_name()
    test.test_list_shard_with_unexisted_topic_name()
    test.test_split_shard_success()
    test.test_split_shard_with_invalid_state()
    test.test_split_shard_with_invalid_key()
    test.test_split_shard_with_limit_exceeded()
    test.test_split_shard_with_empty_project_name()
    test.test_split_shard_with_empty_topic_name()
    test.test_split_shard_with_empty_shard_id()
    test.test_split_shard_with_unexisted_project_name()
    test.test_split_shard_with_unexisted_topic_name()
    test.test_split_shard_with_unexisted_shard_id()
    test.test_split_shard_success_without_split_key()
    test.test_merge_shard_success()
    test.test_merge_shard_with_invalid_state()
    test.test_merge_shard_with_limit_exceeded()
    test.test_merge_shard_with_shards_not_adjacent()
    test.test_merge_shard_with_empty_project_name()
    test.test_merge_shard_with_empty_topic_name()
    test.test_merge_shard_with_unexisted_adj_shard_id()
    test.test_merge_shard_with_empty_adj_shard_id()
    test.test_merge_shard_with_unexisted_project_name()
    test.test_merge_shard_with_unexisted_topic_name()
    test.test_merge_shard_with_empty_shard_id()
    test.test_merge_shard_with_unexisted_shard_id()
