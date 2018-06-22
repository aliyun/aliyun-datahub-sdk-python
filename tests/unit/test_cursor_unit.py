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
from datahub.exceptions import ResourceNotFoundException, InvalidParameterException
from datahub.models import CursorType

_TESTS_PATH = os.path.abspath(os.path.dirname(__file__))
_FIXTURE_PATH = os.path.join(_TESTS_PATH, '../fixtures')

dh = DataHub('access_id', 'access_key', 'http://endpoint')


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
    except (IOError, ValueError) as e:
        content['ErrorMessage'] = 'Loads fixture %s failed, error: %s' % (res_file, e)
    return response(status_code, content, headers, request=request)


class TestCursor:

    def test_get_cursor_success(self):
        project_name = 'cursor'
        topic_name = 'success'
        shard_id = '0'
        with HTTMock(datahub_api_mock):
            cursor_oldest = dh.get_cursor(project_name, topic_name, shard_id, CursorType.OLDEST)
            cursor_latest = dh.get_cursor(project_name, topic_name, shard_id, CursorType.LATEST)
            cursor_sequence = dh.get_cursor(project_name, topic_name, shard_id, CursorType.SEQUENCE, 0)
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

    def test_get_cursor_with_invalid_param(self):
        project_name = 'cursor'
        topic_name = 'invalid_param'
        shard_id = '0'

        try:
            with HTTMock(datahub_api_mock):
                cursor_system_time = dh.get_cursor(project_name, topic_name, shard_id, CursorType.SYSTEM_TIME,
                                                   999999999)
        except InvalidParameterException:
            pass
        else:
            raise Exception('get cursor success with invalid param!')

    def test_get_cursor_with_empty_project_name(self):
        project_name = ''
        topic_name = 'valid'
        shard_id = '0'

        try:
            cursor_system_time = dh.get_cursor(project_name, topic_name, shard_id, CursorType.SYSTEM_TIME, 0)
        except InvalidParameterException:
            pass
        else:
            raise Exception('get cursor success with empty project name!')

    def test_get_cursor_with_empty_topic_name(self):
        project_name = 'valid'
        topic_name = ''
        shard_id = '0'

        try:
            cursor_system_time = dh.get_cursor(project_name, topic_name, shard_id, CursorType.SYSTEM_TIME, 0)
        except InvalidParameterException:
            pass
        else:
            raise Exception('get cursor success with empty topic name!')

    def test_get_cursor_with_empty_shard_id(self):
        project_name = 'valid'
        topic_name = 'valid'
        shard_id = ''

        try:
            cursor_system_time = dh.get_cursor(project_name, topic_name, shard_id, CursorType.SYSTEM_TIME, 0)
        except InvalidParameterException:
            pass
        else:
            raise Exception('get cursor success with empty shard id!')

    def test_get_cursor_with_invalid_cursor_type(self):
        project_name = 'valid'
        topic_name = 'valid'
        shard_id = ''

        try:
            cursor_system_time = dh.get_cursor(project_name, topic_name, shard_id, 'system_time', 0)
        except InvalidParameterException:
            pass
        else:
            raise Exception('get cursor success with invalid cursor type!')

    def test_get_cursor_with_unexisted_project_name(self):
        project_name = 'unexisted'
        topic_name = 'valid'
        shard_id = '0'

        try:
            with HTTMock(datahub_api_mock):
                cursor_system_time = dh.get_cursor(project_name, topic_name, shard_id, CursorType.SYSTEM_TIME, 0)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('get cursor success with unexisted project name!')

    def test_get_cursor_with_unexisted_topic_name(self):
        project_name = 'valid'
        topic_name = 'unexisted'
        shard_id = '0'

        try:
            with HTTMock(datahub_api_mock):
                cursor_system_time = dh.get_cursor(project_name, topic_name, shard_id, CursorType.SYSTEM_TIME, 0)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('get cursor success with unexisted topic name!')

    def test_get_cursor_with_unexisted_shard_id(self):
        project_name = 'valid'
        topic_name = 'valid'
        shard_id = '0'

        try:
            with HTTMock(datahub_api_mock):
                cursor_system_time = dh.get_cursor(project_name, topic_name, shard_id, CursorType.SYSTEM_TIME, 0)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('get cursor success with unexisted shard id!')

    def test_get_cursor_without_param(self):
        project_name = 'valid'
        topic_name = ''
        shard_id = '0'

        try:
            cursor_system_time = dh.get_cursor(project_name, topic_name, shard_id, CursorType.SYSTEM_TIME)
        except InvalidParameterException:
            pass
        else:
            raise Exception('get system time cursor success without system time!')

        try:
            cursor_sequence = dh.get_cursor(project_name, topic_name, shard_id, CursorType.SEQUENCE)
        except InvalidParameterException:
            pass
        else:
            raise Exception('get sequence cursor success without sequence!')


if __name__ == '__main__':
    test = TestCursor()
    test.test_get_cursor_success()
    test.test_get_cursor_with_invalid_param()
    test.test_get_cursor_with_empty_project_name()
    test.test_get_cursor_with_empty_topic_name()
    test.test_get_cursor_with_empty_shard_id()
    test.test_get_cursor_with_invalid_cursor_type()
    test.test_get_cursor_with_unexisted_project_name()
    test.test_get_cursor_with_unexisted_topic_name()
    test.test_get_cursor_with_unexisted_shard_id()
    test.test_get_cursor_without_param()
