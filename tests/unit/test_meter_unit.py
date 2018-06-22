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
import os

from httmock import HTTMock, urlmatch, response

from datahub import DataHub
from datahub.exceptions import InvalidParameterException, ResourceNotFoundException

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


class TestMeter:

    def test_get_metering_info_success(self):
        project_name = 'meter'
        topic_name = 'success'
        shard_id = '0'

        with HTTMock(datahub_api_mock):
            meter_result = dh.get_metering_info(project_name, topic_name, shard_id)
        print(meter_result)
        assert meter_result.active_time == 1590206
        assert meter_result.storage == 0

    def test_get_metering_info_with_empty_project_name(self):
        project_name = ''
        topic_name = 'valid'
        shard_id = '0'

        try:
            meter_result = dh.get_metering_info(project_name, topic_name, shard_id)
        except InvalidParameterException:
            pass
        else:
            raise Exception('get meter info success with empty project name!')

    def test_get_metering_info_with_empty_topic_name(self):
        project_name = 'valid'
        topic_name = ''
        shard_id = '0'

        try:
            meter_result = dh.get_metering_info(project_name, topic_name, shard_id)
        except InvalidParameterException:
            pass
        else:
            raise Exception('get meter info success with empty topic name!')

    def test_get_metering_info_with_empty_shard_id(self):
        project_name = 'valid'
        topic_name = 'valid'
        shard_id = ''

        try:
            meter_result = dh.get_metering_info(project_name, topic_name, shard_id)
        except InvalidParameterException:
            pass
        else:
            raise Exception('get meter info success with empty shard id!')

    def test_get_metering_info_with_unexisted_project_name(self):
        project_name = 'unexisted'
        topic_name = 'valid'
        shard_id = '0'

        try:
            with HTTMock(datahub_api_mock):
                meter_result = dh.get_metering_info(project_name, topic_name, shard_id)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('get meter info success with unexisted project name!')

    def test_get_metering_info_with_unexisted_topic_name(self):
        project_name = 'valid'
        topic_name = 'unexisted'
        shard_id = '0'

        try:
            with HTTMock(datahub_api_mock):
                meter_result = dh.get_metering_info(project_name, topic_name, shard_id)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('get meter info success with unexisted topic name!')

    def test_get_metering_info_with_unexisted_shard_id(self):
        project_name = 'valid'
        topic_name = 'valid'
        shard_id = '0'

        try:
            with HTTMock(datahub_api_mock):
                meter_result = dh.get_metering_info(project_name, topic_name, shard_id)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('get meter info success with unexisted shard id!')


if __name__ == '__main__':
    test = TestMeter()
    test.test_get_metering_info_success()
    test.test_get_metering_info_with_empty_project_name()
    test.test_get_metering_info_with_empty_topic_name()
    test.test_get_metering_info_with_empty_shard_id()
    test.test_get_metering_info_with_unexisted_project_name()
    test.test_get_metering_info_with_unexisted_topic_name()
    test.test_get_metering_info_with_unexisted_shard_id()
