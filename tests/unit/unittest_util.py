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

from httmock import urlmatch, response

from datahub.exceptions import InvalidParameterException

_TESTS_PATH = os.path.abspath(os.path.dirname(__file__))
_FIXTURE_PATH = os.path.join(_TESTS_PATH, '../fixtures')


def gen_mock_api(check):
    @urlmatch(netloc=r'(.*\.)?endpoint')
    def datahub_api_mock(url, request):
        check(request)
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

    return datahub_api_mock


def gen_pb_mock_api(check):
    @urlmatch(netloc=r'(.*\.)?endpoint')
    def datahub_pb_api_mock(url, request):
        check(request)
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

    return datahub_pb_api_mock
