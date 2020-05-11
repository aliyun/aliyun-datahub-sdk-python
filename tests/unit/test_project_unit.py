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

from httmock import HTTMock
import sys

sys.path.append('./')
from datahub import DataHub
from datahub.exceptions import InvalidParameterException, ResourceNotFoundException, ResourceExistException
from .unittest_util import gen_mock_api

dh = DataHub('access_id', 'access_key', 'http://endpoint')


class TestProject:

    def test_list_project(self):
        def check(request):
            assert request.method == 'GET'
            assert request.url == 'http://endpoint/projects'

        with HTTMock(gen_mock_api(check)):
            result = dh.list_project()
            print(result)
            assert 'project_name_1' in result.project_names

    def test_create_project_success(self):
        def check(request):
            assert request.method == 'POST'
            assert request.url == 'http://endpoint/projects/valid_name'
            assert request.body == '{"Comment": "comment"}'

        with HTTMock(gen_mock_api(check)):
            dh.create_project('valid_name', 'comment')

    def test_update_project_success(self):
        def check(request):
            assert request.method == 'PUT'
            assert request.url == 'http://endpoint/projects/valid_name'
            assert request.body == '{"Comment": "new_comment"}'

        with HTTMock(gen_mock_api(check)):
            dh.update_project('valid_name', 'new_comment')

    def test_create_project_with_invalid_project_name(self):
        invalid_project_names = [None, "", "1invalid", "_invalid", "!invalid", "in",
                                 "invalidinvalidinvalidinvalidinvalidinvalidinvalidinvalid"]

        for invalid_project_name in invalid_project_names:
            try:
                dh.create_project(invalid_project_name, '')
            except InvalidParameterException:
                pass
            else:
                raise Exception('create success with invalid project name!')

    def test_get_project_success(self):
        project_name = 'success'

        def check(request):
            assert request.method == 'GET'
            assert request.url == 'http://endpoint/projects/success'

        with HTTMock(gen_mock_api(check)):
            get_result = dh.get_project(project_name)
            print(get_result)

        assert get_result.project_name == 'success'
        assert get_result.comment == 'get project'
        assert get_result.create_time == 1525312757
        assert get_result.last_modify_time == 1525312757

    def test_get_project_already_existed(self):
        project_name = 'existed'
        try:
            def check(request):
                assert request.method == 'GET'
                assert request.url == 'http://endpoint/projects/existed'

            with HTTMock(gen_mock_api(check)):
                get_result = dh.get_project(project_name)
        except ResourceExistException:
            pass
        else:
            raise Exception('get success with project already existed!')

    def test_get_project_with_unexisted_project_name(self):
        project_name = 'unexisted'
        try:
            def check(request):
                assert request.method == 'GET'
                assert request.url == 'http://endpoint/projects/unexisted'

            with HTTMock(gen_mock_api(check)):
                get_result = dh.get_project(project_name)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('get success with unexisted project name!')

    def test_get_project_with_empty_project_name(self):
        project_name = ''
        try:
            get_result = dh.get_project(project_name)
        except InvalidParameterException:
            pass
        else:
            raise Exception('get success with empty project name!')

    def test_delete_project_success(self):
        def check(request):
            assert request.method == 'DELETE'
            assert request.url == 'http://endpoint/projects/valid_name'

        with HTTMock(gen_mock_api(check)):
            dh.delete_project('valid_name')

    def test_delete_project_with_unexisted_project_name(self):
        project_name = 'unexisted'
        try:
            def check(request):
                assert request.method == 'DELETE'
                assert request.url == 'http://endpoint/projects/unexisted'

            with HTTMock(gen_mock_api(check)):
                dh.delete_project(project_name)
        except ResourceNotFoundException:
            pass
        else:
            raise Exception('get success with unexisted project name!')

    def test_delete_project_with_empty_project_name(self):
        project_name = ''
        try:
            dh.delete_project(project_name)
        except InvalidParameterException:
            pass
        else:
            raise Exception('get success with empty project name!')


if __name__ == '__main__':
    test = TestProject()
    test.test_list_project()
    test.test_create_project_success()
    test.test_update_project_success()
    test.test_get_project_already_existed()
    test.test_create_project_with_invalid_project_name()
    test.test_get_project_success()
    test.test_get_project_with_empty_project_name()
    test.test_get_project_with_unexisted_project_name()
    test.test_delete_project_success()
    test.test_delete_project_with_empty_project_name()
    test.test_delete_project_with_unexisted_project_name()
