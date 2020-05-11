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

import os
import random
import sys
import time
import traceback

from six.moves import configparser

from datahub import DataHub
from datahub.exceptions import ResourceExistException, ResourceNotFoundException, InvalidParameterException

current_path = os.path.split(os.path.realpath(__file__))[0]
root_path = os.path.join(current_path, '../..')

configer = configparser.ConfigParser()
configer.read(os.path.join(current_path, '../datahub.ini'))
access_id = configer.get('datahub', 'access_id')
access_key = configer.get('datahub', 'access_key')
endpoint = configer.get('datahub', 'endpoint')

print("=======================================")
print("access_id: %s" % access_id)
print("access_key: %s" % access_key)
print("endpoint: %s" % endpoint)
print("=======================================\n\n")

if not access_id or not access_key or not endpoint:
    print("[access_id, access_key, endpoint] must be set in datahub.ini!")
    sys.exit(-1)

dh = DataHub(access_id, access_key, endpoint)


class TestProject:

    def test_list_project(self):
        project_name = "project_test_p%d" % int(time.time())

        try:
            dh.create_project(project_name, '')
            result = dh.list_project()
            print(result)
            assert project_name in result.project_names
        except ResourceExistException:
            pass
        except Exception:
            print(traceback.format_exc())
            sys.exit(-1)
        finally:
            dh.delete_project(project_name)

    def test_create_and_delete_project(self):
        project_name = "project_test_p%d" % int(time.time())

        try:
            dh.create_project(project_name, "comment_test")
        except ResourceExistException:
            pass
        except Exception:
            print(traceback.format_exc())
            sys.exit(-1)
        finally:
            dh.delete_project(project_name)

    def test_update_project(self):
        project_name = "project_test_p%d" % int(time.time())

        try:
            dh.create_project(project_name, "comment_test")
            dh.update_project(project_name, "new comment")
            result = dh.get_project(project_name)
            assert result.comment == "new comment"
        except ResourceExistException:
            pass
        except Exception:
            print(traceback.format_exc())
            sys.exit(-1)
        finally:
            dh.delete_project(project_name)

    def test_create_invalid_project(self):
        invalid_project_names = ["", "1invalid", "_invalid", "!invalid", "in",
                                 "invalidinvalidinvalidinvalidinvalidinvalidinvalidinvalid"]

        for invalid_project_name in invalid_project_names:
            try:
                dh.create_project(invalid_project_name, '')
            except InvalidParameterException:
                pass
            else:
                raise Exception('Create success with invalid project name!')

    def test_get_exist_project(self):
        project_name = "project_test_p%d" % int(time.time())

        try:
            dh.create_project(project_name, 'comment_test')
            project_1 = dh.get_project(project_name)

            assert project_1.project_name == project_name
            assert project_1.comment == 'comment_test'
            assert project_1.create_time > 0
            assert project_1.last_modify_time > 0
        except ResourceExistException:
            pass
        except Exception:
            print(traceback.format_exc())
            sys.exit(-1)
        finally:
            dh.delete_project(project_name)

    def test_get_unexist_project(self):
        unexist_project_name = "unexist_project_test_p%d" % random.randint(1000, 9999)
        results = dh.list_project()
        print(results)
        unexist = True

        # try to find an unexist project name and test
        for i in range(0, 10):
            if unexist_project_name in results.project_names:
                unexist = False
                unexist_project_name = "unexist_project_test_p%d" % random.randint(1000, 9999)
            else:
                unexist = True
                break

        if unexist:
            try:
                dh.get_project(unexist_project_name)
            except ResourceNotFoundException:
                pass


# run directly
if __name__ == '__main__':
    test = TestProject()
    test.test_list_project()
    test.test_create_and_delete_project()
    test.test_update_project()
    test.test_create_invalid_project()
    test.test_get_unexist_project()
    test.test_get_exist_project()
