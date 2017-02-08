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

from __future__ import absolute_import

import json

from .. import utils
from .rest import HTTPMethod, RestModel
from .. import errors

class Project(RestModel):
    """
    Project class
    """
    def __init__(self, *args, **kwds):
        super(Project, self).__init__(*args, **kwds)

    def throw_exception(self, response_result):
        if 'ProjectAlreadyExist' == response_result.error_code:
            raise errors.ObjectAlreadyExistException(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)
        elif 'NoSuchProject' == response_result.error_code:
            raise errors.NoSuchObjectException(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)
        elif response_result.status_code >= 500:
            raise errors.ServerInternalError(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)
        else:
            raise errors.DatahubException(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)

    def resource(self):
        return "/projects/%s" % self._name

    def encode(self, method):
        ret = {}
        if HTTPMethod.POST == method:
            ret["data"] = json.dumps({"Comment": "%s" % self._comment})

        return ret

    def decode(self, method, resp):
        if HTTPMethod.GET == method:
            content = json.loads(resp.content)
            self._comment = content['Comment']
            self._create_time = content['CreateTime']
            self._last_modify_time = content['LastModifyTime']

class Projects(RestModel):
    """
    Projects class.
    List project interface will use it.
    """

    __slots__ = ('_project_names')

    def __init__(self):
        self._project_names = []

    def __len__(self):
        return len(self._project_names)

    def append(self, project_name):
        self._project_names.append(project_name)

    def extend(self, project_names):
        self._project_names.extend(project_names)

    def __contains__(self, name):
        return utils.to_str(name) in self._project_names

    def __setitem__(self, index, project_name):
        if index < 0  or index > len(self._project_names) - 1:
            raise ValueError('index out range')
        self._project_names[index] = project_name

    def __getitem__(self, index):
        if index < 0  or index > len(self._project_names) - 1:
            raise ValueError('index out range')
        return self._project_names[index]

    def __str__(self):
        projectsjson = {}
        projectsjson['ProjectNames'] = []
        for project_name in self._project_names:
            projectsjson['ProjectNames'].append(project_name)
        return json.dumps(projectsjson)

    def __iter__(self):
        for name in self._project_names:
            yield name

    def throw_exception(self, response_result):
        if response_result.status_code >= 500:
            raise errors.ServerInternalError(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)
        else:
            raise errors.DatahubException(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)

    def resource(self):
        return "/projects"

    def encode(self, method):
        ret = {}
        return ret

    def decode(self, method, resp):
        if HTTPMethod.GET == method:
            content = json.loads(resp.content)
            for project_name in content['ProjectNames']:
                self.append(project_name)
