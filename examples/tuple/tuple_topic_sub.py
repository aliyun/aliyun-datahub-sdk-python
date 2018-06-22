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

import sys
import time
import traceback

from datahub import DataHub
from datahub.exceptions import DatahubException, ResourceExistException
from datahub.models import RecordType, CursorType

access_id = '******* your access id *******'
access_key = '******* your access key *******'
endpoint = '******* your endpoint *******'

dh = DataHub(access_id, access_key, endpoint)

project_name = 'tuple_record_test'
topic_name = 'tuple_record_test'

try:
    dh.create_project(project_name, 'comment')
    print("create project success!")
    print("=======================================\n\n")
except ResourceExistException as e:
    print("project already exist!")
    print("=======================================\n\n")
except Exception:
    print(traceback.format_exc())
    sys.exit(-1)

try:
    topic_result = dh.get_topic(topic_name, project_name)
    print("get topic suc! topic=%s" % str(topic_result))
    if topic_result.record_type != RecordType.TUPLE:
        print("topic type illegal!")
        sys.exit(-1)
    print("=======================================\n\n")

    cursor = dh.get_cursor(project_name, topic_name, '0', CursorType.OLDEST).cursor
    while True:
        get_result = dh.get_tuple_records(project_name, topic_name, '0', topic_result.record_schema, cursor, 10)
        for record in get_result.records:
            print(record)
        if 0 == get_result.record_count:
            time.sleep(1)
        cursor = get_result.next_cursor

except DatahubException as e:
    print(traceback.format_exc(e))
    sys.exit(-1)
else:
    sys.exit(-1)
