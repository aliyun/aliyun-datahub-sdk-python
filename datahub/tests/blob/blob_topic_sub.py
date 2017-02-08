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
from datahub.utils import Configer
from datahub.models import Topic, RecordType, FieldType, RecordSchema, BlobRecord, CursorType
from datahub.errors import DatahubException, ObjectAlreadyExistException

configer = Configer('../datahub.ini')
access_id = configer.get('datahub', 'access_id', '')
access_key = configer.get('datahub', 'access_key', '')
endpoint = configer.get('datahub', 'endpoint', '')
project_name = configer.get('datahub', 'project_name', 'pydatahub_project_test')
topic_name = configer.get('datahub', 'topic_name', 'pydatahub_blob_topic_test')

print "======================================="
print "access_id: %s" % access_id
print "access_key: %s" % access_key
print "endpoint: %s" % endpoint
print "project_name: %s" % project_name
print "topic_name: %s" % topic_name
print "=======================================\n\n"

if not access_id or not access_key or not endpoint:
    print "access_id and access_key and endpoint must be set!"
    sys.exit(-1)

dh = DataHub(access_id, access_key, endpoint)

topic = Topic(name=topic_name)
topic.project_name = project_name
topic.shard_count = 3
topic.life_cycle = 7
topic.record_type = RecordType.BLOB

try:
    dh.create_topic(topic)
    print "create topic success!"
    print "=======================================\n\n"
except ObjectAlreadyExistException, e:
    print "topic already exist!"
    print "=======================================\n\n"
except Exception, e:
    print traceback.format_exc()
    sys.exit(-1)

try:
    topic = dh.get_topic(topic_name, project_name)
    print "get topic suc! topic=%s" % str(topic)
    if topic.record_type != RecordType.BLOB:
        print "topic type illegal!"
        sys.exit(-1)
    print "=======================================\n\n"

    cursor = dh.get_cursor(project_name, topic_name, CursorType.OLDEST, '0')
    index = 0
    while True:
        (record_list, record_num, next_cursor) = dh.get_records(topic, '0', cursor, 3)
        for record in record_list:
            with open('0_%d.png' % index, 'wb') as f:
                f.write(record.blobdata)
                print "create 0_%d.png suc" % index
            index+=1
        if 0 == record_num:
            time.sleep(1)
        cursor = next_cursor

except DatahubException, e:
    print traceback.format_exc()
    sys.exit(-1)
else:
    sys.exit(-1)
