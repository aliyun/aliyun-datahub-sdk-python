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
    # block等待所有shard状态ready
    if dh.wait_shards_ready(project_name, topic_name):
        print "shards all ready!!!"
    else:
        print "some shards not ready!!!"
        sys.exit(-1)
    print "=======================================\n\n"

    topic = dh.get_topic(topic_name, project_name)
    print "get topic suc! topic=%s" % str(topic)
    if topic.record_type != RecordType.BLOB:
        print "topic type illegal!"
        sys.exit(-1)
    print "=======================================\n\n"

    shards = dh.list_shards(project_name, topic_name)
    for shard in shards:
        print shard
    print "=======================================\n\n"

    records = []
    
    data = None
    with open('datahub.png', 'rb') as f:
        data = f.read()
    
    record0 = BlobRecord(blobdata=data)
    record0.shard_id = '0'
    records.append(record0)
    
    record1 = BlobRecord(blobdata=data)
    record1.shard_id = '1'
    records.append(record1)
    
    record2 = BlobRecord(blobdata=data)
    record2.shard_id = '2'
    records.append(record2)
    
    failed_indexs = dh.put_records(project_name, topic_name, records)
    print "put blob %d records, failed list: %s" %(len(records), failed_indexs)
    # failed_indexs如果非空最好对failed record再进行重试
    print "=======================================\n\n"
    
except DatahubException, e:
    print traceback.format_exc()
    sys.exit(-1)
else:
    sys.exit(-1)
