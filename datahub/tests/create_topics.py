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
from datahub.models import Project, Topic, RecordType, FieldType, RecordSchema, TupleRecord, CursorType
from datahub.errors import DatahubException, ObjectAlreadyExistException

configer = Configer('datahub.ini')
access_id = configer.get('datahub', 'access_id', '')
access_key = configer.get('datahub', 'access_key', '')
endpoint = configer.get('datahub', 'endpoint', '')
project_name = configer.get('datahub', 'project_name', 'meter_project_test')
topic_name = configer.get('datahub', 'topic_name', 'meter_topic_test')

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

try:
    for pi in range(1,10):
        project_name = "meter_project_test_%d" % pi
        project = Project(name=project_name, comment="meter project test")
        try:
            dh.create_project(project)
            print "create project %s success!" % project_name
            print "=======================================\n\n"
        except ObjectAlreadyExistException, e:
            print "project %s already exist!" % project_name
        for ti in range(1,100):
            topic_name = "meter_topic_test_%d_%d" %(pi, ti)
            topic = Topic(name=topic_name)
            topic.project_name = project_name
            topic.shard_count = 20
            topic.life_cycle = 7
            topic.record_type = RecordType.TUPLE
            topic.record_schema = RecordSchema.from_lists(['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'], [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])
            try:
                dh.create_topic(topic)
                print "create topic %s success!" % topic_name
                # block等待所有shard状态ready
                dh.wait_shards_ready(project_name, topic_name)
                print "shards all ready!!!"
                shards = dh.list_shards(project_name, topic_name)
                for shard in shards:
                    record = TupleRecord(schema=topic.record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
                    record.shard_id = shard.shard_id
                    record.put_attribute('AK', '47')
                    records = []
                    records.append(record)
                    failed_indexs = dh.put_records(project_name, topic_name, records)
                    print "put record to project:%s topic:%s failed_index:%s" %(project_name, topic_name, failed_indexs)
            except ObjectAlreadyExistException, e:
                print "topic %s already exist!" % topic_name
            print "=======================================\n\n"
except Exception, e:
    print traceback.format_exc()
    sys.exit(-1)
