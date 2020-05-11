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
# Unless required by applicable law or agreed to in writing,r
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import sys
import traceback

from datahub import DataHub
from datahub.exceptions import DatahubException, ResourceExistException
from datahub.models import RecordType, FieldType, RecordSchema, TupleRecord

access_id = '******* your access id *******'
access_key = '******* your access key *******'
endpoint = '******* your endpoint *******'

dh = DataHub(access_id, access_key, endpoint, read_timeout=10)

project_name = 'tuple_record_test'
topic_name = 'tuple_record_test'
shard_count = 3
life_cycle = 7
record_type = RecordType.TUPLE
record_schema = RecordSchema.from_lists(
    ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
    [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])
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
    dh.create_tuple_topic(project_name, topic_name, shard_count, life_cycle, record_schema, 'comment')
    print("create topic success!")
    print("=======================================\n\n")
except ResourceExistException as e:
    print("topic already exist!")
    print("=======================================\n\n")
except Exception:
    print(traceback.format_exc())
    sys.exit(-1)

try:
    # block等待所有shard状态ready
    dh.wait_shards_ready(project_name, topic_name)

    topic = dh.get_topic(project_name, topic_name)
    print("get topic suc! topic=%s" % str(topic))
    if topic.record_type != RecordType.TUPLE:
        print("topic type illegal!")
        sys.exit(-1)
    print("=======================================\n\n")

    shards_result = dh.list_shard(project_name, topic_name)
    shards = shards_result.shards
    for shard in shards:
        print(shard)
    print("=======================================\n\n")

    while True:
        records = []

        record0 = TupleRecord(schema=topic.record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
        record0.shard_id = shards[0].shard_id
        record0.put_attribute('AK', '47')
        records.append(record0)

        record1 = TupleRecord(schema=topic.record_schema)
        record1.values = [1, 'yc1', 10.01, True, 1455869335000000]
        record1.shard_id = shards[1].shard_id
        records.append(record1)

        record2 = TupleRecord(schema=topic.record_schema)
        record2.set_value(0, 3)
        record2.set_value(1, 'yc3')
        record2.set_value('double_field', 10.03)
        record2.set_value('bool_field', False)
        record2.set_value('time_field', 1455869335000013)
        record2.shard_id = shards[2].shard_id
        records.append(record2)

        failed_indexs = dh.put_records(project_name, topic_name, records)
        print("put tuple %d records, failed list: %s" % (len(records), failed_indexs))
        # failed_indexs如果非空最好对failed record再进行重试
        print("=======================================\n\n")


except DatahubException as e:
    print(traceback.format_exc())
    sys.exit(-1)
else:
    sys.exit(-1)
