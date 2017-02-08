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
import time
import traceback

from datahub import DataHub
from datahub.exceptions import ResourceExistException, DatahubException
from datahub.models import FieldType, RecordSchema, TupleRecord, BlobRecord, CursorType, RecordType

access_id = ''
access_key = ''
endpoint = ''

dh = DataHub(access_id, access_key, endpoint)

# ===================== 创建project =====================
project_name = 'project'
comment = 'comment'

try:
    dh.create_project(project_name, comment)
    print("create project success!")
    print("=======================================\n\n")
except ResourceExistException:
    print("project already exist!")
    print("=======================================\n\n")
except Exception as e:
    print(traceback.format_exc())
    sys.exit(-1)

# ===================== 创建topic =====================

# --------------------- Tuple topic ---------------------
tuple_topic_name = "tuple_topic"
shard_count = 3
life_cycle = 7
record_schema = RecordSchema.from_lists(
    ['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
    [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])

try:
    dh.create_tuple_topic(project_name, tuple_topic_name, shard_count, life_cycle, record_schema, comment)
    print("create tuple topic success!")
    print("=======================================\n\n")
except ResourceExistException:
    print("topic already exist!")
    print("=======================================\n\n")
except Exception as e:
    print(traceback.format_exc())
    sys.exit(-1)

# --------------------- Blob topic ---------------------
blob_topic_name = "blob_topic"
shard_count = 3
life_cycle = 7
try:
    dh.create_blob_topic(project_name, blob_topic_name, shard_count, life_cycle, comment)
    print("create blob topic success!")
    print("=======================================\n\n")
except ResourceExistException:
    print("topic already exist!")
    print("=======================================\n\n")
except Exception as e:
    print(traceback.format_exc())
    sys.exit(-1)

# ===================== get topic =====================
topic_result = dh.get_topic(project_name, tuple_topic_name)
print(topic_result)
print(topic_result.record_schema)

# ===================== list shard =====================
shards_result = dh.list_shard(project_name, tuple_topic_name)
print(shards_result)

# ===================== put tuple records =====================
try:
    # block等待所有shard状态ready
    dh.wait_shards_ready(project_name, tuple_topic_name)
    print("shards all ready!!!")
    print("=======================================\n\n")

    topic_result = dh.get_topic(project_name, "tuple_topic")
    print(topic_result)
    if topic_result.record_type != RecordType.TUPLE:
        print("topic type illegal!")
        sys.exit(-1)
    print("=======================================\n\n")

    record_schema = topic_result.record_schema

    records0 = []
    record0 = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
    record0.shard_id = '0'
    record0.put_attribute('AK', '47')
    records0.append(record0)

    record1 = TupleRecord(schema=record_schema)
    record1.set_value('bigint_field', 2)
    record1.set_value('string_field', 'yc2')
    record1.set_value('double_field', None)
    record1.set_value('bool_field', False)
    record1.set_value('time_field', 1455869335000011)
    record1.hash_key = '4FFFFFFFFFFFFFFD7FFFFFFFFFFFFFFD'
    records0.append(record1)

    record2 = TupleRecord(schema=record_schema)
    record2.set_value(0, 3)
    record2.set_value(1, 'yc3')
    record2.set_value(2,  1.1)
    record2.set_value(3, False)
    record2.set_value(4, 1455869335000011)
    record2.attributes = {'key': 'value'}
    record2.partition_key = 'TestPartitionKey'
    records0.append(record2)

    put_result = dh.put_records(project_name, tuple_topic_name, records0)
    print(put_result)

    print("put tuple %d records, failed count: %d" %(len(records0), put_result.failed_record_count))
    # failed_record_count如果大于0最好对failed record再进行重试
    print("=======================================\n\n")
except DatahubException as e:
    print(e)
    sys.exit(-1)

# ===================== put blob records =====================
try:
    records1 = []
    record3 = BlobRecord(blob_data=b'data')
    record3.shard_id = '0'
    record3.put_attribute('a', 'b')
    records1.append(record3)
    put_result = dh.put_records(project_name, blob_topic_name, records1)
    print(put_result)
except DatahubException as e:
    print(e)
    sys.exit(-1)

# ===================== get cursor =====================
shard_id = "0"
sequence = 0
time_stamp = int(time.time())
cursor_result0 = dh.get_cursor(project_name, tuple_topic_name, shard_id, CursorType.OLDEST)
cursor_result1 = dh.get_cursor(project_name, tuple_topic_name, shard_id, CursorType.LATEST)
cursor_result2 = dh.get_cursor(project_name, tuple_topic_name, shard_id, CursorType.SEQUENCE, sequence)
cursor_result3 = dh.get_cursor(project_name, tuple_topic_name, shard_id, CursorType.SYSTEM_TIME, time_stamp)
cursor = cursor_result0.cursor

# ===================== get blob records =====================
get_result = dh.get_blob_records(project_name, blob_topic_name, '0', cursor, 10)
print(get_result)
print(get_result.records)
print(get_result.records[0].blob_data)

# ===================== get tuple records =====================
get_result = dh.get_tuple_records(project_name, tuple_topic_name, '0', record_schema, cursor, 10)
print(get_result)
print(get_result.records)
print(get_result.records[0].values)

try:
    # block等待所有shard状态ready
    dh.wait_shards_ready(project_name, tuple_topic_name)
    print("shards all ready!!!")
    print("=======================================\n\n")

    topic_result = dh.get_topic(project_name, "tuple_topic")
    print(topic_result)
    if topic_result.record_type != RecordType.TUPLE:
        print("topic type illegal!")
        sys.exit(-1)
    print("=======================================\n\n")

    shard_id = '0'
    limit = 10
    cursor_result = dh.get_cursor(project_name, tuple_topic_name, shard_id, CursorType.OLDEST)
    cursor = cursor_result.cursor
    while True:
        get_result = dh.get_tuple_records(project_name, tuple_topic_name, shard_id, record_schema, cursor, limit)
        for record in get_result.records:
            print(record)
        if 0 == get_result.record_count:
            time.sleep(1)
        cursor = get_result.next_cursor

except DatahubException as e:
    print(e)
    sys.exit(-1)
