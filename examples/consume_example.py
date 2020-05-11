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

import time

from datahub import DataHub
from datahub.exceptions import ResourceNotFoundException, InvalidParameterException, DatahubException, \
    InvalidOperationException, OffsetResetException
from datahub.models import CursorType, OffsetWithSession

endpoint = ''
access_id = ''
access_key = ''
project_name = ''
topic_name = ''
sub_id = ''
shard_id = '0'
shards = [shard_id]

dh = DataHub(access_id, access_key, endpoint)

try:
    offset_result = dh.init_and_get_subscription_offset(project_name, topic_name, sub_id, shards)
    offsets = offset_result.offsets
except ResourceNotFoundException as e:
    print(e)
    exit(-1)
except InvalidParameterException as e:
    print(e)
    exit(-1)
except DatahubException as e:
    print(e)
    exit(-1)  # or retry

offset = offsets.get(shard_id)
try:
    if offset.sequence >= 0:
        # sequence is valid
        sequence = offset.sequence
        cursor = dh.get_cursor(project_name, topic_name, shard_id, CursorType.SEQUENCE, sequence).cursor
    else:
        cursor = dh.get_cursor(project_name, topic_name, shard_id, CursorType.OLDEST).cursor
except DatahubException as e:
    print(e)
    exit(-1)

fetch_num = 10
record_count = 0

try:
    schema = dh.get_topic(project_name, topic_name).record_schema
except DatahubException as e:
    print(e)
    exit(-1)

while True:
    try:
        record_result = dh.get_tuple_records(project_name, topic_name, shard_id, schema, cursor, fetch_num)
        if record_result.record_count <= 0:
            time.sleep(1)
            continue

        for record in record_result.records:
            record_count += 1
            offset.sequence = record.sequence
            offset.timestamp = record.system_time

            if record_count % 1000 == 0:
                offsets_to_commit = {
                    shard_id: OffsetWithSession(
                        offset.sequence,
                        offset.timestamp,
                        offset.version,
                        offset.session_id
                    )
                }
                print(offsets_to_commit)
                dh.update_subscription_offset(project_name, topic_name, sub_id, offsets_to_commit)

        cursor = record_result.next_cursor
    except OffsetResetException as e:
        new_offsets = dh.get_subscription_offset(project_name, topic_name, sub_id).offsets
        next_sequence = new_offsets.get(shard_id).sequence
        offset.version = new_offsets.get(shard_id).version
        cursor = dh.get_cursor(project_name, topic_name, shard_id, CursorType.SEQUENCE, next_sequence).cursor
    except InvalidOperationException as e:
        print(e)
        exit(-1)
    except DatahubException as e:
        print(e)
        exit(-1)  # or retry
