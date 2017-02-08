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
from datahub.exceptions import DatahubException, ResourceExistException
from datahub.models import RecordType, BlobRecord

access_id = '******* your access id *******'
access_key = '******* your access key *******'
endpoint = '******* your endpoint *******'

dh = DataHub(access_id, access_key, endpoint)

project_name = 'blob_record_test'
topic_name = 'blob_record_test'
shard_count = 3
life_cycle = 7

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
    dh.create_blob_topic(project_name, topic_name, shard_count, life_cycle, 'comment')
    print("create topic success!")
    print("=======================================\n\n")
except ResourceExistException as e:
    print("topic already exist!")
    print("=======================================\n\n")
except Exception:
    print(traceback.format_exc())
    sys.exit(-1)

try:
    dh.wait_shards_ready(project_name, topic_name)

    topic = dh.get_topic(topic_name, project_name)
    print("get topic suc! topic=%s" % str(topic))
    if topic.record_type != RecordType.BLOB:
        print("topic type illegal!")
        sys.exit(-1)
    print("=======================================\n\n")

    shards_result = dh.list_shard(project_name, topic_name)
    for shard in shards_result.shards:
        print(shard)
    print("=======================================\n\n")

    records = []

    data = 'iVBORw0KGgoAAAANSUhEUgAAB5FrTVeMB4wHjAeMBD3nAgEU'

    record0 = BlobRecord(blob_data=data)
    record0.shard_id = '0'
    records.append(record0)

    record1 = BlobRecord(blob_data=data)
    record1.shard_id = '1'
    records.append(record1)

    record2 = BlobRecord(blob_data=data)
    record2.shard_id = '2'
    records.append(record2)

    failed_indices = dh.put_records(project_name, topic_name, records)
    print("put blob %d records, failed list: %s" % (len(records), failed_indices))
    print("=======================================\n\n")

except DatahubException as e:
    print(traceback.format_exc())
    sys.exit(-1)
else:
    sys.exit(-1)
