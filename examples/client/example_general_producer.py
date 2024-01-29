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


import os.path
import configparser
from datahub.core import DatahubProtocolType
from datahub.exceptions import DatahubException
from datahub.models import BlobRecord, FieldType, TupleRecord, CompressFormat, RecordType
from datahub.client import DatahubProducer, ProducerConfig


RECORD_NUM = 5
EPOCH_NUM = 10

parser = configparser.ConfigParser()
parser.read(filenames=os.path.join("./datahub.config.template"))

endpoint         = parser.get("datahub", "endpoint")
access_id        = parser.get("datahub", "access_id")
access_key       = parser.get("datahub", "access_key")
project_name     = parser.get("datahub", "project_name")
topic_name       = parser.get("datahub", "topic_name")
protocol_type = DatahubProtocolType.PB
compress_format = CompressFormat.LZ4

retry_times = parser.get("common", "retry_times")
retry_times = int(retry_times) if len(retry_times) > 0 else -1
async_thread_limit = parser.get("common", "async_thread_limit")
async_thread_limit = int(async_thread_limit) if len(async_thread_limit) > 0 else -1
thread_queue_limit = parser.get("common", "thread_queue_limit")
thread_queue_limit = int(thread_queue_limit) if len(thread_queue_limit) > 0 else -1

producer_config = ProducerConfig(access_id, access_key, endpoint)

if retry_times > 0:
    producer_config.retry_times = retry_times
if async_thread_limit > 0:
    producer_config.async_thread_limit = async_thread_limit
if thread_queue_limit > 0:
    producer_config.thread_queue_limit = thread_queue_limit


def gen_blob_record(data):
    record = BlobRecord(data)
    record.put_attribute("key", "value")
    return record


def gen_tuple_record(schema):
    record = TupleRecord(schema=schema)
    for id, field in enumerate(schema.field_list):
        if field.type in (FieldType.BOOLEAN, ):
            record.set_value(id, True)
        elif field.type in (FieldType.DOUBLE, FieldType.FLOAT, ):
            record.set_value(id, 1.23)
        elif field.type in (FieldType.BIGINT, FieldType.INTEGER, FieldType.SMALLINT, FieldType.TINYINT, ):
            record.set_value(id, 123)
        elif field.type in (FieldType.STRING, ):
            record.set_value(id, "123")
        elif field.type in (FieldType.TIMESTAMP, ):
            record.set_value(id, 123456789)
        elif field.type in (FieldType.DECIMAL, ):
            record.set_value(id, 123)
    record.put_attribute("key", "value")
    return record


def gen_records(topic_meta, record_num):
    records = []
    if topic_meta.record_type == RecordType.BLOB:
        for i in range(record_num):
            data = "test_record_{}".format(i)
            records.append(gen_blob_record(data))
    else:
        for i in range(record_num):
            records.append(gen_tuple_record(topic_meta.record_schema))
    return records


def general_produce():
    datahub_producer = DatahubProducer(project_name, topic_name, producer_config)
    records = gen_records(datahub_producer.topic_meta, RECORD_NUM)

    shard_records = dict()
    try:
        for i in range(EPOCH_NUM):
            shard_id = datahub_producer.write(records)
            if shard_id is None:
                break

            if shard_id not in shard_records:
                shard_records[shard_id] = 0
            shard_records[shard_id] += RECORD_NUM
    except DatahubException as e:
        print("Write record fail. DatahubException: ", e)
    finally:
        datahub_producer.close()

    for shard_id, cnt in shard_records.items():
        print("Write {} records to shard {}".format(cnt, shard_id))


if __name__ == "__main__":
    general_produce()
