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
from datahub.models.compress import CompressFormat
from datahub.client import DatahubConsumer, ConsumerConfig


parser = configparser.ConfigParser()
parser.read(filenames=os.path.join("./datahub.config.template"))

endpoint         = parser.get("datahub", "endpoint")
access_id        = parser.get("datahub", "access_id")
access_key       = parser.get("datahub", "access_key")
project_name     = parser.get("datahub", "project_name")
topic_name       = parser.get("datahub", "topic_name")
sub_id           = parser.get("datahub", "sub_id")
protocol_type = DatahubProtocolType.PB
compress_format = CompressFormat.LZ4

retry_times = parser.get("common", "retry_times")
retry_times = int(retry_times) if len(retry_times) > 0 else -1
async_thread_limit = parser.get("common", "async_thread_limit")
async_thread_limit = int(async_thread_limit) if len(async_thread_limit) > 0 else -1
thread_queue_limit = parser.get("common", "thread_queue_limit")
thread_queue_limit = int(thread_queue_limit) if len(thread_queue_limit) > 0 else -1

auto_ack_offset = parser.get("consumer", "auto_ack_offset")
auto_ack_offset = bool(int(auto_ack_offset)) if len(auto_ack_offset) > 0 else True
session_timeout = parser.get("consumer", "session_timeout")
session_timeout = int(session_timeout) if len(session_timeout) > 0 else -1
max_record_buffer_size = parser.get("consumer", "max_record_buffer_size")
max_record_buffer_size = int(max_record_buffer_size) if len(max_record_buffer_size) > 0 else -1
fetch_limit = parser.get("consumer", "fetch_limit")
fetch_limit = int(fetch_limit) if len(fetch_limit) > 0 else -1

consumer_config = ConsumerConfig(access_id, access_key, endpoint)

if retry_times > 0:
    consumer_config.retry_times = retry_times
if async_thread_limit > 0:
    consumer_config.async_thread_limit = async_thread_limit
if thread_queue_limit > 0:
    consumer_config.thread_queue_limit = thread_queue_limit
if auto_ack_offset is False:
    consumer_config.auto_ack_offset = auto_ack_offset
if session_timeout > 0:
    consumer_config.session_timeout = session_timeout
if max_record_buffer_size > 0:
    consumer_config.max_record_buffer_size = max_record_buffer_size
if fetch_limit > 0:
    consumer_config.fetch_limit = fetch_limit


def process_result(record):
    pass


def collaborative_consume():
    datahub_consumer = DatahubConsumer(project_name, topic_name, sub_id, consumer_config)

    record_cnt = 0
    try:
        while True:
            record = datahub_consumer.read(timeout=60)
            if record is None:
                break
            # TODO: deal with record data
            process_result(record)
            record_cnt += 1
            if not consumer_config.auto_ack_offset:  # ack the record manually if auto_ack_offset is False
                record.record_key.ack()
    except DatahubException as e:
        print("Read record fail. DatahubException: ", e)
    finally:
        datahub_consumer.close()

    print("Read {} records total".format(record_cnt))


if __name__ == "__main__":
    collaborative_consume()

