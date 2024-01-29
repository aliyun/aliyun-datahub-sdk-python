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


import logging
from datahub.exceptions import DatahubException
from ..common.thread_pool import HashThreadPool


class MessageWriter:

    def __init__(self, meta_data, queue_limit_num, threads_num):
        self._meta_data = meta_data
        self._logger = logging.getLogger(MessageWriter.__name__)
        self._executor = HashThreadPool(queue_limit_num, threads_num, "MessageWriter")

    def close(self):
        self._executor.shutdown()

    def empty(self, key):
        return self._executor.empty(key)

    def send_task(self, key, task, *args, **kwargs):
        return self._executor.submit(key, task, *args, **kwargs)

    def put_record(self, records):
        topic_meta = self._meta_data.topic_meta
        datahub_client = self._meta_data.datahub_client

        try:
            datahub_client.put_records(topic_meta.project_name, topic_meta.topic_name, records)
        except DatahubException as e:
            self._logger.warning("Put records fail. records count: {}, DatahubException: {}".format(len(records), e))
            raise e
        except Exception as e:
            self._logger.warning("Put records fail. records count: {}, {}".format(len(records), e))
            raise e

    def put_record_by_shard(self, shard_id, records):
        topic_meta = self._meta_data.topic_meta
        datahub_client = self._meta_data.datahub_client

        try:
            datahub_client.put_records_by_shard(topic_meta.project_name, topic_meta.topic_name, shard_id, records)
        except DatahubException as e:
            self._logger.warning("Put records by shard fail. shard_id: {}, records count: {}, DatahubException: {}".format(shard_id, len(records), e))
            raise e
        except Exception as e:
            self._logger.warning("Put records by shard fail. shard_id: {}, records count: {}, {}".format(shard_id, len(records), e))
            raise e
