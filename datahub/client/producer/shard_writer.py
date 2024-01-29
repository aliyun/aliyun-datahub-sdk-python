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


import time
import atomic
import logging
import threading
from datahub.exceptions import DatahubException
from .write_result import WriteResult
from .record_pack_queue import RecordPackQueue
from ..common.datahub_factory import DatahubFactory


class ShardWriter:

    def __init__(self, project_name, topic_name, sub_id, message_writer, producer_config, shard_id):
        self._closed = False
        self._logger = logging.getLogger(ShardWriter.__name__)

        self._lock = threading.Lock()
        self._project_name = project_name
        self._topic_name = topic_name
        self._sub_id = sub_id
        self._uniq_key = "{}:{}:{}".format(project_name, topic_name, sub_id)
        self._message_writer = message_writer
        self._shard_id = shard_id
        self._max_retry_times = producer_config.retry_times

        self._task_num = atomic.AtomicLong(0)
        self._condition = threading.Condition()

        self._has_write_count = atomic.AtomicLong(0)
        self._datahub_client = DatahubFactory.create_datahub_client(producer_config)

        self._record_package_queue = RecordPackQueue(producer_config.max_async_buffer_size, producer_config.max_async_buffer_records,
                                                     producer_config.max_async_buffer_time, producer_config.max_record_pack_queue_limit)

    def close(self):
        self._closed = True
        self._logger.info("ShardWriter closed. key: {}, shard_id: {}, write count: {}".format(self._uniq_key, self._shard_id, self._has_write_count.value))

    @property
    def shard_id(self):
        return self._shard_id

    def write(self, records):
        if self._closed:
            self._logger.warning("ShardWriter closed when write. key: {}, shard_id: {}".format(self._uniq_key, self._shard_id))
            raise DatahubException("ShardWriter closed when write")

        self.__write_once(records)
        self._logger.debug("Send next write task success. key: {}, record count: {}".format(self._uniq_key, len(records)))

    def write_async(self, records):
        if self._closed:
            self._logger.warning("ShardWriter closed when write async. key: {}, shard_id: {}".format(self._uniq_key, self._shard_id))
            raise DatahubException("ShardWriter closed when write async")

        result = self._record_package_queue.append_record(records)

        if self._task_num.value == 0:
            self.__send_next_task()
        return result

    def flush(self):
        if self._closed:
            self._logger.warning("ShardWriter closed when flush. key: {}, shard_id: {}".format(self._uniq_key, self._shard_id))
            raise DatahubException("ShardWriter closed when flush")

        self._record_package_queue.flush()
        self.__send_next_task()

        while self._task_num.value > 0:
            with self._condition:
                self._condition.wait()

    def __send_next_task(self):
        with self._lock:
            pack = self._record_package_queue.obtain_ready_record_pack()
            if pack is not None:
                if not self._message_writer.send_task(int(self._shard_id), self.__gen_next_write_task, pack):
                    # Add task fail when thread pool full
                    self._logger.warning("Send next task fail. key: {}, shard_id: {}, task num: {}"
                                         .format(self._uniq_key, self._shard_id, self._task_num.value))
                    raise DatahubException("Send next task fail. key: {}, shard_id: {}".format(self._uniq_key, self._shard_id))
                self._task_num += 1
                self._logger.debug("Send next task once. key: {}, shard_id: {}, task_num: {}"
                                   .format(self._uniq_key, self._shard_id, self._task_num.value))

    def __write_once(self, records):
        retry_time = 0
        while True:
            try:
                self._message_writer.put_record_by_shard(self._shard_id, records)
                self._has_write_count += len(records)
                return
            except DatahubException as e:
                self._logger.warning("Write records fail. key: {}, shard_id: {}, records size: {}, max retry time: {}, this time: {}, DatahubException: {}"
                                     .format(self._uniq_key, self._shard_id, len(records), self._max_retry_times, retry_time, e))
                retry_time += 1
                if retry_time >= self._max_retry_times:
                    raise e
            except Exception as e:
                self._logger.warning("Write records fail. key: {}, shard_id: {}, records size: {}, {}".format(self._uniq_key, self._shard_id, len(records), e))
                raise e

    def __gen_next_write_task(self, record_pack):
        records = record_pack.records
        futures = record_pack.write_result_futures
        init_time = record_pack.init_time

        try:
            start_time = time.time()
            self.__write_once(records)
            end_time = time.time()

            self._logger.debug("write async once success. key: {}, shard_id: {}, records size: {}"
                               .format(self._uniq_key, self._shard_id, len(records)))
            self.__set_result_to_futures(futures, WriteResult(self._shard_id, end_time - init_time, end_time - start_time))
        except DatahubException as e:
            self._logger.warning("write async once fail. key: {}, shard_id: {}, records size: {}, DatahubException: {}"
                                 .format(self._uniq_key, self._shard_id, len(records), e))
            self.__set_exception_to_futures(futures, e)
        except Exception as e:
            self._logger.warning("write async once fail. key: {}, shard_id: {}, records size: {}, Exception: {}"
                                 .format(self._uniq_key, self._shard_id, len(records), e))
            self.__set_exception_to_futures(futures, e)

    def __set_result_to_futures(self, futures, target):
        for future in futures:
            future.set_result(target)
        self.__task_done()

    def __set_exception_to_futures(self, futures, target):
        for future in futures:
            future.set_exception(target)
        self.__task_done()

    def __task_done(self):
        self.__send_next_task()
        self._task_num -= 1
        with self._condition:
            self._condition.notify_all()
