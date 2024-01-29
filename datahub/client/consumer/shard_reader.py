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
import queue
import threading
import atomic
from enum import Enum
from datahub.exceptions import InvalidParameterException, DatahubException
from .message_key import MessageKey
from ..common.timer import Timer
from ..common.constant import Constant
from ..common.offset_meta import ConsumeOffset


class CompleteType(Enum):

    T_NORMAL    = "T_NORMAL",
    T_EXCEPTION = "T_EXCEPTION",
    T_DELAY     = "T_DELAY"


class CompleteFetch:

    def __init__(self, complete_type):
        self._complete_type = complete_type
        self._records = None
        self._exception = None
        self._timer = None

    @property
    def complete_type(self):
        return self._complete_type

    @property
    def records(self):
        if self._complete_type != CompleteType.T_NORMAL:
            raise DatahubException("CompleteType error. type: {}".format(self._complete_type))
        return self._records

    @records.setter
    def records(self, value):
        if self._complete_type != CompleteType.T_NORMAL:
            raise DatahubException("CompleteType error. type: {}".format(self._complete_type))
        self._records = value

    @property
    def exception(self):
        if self._complete_type != CompleteType.T_EXCEPTION:
            raise DatahubException("CompleteType error. type: {}".format(self._complete_type))
        return self._exception

    @exception.setter
    def exception(self, value):
        if self._complete_type != CompleteType.T_EXCEPTION:
            raise DatahubException("CompleteType error. type: {}".format(self._complete_type))
        self._exception = value

    @property
    def timer(self):
        if self._complete_type != CompleteType.T_DELAY:
            raise DatahubException("CompleteType error. type: {}".format(self._complete_type))
        return self._timer

    @timer.setter
    def timer(self, value):
        if self._complete_type != CompleteType.T_DELAY:
            raise DatahubException("CompleteType error. type: {}".format(self._complete_type))
        self._timer = value


class ShardReader:

    def __init__(self, project_name, topic_name, sub_id, message_reader, shard_id, offset, fetch_num):
        self._closed = False
        self._logger = logging.getLogger(ShardReader.__name__)

        self._project_name = project_name
        self._topic_name = topic_name
        self._sub_id = sub_id
        self._uniq_key = "{}:{}:{}".format(project_name, topic_name, sub_id)
        self._message_reader = message_reader
        self._shard_id = shard_id
        self._read_offset = offset
        self._fetch_num = fetch_num
        self._has_read_count = atomic.AtomicLong(0)

        self._read_lock = threading.Lock()
        self._fetch_lock = threading.Condition()
        self._cache_record_queue = queue.Queue()
        self._remain_records = atomic.AtomicLong(0)

    def close(self):
        self._closed = True
        self._logger.info("ShardReader closed. key: {}, shard_id: {}, read count: {}".format(self._uniq_key, self._shard_id, self._has_read_count.value))

    def read(self, timeout):
        if self._closed:
            self._logger.warning("ShardReader closed when read. key: {}, shard_id: {}".format(self._uniq_key, self._shard_id))
            raise DatahubException("ShardReader closed when read")

        record = self.__read_next(timeout)
        if record:
            offset = ConsumeOffset(record.sequence, record.system_time, record.batch_index)
            offset.next_cursor = self._read_offset.next_cursor
            record.record_key = MessageKey(self._shard_id, offset)
            self._has_read_count.get_and_set(self._has_read_count.value + 1)
        return record

    def reset_offset(self):
        self._read_offset.reset_timestamp(-1)

    @property
    def shard_id(self):
        return self._shard_id

    def __read_next(self, timeout):
        timer = Timer(max(timeout, Constant.MIN_TIMEOUT_WAIT_FETCH))
        with self._read_lock:
            record = None
            while not self._closed and record is None and not timer.is_expired():
                if self._remain_records.value > 0:
                    complete_fetch = self._cache_record_queue.get()
                    self._remain_records.get_and_set(self._remain_records.value-1)
                    if complete_fetch.complete_type == CompleteType.T_NORMAL:
                        return complete_fetch.records
                    elif complete_fetch.complete_type == CompleteType.T_EXCEPTION:
                        raise complete_fetch.exception
                    else:
                        try:
                            complete_fetch.timer.wait_expire()
                        except Exception as e:
                            raise e
                else:
                    self._message_reader.send_task(self.__gen_next_fetch_task, self.__deal_with_task)
                    with self._fetch_lock:
                        try:
                            self._fetch_lock.wait_for(self.__not_empty, timer.deadline_time-Timer.get_curr_time())
                        except Exception as e:
                            raise e
            return record

    def __not_empty(self):
        return not self._cache_record_queue.empty()

    def __gen_next_fetch_task(self):
        try:
            cursor = self._message_reader.get_cursor(self._shard_id, self._read_offset)
            if not cursor:
                raise InvalidParameterException("Get cursor is None. shard_id: {}, offset: {}".format(self._shard_id, self._read_offset.to_string()))
            record_result = self._message_reader.get_records(self._shard_id, cursor, self._fetch_num)
            return record_result
        except DatahubException as e:
            self._logger.warning("Generate fetch task fail. key: {}. DatahubException: {}".format(self._uniq_key, e))
            raise e
        except Exception as e:
            self._logger.warning("Generate fetch task fail. key: {}. Exception: {}".format(self._uniq_key, e))
            raise e

    def __deal_with_task(self, completed_task):
        with self._fetch_lock:
            if completed_task.exception():
                self.__push_with_exception(completed_task.exception())
            else:
                record_result = completed_task.result()
                if record_result.record_count > 0:
                    self.__push_with_records(record_result)
                else:
                    self.__push_with_delay(record_result, Constant.DELAY_TIMEOUT_FOR_READ_END)
            self._fetch_lock.notify_all()

    def __push_with_exception(self, exception):
        complete_fetch = CompleteFetch(CompleteType.T_EXCEPTION)
        complete_fetch.exception = exception
        self._cache_record_queue.put(complete_fetch)
        self._remain_records.get_and_set(self._remain_records.value + 1)
        self._read_offset.next_cursor = None
        self._logger.warning("Push to cache queue with exception. shard_id: {}, key: {}, exception: {}"
                             .format(self._shard_id, self._uniq_key, exception))

    def __push_with_records(self, record_result):
        for tmp_record in record_result.records:
            complete_fetch = CompleteFetch(CompleteType.T_NORMAL)
            complete_fetch.records = tmp_record
            self._cache_record_queue.put(complete_fetch)
        self._remain_records.get_and_set(self._remain_records.value + record_result.record_count)
        self._read_offset.next_cursor = record_result.next_cursor
        self._logger.debug("Push to cache queue with records. shard_id: {}, key: {}, record count: {}"
                           .format(self._shard_id, self._uniq_key, record_result.record_count))

    def __push_with_delay(self, record_result, timeout):
        complete_fetch = CompleteFetch(CompleteType.T_DELAY)
        complete_fetch.timer = Timer(timeout)
        self._cache_record_queue.put(complete_fetch)
        self._remain_records.get_and_set(self._remain_records.value + 1)
        self._read_offset.next_cursor = record_result.next_cursor
        self._logger.debug("Push to cache queue with delay. shard_id: {}, key: {}, delay timeout: {}"
                           .format(self._shard_id, self._uniq_key, timeout))
