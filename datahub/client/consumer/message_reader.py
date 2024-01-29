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
from datahub.exceptions import DatahubException, InvalidParameterException, SeekOutOfRangeException
from datahub.models import RecordType, CursorType
from ..common.thread_pool import ThreadPool


class MessageReader:

    def __init__(self, meta_data, queue_limit_num, threads_num):
        self._meta_data = meta_data
        self._logger = logging.getLogger(MessageReader.__name__)
        self._executor = ThreadPool(queue_limit_num, threads_num, "MessageReader")

    def send_task(self, task, callback, *args, **kwargs):
        self._executor.submit(task, *args, **kwargs).add_done_callback(callback)

    def get_cursor(self, shard_id, offset):
        if offset.next_cursor:
            return offset.next_cursor

        cursor_type, parm = CursorType.OLDEST, -1
        cursor = None
        if offset.sequence != -1:
            cursor_type, parm = CursorType.SEQUENCE, offset.sequence
            cursor = self.__get_cursor_once(shard_id, cursor_type, parm)
        if not cursor and offset.timestamp != -1:
            cursor_type, parm = CursorType.SYSTEM_TIME, offset.timestamp
            cursor = self.__get_cursor_once(shard_id, cursor_type, parm)
        if not cursor:
            cursor_type, parm = CursorType.OLDEST, -1
            cursor = self.__get_cursor_once(shard_id, cursor_type, parm)

        if not cursor:
            self._logger.warning("Init cursor failed. key: {}, shard_id: {}, cursor type: {}, parm: {}"
                                 .format(self._meta_data.class_key, shard_id, cursor_type, parm))
            raise DatahubException("Get cursor fail. key: {}, shard_id: {}".format(self._meta_data.class_key, shard_id))

        self._logger.info("Init cursor success. key: {}, shard_id: {}, cursor type: {}, parm: {}, cursor: {}"
                          .format(self._meta_data.class_key, shard_id, cursor_type, parm, cursor))
        return cursor

    def get_records(self, shard_id, cursor, fetch_limit):
        topic_meta = self._meta_data.topic_meta
        datahub_client = self._meta_data.datahub_client

        if topic_meta.record_type == RecordType.TUPLE:
            try:
                return datahub_client.get_tuple_records(topic_meta.project_name, topic_meta.topic_name, shard_id,
                                                        topic_meta.record_schema, cursor, fetch_limit)
            except DatahubException as e:
                self._logger.warning("Get TUPLE record fail. shard_id: {}, cursor: {}, DatahubException: {}".format(shard_id, cursor, e))
                raise e
            except Exception as e:
                self._logger.warning("Get TUPLE record fail. shard_id: {}, cursor: {}, {}".format(shard_id, cursor, e))
                raise e
        elif topic_meta.record_type == RecordType.BLOB:
            try:
                return datahub_client.get_blob_records(topic_meta.project_name, topic_meta.topic_name, shard_id,
                                                       cursor, fetch_limit)
            except DatahubException as e:
                self._logger.warning("Get BLOB record fail. shard_id: {}, cursor: {}, DatahubException: {}".format(shard_id, cursor, e))
                raise e
            except Exception as e:
                self._logger.warning("Get BLOB record fail. shard_id: {}, cursor: {}, {}".format(shard_id, cursor, e))
                raise e
        else:
            self._logger.warning("Invalid record type, should be TUPLE or BLOB!")
            raise InvalidParameterException("Invalid record type")

    def close(self):
        self._executor.shutdown()

    def __get_cursor_once(self, shard_id, cursor_type, value):
        topic_meta = self._meta_data.topic_meta
        datahub_client = self._meta_data.datahub_client

        try:
            cursor_result = datahub_client.get_cursor(topic_meta.project_name, topic_meta.topic_name,
                                                      shard_id, cursor_type, value)
            return cursor_result.cursor
        except SeekOutOfRangeException as e:
            self._logger.warning("Get cursor fail. key: {}, shard_id: {}, cursor type: {}, value: {}, {}"
                                 .format(self._meta_data.class_key, shard_id, cursor_type, value, e))
            return None
        except Exception as e:
            self._logger.warning("Get cursor fail. key: {}, shard_id: {}, cursor_type: {}, value: {}, {}"
                                 .format(self._meta_data.class_key, shard_id, cursor_type, value, e))
            raise e
