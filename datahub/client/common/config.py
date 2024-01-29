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


from .constant import Constant


class Utils:

    DATE_FMT = "[%Y-%m-%d %H:%M:%S]"
    FORMAT = "%(asctime)s|%(levelname)7s｜%(threadName)10s|%(message)s"


class DatahubConfig:

    __slots__ = '_access_id', '_access_key', '_endpoint', '_protocol_type', '_compress_format'

    def __init__(self, access_id, access_key, endpoint, protocol_type, compress_format):
        self._access_id = access_id
        self._access_key = access_key
        self._endpoint = endpoint
        self._protocol_type = protocol_type
        self._compress_format = compress_format

    @property
    def access_id(self):
        return self._access_id

    @access_id.setter
    def access_id(self, value):
        self._access_id = value

    @property
    def access_key(self):
        return self._access_key

    @access_key.setter
    def access_key(self, value):
        self._access_key = value

    @property
    def endpoint(self):
        return self._endpoint

    @endpoint.setter
    def endpoint(self, value):
        self._endpoint = value

    @property
    def protocol_type(self):
        return self._protocol_type

    @protocol_type.setter
    def protocol_type(self, value):
        self._protocol_type = value

    @property
    def compress_format(self):
        return self._compress_format

    @compress_format.setter
    def compress_format(self, value):
        self._compress_format = value


class CommonConfig(DatahubConfig):

    __slots__ = '_retry_times', '_async_thread_limit', '_thread_queue_limit', '_logging_level', '_logging_filename'

    def __init__(self, access_id, access_key, endpoint, protocol_type, compress_format):
        super().__init__(access_id, access_key, endpoint, protocol_type, compress_format)
        self._retry_times = Constant.DEFAULT_RETRY_TIMES
        self._async_thread_limit = Constant.DEFAULT_ASYNC_THREAD_LIMIT
        self._thread_queue_limit = Constant.DEFAULT_THREAD_QUEUE_LIMIT
        self._logging_level = Constant.DEFAULT_LOGING_LEVEL
        self._logging_filename = Constant.DEFAULT_LOGING_FILENAME

    @property
    def retry_times(self):
        return self._retry_times

    @retry_times.setter
    def retry_times(self, value):
        self._retry_times = value

    @property
    def async_thread_limit(self):
        return self._async_thread_limit

    @async_thread_limit.setter
    def async_thread_limit(self, value):
        self._async_thread_limit = value

    @property
    def thread_queue_limit(self):
        return self._thread_queue_limit

    @thread_queue_limit.setter
    def thread_queue_limit(self, value):
        self._thread_queue_limit = value

    @property
    def logging_level(self):
        return self._logging_level

    @logging_level.setter
    def logging_level(self, value):
        self._logging_level = value

    @property
    def logging_filename(self):
        return self._logging_filename

    @logging_filename.setter
    def logging_filename(self, value):
        self._logging_filename = value


class ConsumerConfig(CommonConfig):
    """
    Config for datahub producer

    Members:
        access_id (:class:`string`): Aliyun access id

        access_key (:class:`string`): Aliyun access key

        endpoint (:class:`string`): Datahub endpoint

        protocol_type (:class:`datahub.core.DatahubProtocolType`): Protocol type for datahub client

        compress_format (:class:`datahub.models.compress.CompressFormat`): Compress format for records data

        retry_times (:class:`int`): Retry times when request error

        async_thread_limit (:class:`int`): Thread num limit for thread pool in message reader

        thread_queue_limit (:class:`int`): Task num limit for queue in thread pool in message reader

        logging_level (:class:`int`): Logging level

        logging_filename (:class:`string`): Logging file name

        auto_ack_offset (:class:`bool`): Auto ack offset for fetched records or not

        session_timeout (:class:`int`): Session timeout

        max_record_buffer_size (:class:`int`): Max record buffer size in consumer

        fetch_limit (:class:`int`): Fetch num limit need to consume
    """

    __slots__ = '_auto_ack_offset', '_session_timeout', '_max_record_buffer_size', '_fetch_limit'

    def __init__(self, access_id, access_key, endpoint, protocol_type=Constant.DEFAULT_PROTOCOL_TYPE,
                 compress_format=Constant.DEFAULT_COMPRESS_FORMAT):
        super().__init__(access_id, access_key, endpoint, protocol_type, compress_format)
        self._auto_ack_offset = Constant.DEFAULT_AUTO_ACK_OFFSET
        self._session_timeout = Constant.DEFAULT_SESSION_TIMEOUT
        self._max_record_buffer_size = Constant.DEFAULT_MAX_RECORD_BUFFER_SIZE
        self._fetch_limit = Constant.DEFAULT_FETCH_LIMIT

    @property
    def auto_ack_offset(self):
        return self._auto_ack_offset

    @auto_ack_offset.setter
    def auto_ack_offset(self, value):
        self._auto_ack_offset = value

    @property
    def session_timeout(self):
        return self._session_timeout

    @session_timeout.setter
    def session_timeout(self, value):
        self._session_timeout = value

    @property
    def max_record_buffer_size(self):
        return self._max_record_buffer_size

    @max_record_buffer_size.setter
    def max_record_buffer_size(self, value):
        self._max_record_buffer_size = value

    @property
    def fetch_limit(self):
        return self._fetch_limit

    @fetch_limit.setter
    def fetch_limit(self, value):
        self._fetch_limit = value


class ProducerConfig(CommonConfig):
    """
    Config for datahub producer

    Members:
        access_id (:class:`string`): Aliyun access id

        access_key (:class:`string`): Aliyun access key

        endpoint (:class:`string`): Datahub endpoint

        protocol_type (:class:`datahub.core.DatahubProtocolType`): Protocol type for datahub client

        compress_format (:class:`datahub.models.compress.CompressFormat`): Compress format for records data

        retry_times (:class:`int`): Retry times when request error

        async_thread_limit (:class:`int`): Thread num limit for thread pool in message writer

        thread_queue_limit (:class:`int`): Task num limit for queue in thread pool in message writer

        logging_level (:class:`int`): Logging level

        logging_filename (:class:`string`): Logging file name

        max_async_buffer_records (:class:`int`): Max buffer records number to PutRecords once. Only valid when write async.

        max_async_buffer_size (:class:`int`): Max buffer size to PutRecords once. Only valid when write async.

        max_async_buffer_time (:class:`int`): Max buffer time to PutRecords once. Only valid when write async.

        max_record_pack_queue_limit (:class:`int`): Max ready record pack limit for queue. Only valid when write async.
    """

    __slots__ = '_max_async_buffer_records', '_max_async_buffer_size',\
                '_max_async_buffer_time', '_max_record_pack_queue_limit'

    def __init__(self, access_id, access_key, endpoint, protocol_type=Constant.DEFAULT_PROTOCOL_TYPE,
                 compress_format=Constant.DEFAULT_COMPRESS_FORMAT):
        super().__init__(access_id, access_key, endpoint, protocol_type, compress_format)
        self._max_async_buffer_records = Constant.MAX_ASYNC_BUFFER_RECORD_COUNT
        self._max_async_buffer_size = Constant.MAX_ASYNC_BUFFER_SIZE
        self._max_async_buffer_time = Constant.MAX_ASYNC_BUFFER_TIMEOUT_S
        self._max_record_pack_queue_limit = Constant.MAX_RECORD_PACK_QUEUE_LIMIT

    @property
    def max_async_buffer_records(self):
        return self._max_async_buffer_records

    @max_async_buffer_records.setter
    def max_async_buffer_records(self, value):
        self._max_async_buffer_records = min(value, Constant.MAX_ASYNC_BUFFER_RECORD_COUNT)

    @property
    def max_async_buffer_size(self):
        return self._max_async_buffer_size

    @max_async_buffer_size.setter
    def max_async_buffer_size(self, value):
        self._max_async_buffer_size = min(value, Constant.MAX_ASYNC_BUFFER_SIZE)

    @property
    def max_async_buffer_time(self):
        return self._max_async_buffer_time

    @max_async_buffer_time.setter
    def max_async_buffer_time(self, value):
        self._max_async_buffer_time = min(value, Constant.MAX_ASYNC_BUFFER_TIMEOUT_S)

    @property
    def max_record_pack_queue_limit(self):
        return self._max_record_pack_queue_limit

    @max_record_pack_queue_limit.setter
    def max_record_pack_queue_limit(self, value):
        self._max_record_pack_queue_limit = value
