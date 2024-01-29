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
from datahub import DatahubProtocolType
from datahub.models import CompressFormat


class Constant:

    # CommonConfig
    DEFAULT_LOGING_LEVEL = logging.INFO
    DEFAULT_LOGING_FILENAME = "./DatahubClient.log"
    DEFAULT_PROTOCOL_TYPE = DatahubProtocolType.PB
    DEFAULT_COMPRESS_FORMAT = CompressFormat.LZ4
    DEFAULT_RETRY_TIMES = 3
    DEFAULT_ASYNC_THREAD_LIMIT = 16
    DEFAULT_THREAD_QUEUE_LIMIT = 1024

    # ConsumerConfig
    DEFAULT_AUTO_ACK_OFFSET = True
    DEFAULT_SESSION_TIMEOUT = 6000
    DEFAULT_MAX_RECORD_BUFFER_SIZE = 100
    DEFAULT_FETCH_LIMIT = 1000

    MIN_ASYNC_THREAD_LIMIT = 2                       # MessageReader/MessageWriter 线程池数量
    MAX_ASYNC_THREAD_LIMIT = 100

    # ProducerConfig
    MAX_ASYNC_BUFFER_TIMEOUT_S = 1
    MAX_ASYNC_BUFFER_SIZE = 4000000
    MAX_ASYNC_BUFFER_RECORD_COUNT = 10000
    MAX_RECORD_PACK_QUEUE_LIMIT = 1024


    # MetaData
    SHARD_META_REFRESH_TIMEOUT = 3              # 更新ShardMeta的最小间隔时间

    # ConsumerCoordinator
    MAX_JOIN_GROUP_TIMEOUT = 5

    # Heartbeat
    MIN_HEARTBEAT_INTERVAL_TIMEOUT = 5

    # OffsetManager
    OFFSET_COMMIT_INTERVAL_TIMEOUT = 5          # 提交点位时间间隔
    NOT_ACK_WARNING_TIMEOUT = 10                # 经过多久还没有ack认定为点位提交失败
    OFFSET_CHECK_TIMEOUT = 1                    # 点位提交间歇的等待时间
    FORCE_COMMIT_TIMEOUT = 4                    # 强制提交点位时间限制

    # ShardSelector
    READER_SELECT_EMPTY_SHARD_TIMEOUT = 10

    # ShardGroupReader
    DELAY_TIMEOUT_FOR_NOT_READY = 2

    # ShardReader
    DELAY_TIMEOUT_FOR_READ_END = 2
    MIN_TIMEOUT_WAIT_FETCH = 2
