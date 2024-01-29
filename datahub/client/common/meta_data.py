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
import atomic
from datahub.models import RecordType
from datahub.exceptions import DatahubException
from .timer import Timer
from .constant import Constant
from .datahub_factory import DatahubFactory
from ..consumer.message_reader import MessageReader
from ..producer.message_writer import MessageWriter


class MetaData:

    def __init__(self, key, project_name, topic_name, sub_id, common_config):
        self._class_key = key
        self._logger = logging.getLogger(MetaData.__name__)
        self._endpoint = common_config.endpoint
        self._datahub_client = DatahubFactory.create_datahub_client(common_config)

        # coordinators set
        self._coordinators = set()

        # Update topic
        self._topic_meta = self.__init_topic_meta(project_name, topic_name)

        # Update shard
        self._updating = atomic.AtomicLong(0)
        self._shard_meta_map = dict()
        self._timer = Timer(Constant.SHARD_META_REFRESH_TIMEOUT)
        self.__update_shard_meta_once()

        # pub / sub
        thread_num = max(min(common_config.async_thread_limit, Constant.MAX_ASYNC_THREAD_LIMIT), Constant.MIN_ASYNC_THREAD_LIMIT)
        queue_limit = common_config.thread_queue_limit
        if sub_id:
            self._message_reader, self._message_writer = MessageReader(self, queue_limit, thread_num), None
        else:
            self._message_reader, self._message_writer = None, MessageWriter(self, queue_limit, thread_num)

    def close(self):
        if self._message_writer:
            self._message_writer.close()
        if self._message_reader:
            self._message_reader.close()

    def update_shard_meta(self):
        if self._timer.is_expired():
            try:
                self.__update_shard_meta_once()
            except DatahubException as e:
                self._logger.warning("ShardCoordinator update shard meta fail. key: {}. Exception: {}".format(self._class_key, e))

    def register(self, coordinator):
        self._coordinators.add(coordinator)

    def unregister(self, coordinator):
        self._coordinators.remove(coordinator)
        return len(self._coordinators)

    def __init_topic_meta(self, project_name, topic_name):
        try:
            get_topic_result = self._datahub_client.get_topic(project_name, topic_name)
            return TopicMeta(project_name, topic_name, get_topic_result.record_type, get_topic_result.record_schema)
        except DatahubException as e:
            self._logger.warning("Init topic meta fail. key: {}, DatahubException: {}".format(self._class_key, e))
            raise e
        except Exception as e:
            self._logger.warning("Init topic meta fail. key: {}, {}".format(self._class_key, e))
            raise e

    def __update_shard_meta_once(self):
        if self._updating.compare_and_set(0, 1):
            new_shard_map = dict()
            try:
                list_shard_result = self._datahub_client.list_shard(self._topic_meta.project_name, self._topic_meta.topic_name)
                for shard in list_shard_result.shards:
                    new_shard_map[shard.shard_id] = ShardMeta(shard.shard_id, self._endpoint, shard.state, list_shard_result.protocol)
            except DatahubException as e:
                self._logger.warning("Update shard meta fail. key: {}, DatahubException: {}".format(self._class_key, e))
                raise e
            except Exception as e:
                self._logger.warning("Update shard meta fail. key: {}, {}".format(self._class_key, e))
                raise e

            new_add = [k for k in new_shard_map if k not in self._shard_meta_map]
            new_del = [k for k in self._shard_meta_map if k not in new_shard_map]

            if len(new_add) > 0 or len(new_del) > 0:
                self._logger.debug("Shard changed when update shard meta. key: {}, new_add: {}, new_del: {}".format(self._class_key, new_add, new_del))
                self._shard_meta_map = new_shard_map
                for coordinator in self._coordinators:
                    coordinator.on_shard_meta_change(new_add, new_del)

            self._timer.reset()
            self._logger.debug("Update shard meta success. key: {}".format(self._class_key))
            self._updating.compare_and_set(1, 0)

    @property
    def class_key(self):
        return self._class_key

    @property
    def shard_meta_map(self):
        return self._shard_meta_map

    @property
    def topic_meta(self):
        return self._topic_meta

    @property
    def datahub_client(self):
        return self._datahub_client

    @property
    def message_reader(self):
        return self._message_reader

    @property
    def message_writer(self):
        return self._message_writer


class TopicMeta:
    def __init__(self, project_name, topic_name, record_type, record_schema):
        self._project_name = project_name
        self._topic_name = topic_name
        self._record_type = RecordType(record_type)
        self._record_schema = record_schema

    @property
    def project_name(self):
        return self._project_name

    @property
    def topic_name(self):
        return self._topic_name

    @property
    def record_type(self):
        return self._record_type

    @property
    def record_schema(self):
        return self._record_schema


class ShardMeta:
    def __init__(self, shard_id, address, shard_state, protocol):
        self._shard_id = shard_id
        self._address = address
        self._shard_state = shard_state
        self._protocol = protocol

    @property
    def shard_id(self):
        return self._shard_id

    @property
    def address(self):
        return self._address

    @property
    def shard_state(self):
        return self._shard_state

    @property
    def protocol(self):
        return self._protocol
