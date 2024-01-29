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
import threading
from datahub.models import ShardState
from datahub.exceptions import DatahubException
from .shard_writer import ShardWriter


class ShardGroupWriter:

    def __init__(self, coordinator, shard_ids, producer_config):
        self._closed = False
        self._logger = logging.getLogger(ShardGroupWriter.__name__)

        self._shard_index = -1
        self._coordinator = coordinator
        self._coordinator.assign_shard_list = shard_ids if shard_ids else []
        self._producer_config = producer_config

        self._lock = threading.Lock()
        self._active_shard = []
        self._shard_writer_map = dict()

        self._coordinator.register_shard_change(self.on_shard_change)
        self._coordinator.register_remove_all_shards(self.on_remove_all_shards)

        self.__create_shard_writer_when_init(shard_ids)

    def close(self):
        self._closed = True

        with self._lock:
            for writer in self._shard_writer_map.values():
                writer.close()
            self._shard_writer_map.clear()
        self._logger.info("ShardGroupWriter close success. key: {}".format(self._coordinator.uniq_key))

    def on_shard_change(self, add_shards, del_shards):
        self.__create_shard_writer(add_shards)
        self.__remover_shard_writer(del_shards)

    def on_remove_all_shards(self):
        self.__remove_all_shard_writer()

    def write(self, records):
        if self._closed:
            self._logger.warning("ShardGroupWriter closed when write. key: {}".format(self._coordinator.uniq_key))
            raise DatahubException("ShardGroupWriter closed when write")

        self.__check_records(records)
        self._coordinator.update_shard_info()

        writer = self.__get_next_writer()
        writer.write(records)
        return writer.shard_id

    def write_async(self, records):
        if self._closed:
            self._logger.warning("ShardGroupWriter closed when write async. key: {}".format(self._coordinator.uniq_key))
            raise DatahubException("ShardGroupWriter closed when write async")

        self.__check_records(records)
        self._coordinator.update_shard_info()

        writer = self.__get_next_writer()
        return writer.write_async(records)

    def flush(self):
        if self._closed:
            self._logger.warning("ShardGroupWriter closed when flush. key: {}".format(self._coordinator.uniq_key))
            raise DatahubException("ShardGroupWriter closed when flush")

        self._logger.info("ShardGroupWriter flush start. key: {}".format(self._coordinator.uniq_key))
        with self._lock:
            for shard_writer in self._shard_writer_map.values():
                shard_writer.flush()
        self._logger.info("ShardGroupWriter flush end. key: {}".format(self._coordinator.uniq_key))

    def __check_records(self, records):
        for record in records:
            if record.shard_id or record.hash_key or record.partition_key:
                self._logger.warning("Client producer not support put record by special shardId, partitionKey, hashKey. key: {}, shardId: {}, partitionKey: {}, hashKey: {}"
                                     .format(self._coordinator.uniq_key, record.shard_id, record.partition_key, record.hash_key))
                raise DatahubException("Client producer not support put record by special shardId, partitionKey, hashKey")

    def __create_shard_writer_when_init(self, shard_ids):
        if shard_ids:
            self.__create_shard_writer(shard_ids)
            return

        shard_meta_map = self._coordinator.meta_data.shard_meta_map
        ready_shards = [shard for shard, shard_meta in shard_meta_map.items() if shard_meta.shard_state == ShardState.ACTIVE]
        self.__create_shard_writer(ready_shards)
    
    def __create_shard_writer(self, shard_ids):
        with self._lock:
            try:
                shard_meta_map = self._coordinator.meta_data.shard_meta_map
                for shard_id in shard_ids:
                    if shard_id not in self._shard_writer_map:
                        shard_meta = shard_meta_map.get(shard_id)
                        if not shard_meta or shard_meta.shard_state != ShardState.ACTIVE:
                            self._logger.warning("ShardWriter create fail. May the shard is not active. key: {}. shard_id: {}"
                                                 .format(self._coordinator.uniq_key, shard_id))
                            raise DatahubException("ShardWriter create fail. May the shard is not active")
                        self._active_shard.append(shard_id)
                        self._shard_writer_map[shard_id] = ShardWriter(
                            self._coordinator.project_name,
                            self._coordinator.topic_name,
                            self._coordinator.sub_id,
                            self._coordinator.meta_data.message_writer,
                            self._producer_config,
                            shard_id
                        )
                        self._logger.info("ShardWriter create success. key: {}, shard_id: {}".format(self._coordinator.uniq_key, shard_id))
            except Exception as e:
                self._logger.warning("ShardWriter create fail. key: {}. shard_id: {}, {}".format(self._coordinator.uniq_key, shard_id, e))
                raise e

    def __remover_shard_writer(self, shard_ids):
        with self._lock:
            try:
                for shard_id in shard_ids:
                    if shard_id in self._shard_writer_map:
                        self._active_shard.pop(shard_id)
                        self._shard_writer_map[shard_id].close()
                        self._shard_writer_map.pop(shard_id)
                    self._logger.info("ShardWriter remove success. key: {}, shard_id: {}".format(self._coordinator.uniq_key, shard_id))
            except Exception as e:
                self._logger.warning("ShardWriter remove fail. key: {}. shard_id: {}, {}".format(self._coordinator.uniq_key, shard_id, e))
                raise e

    def __remove_all_shard_writer(self):
        with self._lock:
            try:
                for shard_id in set(self._shard_writer_map.keys()):
                    self._active_shard.pop(shard_id)
                    self._shard_writer_map[shard_id].close()
                    self._shard_writer_map.pop(shard_id)
                    self._logger.info("ShardWriter remove success when remove all. key: {}, shard_id: {}".format(self._coordinator.uniq_key, shard_id))
            except Exception as e:
                self._logger.warning("ShardWriter remove fail when remove all. key: {}. shard_id: {}, {}".format(self._coordinator.uniq_key, shard_id, e))
                raise e

    def __get_next_writer(self):
        shard_id = self.__get_next_shard()
        with self._lock:
            writer = self._shard_writer_map.get(shard_id)
            if not writer:
                self._logger.warning("ShardWriter not found. key: {}, shard_id: {}".format(self._coordinator.uniq_key, shard_id))
                raise DatahubException("ShardWriter not found")
            return writer

    def __get_next_shard(self):
        if len(self._active_shard) > 0:
            self._shard_index = (self._shard_index + 1) % len(self._active_shard)
            return self._active_shard[self._shard_index]
        else:
            if self._coordinator.is_user_shard_assign():
                self._logger.warning("No active shard found. May the specified shards all closed. key: {}, assign shards: {}".format(self._coordinator.uniq_key, self._coordinator.assign_shard_list))
            else:
                self._logger.warning("No active shard found. May topic has do split or merge, please retry. key: {}".format(self._coordinator.uniq_key))
            raise DatahubException("No active shard found")

