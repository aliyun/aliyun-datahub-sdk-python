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
from datahub.exceptions import *
from .shard_reader import ShardReader
from .offset_select_strategy import OffsetSelectStrategy
from ..common.timer import Timer
from ..common.constant import Constant


class ShardGroupReader:

    def __init__(self, coordinator, shard_ids, timestamp):
        self._closed = False
        self._logger = logging.getLogger(ShardGroupReader.__name__)

        self._coordinator = coordinator
        self._coordinator.assign_shard_list = shard_ids if shard_ids else []
        self._shard_reader_map = dict()
        self._select_strategy = OffsetSelectStrategy()

        self._lock = threading.Lock()
        self._coordinator.register_shard_change(self.on_shard_change)
        self._coordinator.register_remove_all_shards(self.on_remove_all_shards)

        self.__create_shard_reader(shard_ids, timestamp)

    def close(self):
        self._closed = True
        with self._lock:
            for reader in self._shard_reader_map.values():
                reader.close()
            self._shard_reader_map.clear()
        self._logger.info("ShardGroupReader close success. key: {}".format(self._coordinator.uniq_key))

    def on_shard_change(self, add_shards, del_shards):
        self.__create_shard_reader(add_shards, -1)
        self.__remover_shard_reader(del_shards)

    def on_remove_all_shards(self):
        self.__remove_all_shard_reader()

    def read(self, shard_id, time_out):
        if self._closed:
            self._logger.warning("ShardGroupReader closed when read. key: {}".format(self._coordinator.uniq_key))
            raise DatahubException("ShardGroupReader closed when read")

        record = None
        timer = Timer(time_out)
        while not self._closed and record is None and not timer.is_expired():
            if self._coordinator.waiting_shard_assign():
                timer.wait_expire(Constant.DELAY_TIMEOUT_FOR_NOT_READY)
            else:
                self._coordinator.update_shard_info()

                with self._lock:
                    reader = self.__get_next_reader(shard_id)
                if reader is None:
                    timer.wait_expire(Constant.DELAY_TIMEOUT_FOR_NOT_READY)
                else:
                    record = self.__read_by_reader(reader)
        return record

    def __read_by_reader(self, reader):
        record = None
        try:
            record = reader.read(1)
            self._select_strategy.after_read(reader.shard_id, record)
            if record:
                self._coordinator.send_record_offset(record.record_key)
                if self._coordinator.auto_ack_offset:
                    record.record_key.ack()
        except ShardSealedException as e:  # error_code: 'InvalidShardOperation'
            self._logger.warning("Read fail. Shard read end. shard_id: {}, key: {}, {}"
                                 .format(reader.shard_id, self._coordinator.uniq_key, e))
            self._coordinator.on_shard_read_end([reader.shard_id])
        except InvalidCursorException as e:  # error_code: 'InvalidCursor'
            self._logger.warning("Read fail. Invalid cursor. shard_id: {}, key: {}, {}"
                                 .format(reader.shard_id, self._coordinator.uniq_key, e))
            reader.reset_offset()
        except DatahubException as e:
            self._logger.warning("Read fail. shard_id: {}, key: {}. DatahubException: {}"
                                 .format(reader.shard_id, self._coordinator.uniq_key, e))
            raise e
        except Exception as e:
            self._logger.warning("Read fail. shard_id: {}, key: {}. Exception: {}"
                                 .format(reader.shard_id, self._coordinator.uniq_key, e))
            raise e
        return record

    def __create_shard_reader(self, shard_ids, timestamp=-1):
        with self._lock:
            try:
                if shard_ids is None or len(shard_ids) == 0:
                    return
                shard_meta_map = self._coordinator.meta_data.shard_meta_map
                shards_offset_map = self.__gen_shards_offset(shard_ids, timestamp)
                for shard_id in shard_ids:
                    shard_meta = shard_meta_map.get(shard_id)
                    if shard_meta is None:
                        raise InvalidParameterException("Shard not found. key: {}, shard_id: {}".format(self._coordinator.uniq_key, shard_id))
                    if shard_id in self._shard_reader_map:
                        continue
                    consume_offset = shards_offset_map.get(shard_id)
                    reader = ShardReader(self._coordinator.project_name, self._coordinator.topic_name, self._coordinator.sub_id,
                                         self._coordinator.meta_data.message_reader, shard_id, consume_offset, self._coordinator.fetch_limit)
                    self._shard_reader_map[shard_id] = reader
                    self._select_strategy.add_shard(shard_id)
                    self._logger.info("ShardReader created. key: {}, shard_id: {}, sequence: {}".format(self._coordinator.uniq_key, shard_id, consume_offset.sequence))
            except DatahubException as e:
                self._logger.warning("ShardReader create fail. key: {}, shard_ids: {}, DatahubException: {}".format(self._coordinator.uniq_key, shard_ids, e))
                raise e
            except Exception as e:
                self._logger.warning("ShardReader create fail. key: {}, shard_ids: {}, {}".format(self._coordinator.uniq_key, shard_ids, e))
                raise e

    def __remover_shard_reader(self, shard_ids):
        with self._lock:
            if shard_ids is None or len(shard_ids) == 0:
                return
            for shard_id in shard_ids:
                if shard_id in self._shard_reader_map:
                    self._shard_reader_map[shard_id].close()
                    self._shard_reader_map.pop(shard_id)
                self._select_strategy.remove_shard(shard_id)
                self._logger.info("ShardReader removed. key: {}, shard_id: {}".format(self._coordinator.uniq_key, shard_id))

    def __remove_all_shard_reader(self):
        with self._lock:
            for shard_id in set(self._shard_reader_map.keys()):
                self._shard_reader_map[shard_id].close()
                self._shard_reader_map.pop(shard_id)
                self._select_strategy.remove_shard(shard_id)
                self._logger.info("ShardReader removed when remove all. key: {}, shard_id: {}".format(self._coordinator.uniq_key, shard_id))

    def __gen_shards_offset(self, shard_ids, timestamp=-1):
        offset_map = self._coordinator.init_and_get_offset(shard_ids)
        if timestamp != -1:
            for shard_id in offset_map:
                offset_map[shard_id].reset_timestamp(timestamp)
        return offset_map

    def __get_next_reader(self, shard_id):
        if shard_id:
            reader = self._shard_reader_map.get(shard_id)
            if not reader:
                raise DatahubException("ShardReader not found. key: {}, shard_id: {}".format(self._coordinator.uniq_key, shard_id))
            return reader
        next_shard = self._select_strategy.get_next_shard()
        if next_shard:
            return self.__get_next_reader(next_shard)
        if len(self._shard_reader_map) == 0:
            self._logger.warning("No ShardReader found. May the consumer group in rebalance state. key: {}".format(self._coordinator.uniq_key))
            return None
        return next(iter(self._shard_reader_map.values()))
