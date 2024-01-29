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


import atomic
from datahub.exceptions import DatahubException
from .offset_manager import OffsetManager
from ..common.offset_meta import ConsumeOffset
from ..common.shard_coordinator import ShardCoordinator


class OffsetCoordinator(ShardCoordinator):

    def __init__(self, project_name, topic_name, sub_id, consumer_config):
        super().__init__(project_name, topic_name, sub_id, consumer_config)
        self._sub_session_changed = False
        self._sub_offline = False
        self._sub_deleted = False
        self._offset_not_ack = False
        self._offset_reset = atomic.AtomicLong(0)

        self._auto_ack_offset = consumer_config.auto_ack_offset
        self._max_record_buffer_size = consumer_config.max_record_buffer_size
        self._fetch_limit = consumer_config.fetch_limit

        self._offset_manager = OffsetManager(self)

    def close(self):
        super().close()
        self._offset_manager.close()
        self._logger.info("OffsetCoordinator close success. key: {}".format(self._uniq_key))

    def update_shard_info(self):
        if self._sub_deleted:
            raise DatahubException("Subscription has been deleted. key: {}".format(self._uniq_key))
        if self._sub_session_changed:
            raise DatahubException("Subscription session has changed. key: {}".format(self._uniq_key))
        if self._sub_offline:
            raise DatahubException("Subscription offline. key: {}".format(self._uniq_key))
        if self._offset_not_ack:
            raise DatahubException("Offset has not been updated for a long time. key: {}".format(self._uniq_key))
        super().update_shard_info()

    def on_shard_read_end(self, shard_ids):
        self._do_shard_change(None, shard_ids)

    def on_offset_reset(self):
        if self._offset_reset.compare_and_set(0, 1):
            if self.is_user_shard_assign():
                self._do_shard_change(self._assign_shard_list, self._assign_shard_list)
            self._offset_reset.compare_and_set(1, 0)

    def waiting_shard_assign(self):
        return False

    def init_and_get_offset(self, shard_ids):
        client = self._meta_data.datahub_client
        try:
            init_result = client.init_and_get_subscription_offset(self._project_name, self._topic_name, self._sub_id, shard_ids)
            consume_offset_map = dict()
            for shard_id, offset in init_result.offsets.items():
                consume_offset_map[shard_id] = ConsumeOffset(
                    sequence=offset.sequence if offset.sequence < 0 else offset.sequence + 1,
                    timestamp=offset.timestamp,
                    batch_index=offset.batch_index,
                    version_id=offset.version,
                    session_id=offset.session_id
                )
                self._logger.info("Init and get offset once success. key: {}, shard_id: {}, offset: {}".format(self._uniq_key, shard_id, offset))
            self._offset_manager.set_offset_meta(consume_offset_map)
            return consume_offset_map
        except DatahubException as e:
            self._logger.warning("Init and get subscription offset fail. key: {}, {}".format(self._uniq_key, e))
            raise e

    def send_record_offset(self, message_key):
        self._offset_manager.send_record_offset(message_key)

    def on_sub_offline(self):
        self._sub_offline = True

    def on_sub_session_changed(self):
        self._sub_session_changed = True

    def on_sub_deleted(self):
        self._sub_deleted = True

    def on_offset_not_ack(self):
        self._offset_not_ack = True

    @property
    def auto_ack_offset(self):
        return self._auto_ack_offset

    @property
    def fetch_limit(self):
        return self._fetch_limit

    @property
    def max_record_buffer_size(self):
        return self._max_record_buffer_size
