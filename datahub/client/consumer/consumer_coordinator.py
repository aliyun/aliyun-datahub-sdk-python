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


from rwlock import RWLock
from datahub.exceptions import SubscriptionOfflineException, DatahubException, TimeoutException
from .sync_group_meta import SyncGroupMeta
from .consumer_heartbeat import ConsumerHeartbeat
from .offset_coordinator import OffsetCoordinator
from ..common.timer import Timer
from ..common.constant import Constant


class ConsumerCoordinator(OffsetCoordinator):

    def __init__(self, project_name, topic_name, sub_id, consumer_config):
        super(ConsumerCoordinator, self).__init__(project_name, topic_name, sub_id, consumer_config)
        self._wr_lock = RWLock()
        self._consumer_id = None
        self._version_id = None
        self._heart_beat = None
        self._session_timeout = consumer_config.session_timeout
        self._sync_group_meta = SyncGroupMeta()

        self.__join_group_and_start_heartbeat()

    def close(self):
        super(ConsumerCoordinator, self).close()
        with self._wr_lock.writer_lock:
            self.__leave_group_and_stop_heartbeat()
        self._logger.info("ConsumerCoordinator close success. key: {}".format(self._uniq_key))

    def on_shard_change(self, add_shards, del_shards):
        self._do_shard_change(add_shards, del_shards)
        if self._offset_manager:
            self._offset_manager.on_shard_release(del_shards)
        if self._sync_group_meta:
            self._sync_group_meta.on_shard_release(del_shards)

    def on_shard_read_end(self, shard_ids):
        super(ConsumerCoordinator, self).on_shard_read_end(shard_ids)
        if self._sync_group_meta:
            self._sync_group_meta.on_shard_read_end(shard_ids)

    def on_offset_reset(self):
        if self._offset_reset.compare_and_set(0, 1):
            self._do_remove_all_shards()
            if self._offset_manager:
                self._offset_manager.on_offset_reset()
            self._offset_reset.compare_and_set(1, 0)

    def waiting_shard_assign(self):
        if self._closed:
            self._logger.warning("ConsumerCoordinator closed. key: {}".format(self._uniq_key))
            raise DatahubException("ConsumerCoordinator closed")

        with self._wr_lock.reader_lock:
            return self._heart_beat is None or self._heart_beat.waiting_shard_assign()

    def update_shard_info(self):
        super(ConsumerCoordinator, self).update_shard_info()
        with self._wr_lock.writer_lock:
            if self._heart_beat is None or self._heart_beat.need_rejoin():
                self.__leave_group_and_stop_heartbeat()
                self.__join_group_and_start_heartbeat()
            self.__sync_group()

    def __join_group_and_start_heartbeat(self):
        self.__join_group()
        self.__start_heartbeat()
        self._logger.info("Join group and start heartbeat success. key: {}".format(self.uniq_key))

    def __leave_group_and_stop_heartbeat(self):
        self.__leave_group()
        self.__stop_heartbeat()
        self._logger.info("Leave group and stop heartbeat success. key: {}".format(self.uniq_key))

    def __sync_group(self):
        if self._sync_group_meta and self._sync_group_meta.need_sync_group():
            release = list(self._sync_group_meta.release_shards)
            read_end = list(self._sync_group_meta.read_end_shards)
            try:
                self._meta_data.datahub_client.sync_group(
                    self._project_name,
                    self._topic_name,
                    self._sub_id,
                    self._consumer_id,
                    self._version_id,
                    release,
                    read_end
                )
                self._sync_group_meta.clear_shard_release()
                self._logger.debug("SyncGroup success. key: {}, release: {}, read end: {}"
                                   .format(self._uniq_key, release, read_end))
            except DatahubException as e:
                self._logger.warning("SyncGroup fail. key: {}, release: {}, read end: {}. {}"
                                     .format(self._uniq_key, release, read_end, e))
                raise e

    def __join_group(self):
        timer = Timer(Constant.MAX_JOIN_GROUP_TIMEOUT)
        while not timer.is_expired():
            try:
                join_result = self._meta_data.datahub_client.join_group(
                    self._project_name,
                    self._topic_name,
                    self._sub_id,
                    self._session_timeout
                )
                self._consumer_id = join_result.consumer_id
                self._version_id = join_result.version_id
                self._session_timeout = join_result.session_timeout
                self._gen_uniq_key(self._consumer_id)
                self._logger.info("JoinGroup success. key: {}, consumer id: {}, version id: {}, session timeout: {}"
                                  .format(self._uniq_key, self._consumer_id, self._version_id, self._session_timeout))
                return
            except SubscriptionOfflineException as e:
                self._logger.warning("JoinGroup fail, subscription offline. key:{}. {}".format(self._uniq_key, e))
                raise e
            except DatahubException as e:
                self._logger.warning("JoinGroup fail. retry again. key:{}. {}".format(self._uniq_key, e))

            try:
                timer.wait_expire(1)
            except Exception as e:
                raise e

        raise TimeoutException("JoinGroup timeout. key: {}, elapsedMs: {}".format(self._uniq_key, timer.elapse()))

    def __leave_group(self):
        try:
            self._meta_data.datahub_client.leave_group(
                self._project_name,
                self._topic_name,
                self._sub_id,
                self._consumer_id,
                self._version_id
            )
            self._logger.info("LeaveGroup success. key:{}".format(self._uniq_key))
        except DatahubException as e:
            self._logger.warning("LeaveGroup fail. key:{}. {}".format(self._uniq_key, e))
            raise e

    def __start_heartbeat(self):
        if not self._heart_beat:
            self._heart_beat = ConsumerHeartbeat(self, self._sync_group_meta, self._consumer_id, self._version_id,
                                                 self._session_timeout / 1000)
            self._logger.info("Start heartbeat success. key:{}".format(self._uniq_key))

    def __stop_heartbeat(self):
        if self._heart_beat:
            self._heart_beat.close()
            self._heart_beat = None
            self._logger.info("Stop heartbeat success. key:{}".format(self._uniq_key))
