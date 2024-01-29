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
from datahub.exceptions import DatahubException, OffsetResetException
from ..common.timer import Timer
from ..common.constant import Constant


class ConsumerHeartbeat:

    def __init__(self, coordinator, sync_group_meta, consumer_id, version_id, session_timeout):
        super().__init__()
        self._logger = logging.getLogger(ConsumerHeartbeat.__name__)
        self._closed = False
        self._offset_reset = False
        self._coordinator = coordinator
        self._sync_group_meta = sync_group_meta
        self._consumer_id = consumer_id
        self._version_id = version_id
        self._session_timeout = session_timeout

        self._curr_shards = []
        self._timer = Timer(Constant.MIN_HEARTBEAT_INTERVAL_TIMEOUT)
        self._heartbeat_timeout = session_timeout/6

        self.__start()

    def close(self):
        self._closed = True
        self._timer.notify_all()
        self._heart_beat_task.join()

    def waiting_shard_assign(self):
        return not self._sync_group_meta.get_valid_shards()

    def need_rejoin(self):
        if self._offset_reset:
            self._offset_reset = False
            return True
        elapse = self._timer.elapse()
        is_expire = elapse > self._session_timeout
        if is_expire:
            self._logger.warning("ConsumerHeartbeat timeout. key:{}, elapsedMs:{}, sessionTimeoutMs:{}"
                                 .format(self._coordinator.uniq_key, elapse, self._session_timeout))
        return is_expire

    def __start(self):
        self._heart_beat_task = threading.Thread(target=self.__keep_heartbeat)
        self._heart_beat_task.setName("Heartbeat")
        self._heart_beat_task.start()

    def __keep_heartbeat(self):
        self._logger.info("ConsumerHeartbeat task start. key: {}, session timeout: {}, heartbeat timeout: {}"
                          .format(self._coordinator.uniq_key, self._session_timeout, self._heartbeat_timeout))
        while not self._closed:
            if self._timer.is_expired():
                self.__heartbeat_once()
                if self._sync_group_meta.get_valid_shards():
                    self._timer.reset(self._heartbeat_timeout)
                else:
                    self._logger.warning("Heartbeat has not assign consumer plan, please wait. key:{}".format(self._coordinator.uniq_key))
                    self._timer.reset(Constant.MIN_HEARTBEAT_INTERVAL_TIMEOUT)
            else:
                try:
                    self._timer.wait_expire()
                except Exception as e:
                    self._logger.warning("ConsumerHeartbeat stop. {}".format(e))
                    break
        self._logger.info("ConsumerHeartbeat task stop. key:{}, sessionTimeoutMs:{}, heartbeatTimeoutMs:{}"
                         .format(self._coordinator.uniq_key, self._session_timeout, self._timer.timeout))

    def __heartbeat_once(self):
        if not self._closed:
            release_shards = self._curr_shards
            read_end_shards = list(self._sync_group_meta.read_end_shards)
            try:
                heartbeat_result = self._coordinator.meta_data.datahub_client.heart_beat(
                    self._coordinator.project_name,
                    self._coordinator.topic_name,
                    self._coordinator.sub_id,
                    self._consumer_id,
                    self._version_id,
                    release_shards,
                    read_end_shards
                )
                plan_version = heartbeat_result.plan_version
                new_shards = heartbeat_result.shard_list

                add_shards = [shard for shard in new_shards if shard not in self._curr_shards]
                del_shards = [shard for shard in self._curr_shards if shard not in new_shards]
                if len(add_shards) != 0 or len(del_shards) != 0:
                    self._logger.info("Consumer heartbeat with plan change. key:{}, version:{}, planVersion:{}, oldShards:{}, newShards:{}"
                                      .format(self._coordinator.uniq_key, self._version_id, plan_version, self._curr_shards, new_shards))
                    self._coordinator.on_shard_change(add_shards, del_shards)
                    self._curr_shards = new_shards
                    self._sync_group_meta.on_heartbeat_done(new_shards)
                self._logger.debug("Heartbeat success. key:{}，version:{}, planVersion:{}, newShards:{}"
                                   .format(self._coordinator.uniq_key, self._version_id, plan_version, new_shards))
            except OffsetResetException as e:
                self._logger.warning("Consumer heartbeat fail, offset reset. key:{}. {}".format(self._coordinator.uniq_key, e))
                self._offset_reset = True
                self._coordinator.on_offset_reset()
            except DatahubException as e:
                if "NoSuchSubscription" == e.error_code:
                    self._logger.warning("CommitOffset fail, subscription deleted. key:{}. {}".format(self._coordinator.uniq_key, e))
                    self._coordinator.on_sub_deleted()
                else:
                    self._logger.warning("Consumer heartbeat fail in DatahubException. key:{}. {}".format(self._coordinator.uniq_key, e))
            except Exception as e:
                self._logger.warning("Consumer heartbeat fail. key:{}. {}".format(self._coordinator.uniq_key, e))
                raise e
