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


import time
import logging
import threading
from collections import deque
from datahub.models import OffsetWithBatchIndex
from datahub.exceptions import SubscriptionOfflineException, ResourceNotFoundException, \
    OffsetResetException, InvalidOperationException, DatahubException
from ..common.timer import Timer
from ..common.constant import Constant


class OffsetManager:

    def __init__(self, coordinator):
        self._closed = False
        self._logger = logging.getLogger(OffsetManager.__name__)

        self._coordinator = coordinator
        self._uniq_key = self._coordinator.uniq_key
        self._timer = Timer(Constant.OFFSET_COMMIT_INTERVAL_TIMEOUT)

        self._lock = threading.Lock()
        self._offset_meta_map = dict()
        self._offset_request_queue_map = dict()
        self._last_offset_map = dict()

        self.__start()

    def close(self):
        self._closed = True
        self._timer.notify_all()
        self._commit_task.join()

    def set_offset_meta(self, consume_offset_map):
        with self._lock:
            for shard_id, consume_offset in consume_offset_map.items():
                self._offset_meta_map[shard_id] = consume_offset
                self._offset_request_queue_map[shard_id] = deque()

    def on_shard_release(self, del_shards):
        self.__force_commit_offset(del_shards)
        with self._lock:
            for shard_id in del_shards:
                if shard_id in self._offset_meta_map:
                    self._offset_meta_map.pop(shard_id)
                if shard_id in self._offset_request_queue_map:
                    self._offset_request_queue_map.pop(shard_id)

    def on_offset_reset(self):
        with self._lock:
            self._last_offset_map.clear()
            self._offset_request_queue_map.clear()
            self._offset_meta_map.clear()

    def send_record_offset(self, message_key):
        if message_key.shard_id not in self._offset_request_queue_map:
            self._logger.warning("Send record offset error. shard_id: {}, key: {}".format(message_key.shard_id, self._uniq_key))
            raise DatahubException("Send record offset error")
        with self._lock:
            queue = self._offset_request_queue_map.get(message_key.shard_id)
            if queue is None:
                raise DatahubException("Offset request deque not found. key: {}, shar_id: {}".format(self._uniq_key, message_key.shard_id))
            queue.append(OffsetRequest(message_key))
            self._logger.debug("Send record offset success. shard_id: {}, key: {}, offset: {}"
                               .format(message_key.shard_id, self._uniq_key, message_key.offset.to_string()))

    def __start(self):
        self._commit_task = threading.Thread(target=self.__commit_offset_task)
        self._commit_task.setName("OffsetManager")
        self._commit_task.start()

    def __commit_offset_task(self):
        self._logger.info("Offset commit task start. key: {}".format(self._uniq_key))
        while not self._closed:
            if self._timer.is_expired():
                try:
                    with self._lock:
                        self.__sync_offsets()
                        self.__commit_offsets()
                        self._timer.reset()
                except OffsetResetException as e:
                    self._logger.warning("CommitOffset fail, subscription offset reset. key:{}. last offset map: {}. {}".format(
                        self._uniq_key, self._last_offset_map, e))
                except InvalidOperationException as e:
                    self._logger.warning("CommitOffset fail, subscription session invalid. key:{}. {}".format(self._uniq_key, e))
                    self._coordinator.on_sub_session_changed()
                except SubscriptionOfflineException as e:
                    self._logger.warning("CommitOffset fail, subscription offline. key:{}. {}".format(self._uniq_key, e))
                    self._coordinator.on_sub_offline()
                except ResourceNotFoundException as e:
                    if "NoSuchSubscription" in e.error_code:
                        self._logger.warning("CommitOffset fail, subscription deleted. key:{}. {}".format(self._uniq_key, e))
                        self._coordinator.on_sub_deleted()
                    else:
                        self._logger.warning("CommitOffset fail. key:{}. NoSuchSubscription: {}".format(self._uniq_key, e))
                except Exception as e:
                    self._logger.warning("CommitOffset fail. key:{}. {}".format(self._uniq_key, e))
                    raise e
            else:
                try:
                    self._timer.wait_expire(Constant.OFFSET_CHECK_TIMEOUT)
                except Exception as e:
                    self._logger.warning("OffsetCommitTask interrupt occur. key: {}, {}".format(self._uniq_key, e))
                    break
        with self._lock:
            self.__sync_offsets()
            self.__commit_offsets()
        self._logger.info("Offset commit task stop. key: {}".format(self._uniq_key))

    def __force_commit_offset(self, shard_ids):
        try:
            timer = Timer(Constant.FORCE_COMMIT_TIMEOUT)
            self.__commit_right_now()
            while not timer.is_expired() and not self.is_request_queue_empty(shard_ids):
                self.__commit_right_now()
        except Exception as e:
            self._logger.warning("Force commit offset fail. key:{}, shard_ids: {}, {}".format(self._uniq_key, shard_ids, e))

    def __commit_right_now(self):
        self._timer.reset_deadline()
        self._timer.notify_all()

    def is_request_queue_empty(self, shard_ids):
        with self._lock:
            for shard_id in shard_ids:
                requests = self._offset_request_queue_map.get(shard_id)
                if requests and len(requests) > 0:
                    return False
            return True

    def __sync_offsets(self):
        for shard_id, request_queue in self._offset_request_queue_map.items():
            request = None
            while len(request_queue) > 0 and request_queue[0].is_ready():
                request = request_queue[0]
                request_queue.popleft()

            if request:
                meta = self._offset_meta_map.get(shard_id)
                if not meta:
                    self._logger.warning("OffsetMeta not found. key:{}, shard_id:{}".format(self._uniq_key, shard_id))
                    raise DatahubException("OffsetMeta not found")
                consume_offset = request.message_key.offset
                self._last_offset_map[shard_id] = OffsetWithBatchIndex(
                    consume_offset.sequence,
                    consume_offset.timestamp,
                    meta.version_id,
                    meta.session_id,
                    consume_offset.batch_index
                )
                self._logger.debug("Sync offset once success. key: {}, shard_id: {}".format(self._uniq_key, shard_id))
            else:
                if len(request_queue) > 0:      # 最先入队列的Request依然没有Ready
                    curr_timeout = int(time.time())
                    diff = curr_timeout - request_queue[0].timestamp
                    if diff > Constant.NOT_ACK_WARNING_TIMEOUT:
                        self._logger.warning("Record not ack for {} s. key:{}, shard_id:{}, currTs:{}, offset:{}"
                                             .format(diff, self._uniq_key, shard_id, curr_timeout, request_queue[0].message_key.to_string()))
                        if diff > Constant.NOT_ACK_WARNING_TIMEOUT * 10:
                            self._coordinator.on_offset_not_ack()

    def __commit_offsets(self):
        try:
            if len(self._last_offset_map) > 0:
                self._coordinator.meta_data.datahub_client.update_subscription_offset(
                    self._coordinator.project_name,
                    self._coordinator.topic_name,
                    self._coordinator.sub_id,
                    self._last_offset_map
                )
                self._logger.info("Commit offset success. key: {}, min offset = {}".format(self._uniq_key, self.__get_min_timestamp()))
                self._last_offset_map.clear()
        except DatahubException as e:
            self._logger.warning("Commit offset fail. key: {}, min offset = {}, DatahubException: {}".format(self._uniq_key, self.__get_min_timestamp(), e))
            raise e
        except Exception as e:
            self._logger.warning("Commit offset fail. key: {}, min offset = {}, {}".format(self._uniq_key, self.__get_min_timestamp(), e))
            raise e

    def __get_min_timestamp(self):
        return min(self._last_offset_map.values(), key=lambda offset: offset.timestamp)


class OffsetRequest:
    def __init__(self, message_key):
        self._timestamp = int(time.time())
        self._message_key = message_key

    def is_ready(self):
        return self._message_key.is_ready()

    @property
    def message_key(self):
        return self._message_key
