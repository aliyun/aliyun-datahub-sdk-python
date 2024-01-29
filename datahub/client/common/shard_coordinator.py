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
from datahub.exceptions import DatahubException
from .library_factory import LibraryFactory


class ShardCoordinator:

    def __init__(self, project_name, topic_name, sub_id, common_config):
        self._closed = False
        self._logger = logging.getLogger(ShardCoordinator.__name__)

        self._endpoint = common_config.endpoint
        self._project_name = project_name
        self._topic_name = topic_name
        self._sub_id = sub_id
        self._uniq_key = None
        self._gen_uniq_key()

        self._meta_data = LibraryFactory.get_meta_data(self, common_config)
        self._assign_shard_list = []
        self._shard_change = None
        self._remove_all_shards = None

    def close(self):
        self._closed = True
        LibraryFactory.remove_meta_data(self)

    def update_shard_info(self):
        if self._closed:
            self._logger.warning("ShardCoordinator closed when update shard info. key: {}".format(self._uniq_key))
            raise DatahubException("ShardCoordinator closed when update shard info")

        self._meta_data.update_shard_meta()

    def register_shard_change(self, shard_change_callback):
        self._shard_change = shard_change_callback

    def register_remove_all_shards(self, remove_all_shards_callback):
        self._remove_all_shards = remove_all_shards_callback

    def on_shard_meta_change(self, add_shards, del_shards):
        if not self.is_user_shard_assign():
            self._do_shard_change(add_shards, del_shards)

    def is_user_shard_assign(self):
        return len(self._assign_shard_list) > 0

    @property
    def assign_shard_list(self):
        return self._assign_shard_list

    @assign_shard_list.setter
    def assign_shard_list(self, value):
        self._assign_shard_list = value

    @property
    def endpoint(self):
        return self._endpoint

    @property
    def project_name(self):
        return self._project_name

    @property
    def topic_name(self):
        return self._topic_name

    @property
    def sub_id(self):
        return self._sub_id

    @property
    def meta_data(self):
        return self._meta_data

    @property
    def uniq_key(self):
        return self._uniq_key

    def _do_shard_change(self, add_shards, del_shards):
        if self._closed:
            self._logger.warning("ShardCoordinator closed when shard change. key: {}".format(self._uniq_key))
            raise DatahubException("ShardCoordinator closed when shard change")

        if self._shard_change and ((add_shards and len(add_shards) != 0) or (del_shards and len(del_shards) != 0)):
            self._shard_change(add_shards, del_shards)

    def _do_remove_all_shards(self):
        if self._closed:
            self._logger.warning("ShardCoordinator closed when remove all shards. key: {}".format(self._uniq_key))
            raise DatahubException("ShardCoordinator closed when remove all shards")

        if self._remove_all_shards:
            self._remove_all_shards()

    def _gen_uniq_key(self, suffix=None):
        if not self._uniq_key:
            self._uniq_key = "{}:{}".format(self._project_name, self._topic_name)
            if self._sub_id:
                self._uniq_key += (":" + self._sub_id)

        if suffix:
            self._uniq_key += (":" + suffix)
