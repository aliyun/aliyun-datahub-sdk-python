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
from .offset_coordinator import OffsetCoordinator
from .consumer_coordinator import ConsumerCoordinator
from .shard_group_reader import ShardGroupReader
from ..common.config import Utils


class DatahubConsumer:
    """
    Consumer client for datahub

    Members:
        project_name (:class:`string`): project name

        topic_name (:class:`string`): topic name

        sub_id (:class:`string`): subscription id for consume

        consumer_config (:class:`datahub.client.common.ConsumerConfig`): config for consumer client

        shard_ids (:class:`list`): list of `string`: shard list you want to consume.
                default is None, means allocated automatically by datahub server

        timestamp (:class:`int`): set the start timestamp for consume.
                default is -1, means start with the subscription offset
    """

    def __init__(self, project_name, topic_name, sub_id, consumer_config, shard_ids=None, timestamp=-1):
        logging.basicConfig(filename=consumer_config.logging_filename, filemode="a",
                            level=consumer_config.logging_level, format=Utils.FORMAT, datefmt=Utils.DATE_FMT)

        if shard_ids:
            # 指定shard消费
            self._coordinator = OffsetCoordinator(project_name, topic_name, sub_id, consumer_config)
        else:
            # 协同消费
            self._coordinator = ConsumerCoordinator(project_name, topic_name, sub_id, consumer_config)

        try:
            self._group_reader = ShardGroupReader(self._coordinator, shard_ids, timestamp)
        except Exception as e:
            self._coordinator.close()
            raise e

    def close(self):
        self._group_reader.close()
        self._coordinator.close()

    def read(self, shard_id=None, timeout=60):
        return self._group_reader.read(shard_id, timeout)
