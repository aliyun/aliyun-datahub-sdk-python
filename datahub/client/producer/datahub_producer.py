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
from .shard_group_writer import ShardGroupWriter
from ..common.config import Utils
from ..common.shard_coordinator import ShardCoordinator


class DatahubProducer:
    """
    Producer client for datahub

    Members:
        project_name (:class:`string`): project name

        topic_name (:class:`string`): topic name

        producer_config (:class:`datahub.client.common.ProducerConfig`): config for producer client

        shard_ids (:class:`list`): list of `string`: shard list you want to producer.
                default is None, means write to all shards evenly
    """

    def __init__(self, project_name, topic_name, producer_config, shard_ids=None):
        logging.basicConfig(filename=producer_config.logging_filename, filemode="a",
                            level=producer_config.logging_level, format=Utils.FORMAT, datefmt=Utils.DATE_FMT)

        self._coordinator = ShardCoordinator(project_name, topic_name, "", producer_config)

        try:
            self._group_writer = ShardGroupWriter(self._coordinator, shard_ids, producer_config)
        except Exception as e:
            self._coordinator.close()
            raise e

    def close(self):
        self._group_writer.close()
        self._coordinator.close()

    def write(self, records):
        return self._group_writer.write(records)

    def write_async(self, records):
        return self._group_writer.write_async(records)

    def flush(self):
        self._group_writer.flush()

    @property
    def topic_meta(self):
        return self._coordinator.meta_data.topic_meta
