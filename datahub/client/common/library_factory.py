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


import abc
import threading
from .meta_data import MetaData


class LibraryFactory(metaclass=abc.ABCMeta):

    _meta_data_pool = dict()
    _meta_data_lock = threading.Lock()

    @staticmethod
    def get_meta_data(coordinator, common_config):
        key = "{}:{}:{}:{}".format(coordinator.endpoint, coordinator.project_name,
                                   coordinator.topic_name, coordinator.sub_id)
        if key not in LibraryFactory._meta_data_pool:
            with LibraryFactory._meta_data_lock:
                if key not in LibraryFactory._meta_data_pool:
                    LibraryFactory._meta_data_pool[key] = MetaData(key, coordinator.project_name, coordinator.topic_name, coordinator.sub_id, common_config)
        meta_data = LibraryFactory._meta_data_pool.get(key)
        meta_data.register(coordinator)
        return meta_data

    @staticmethod
    def remove_meta_data(coordinator):
        meta_data = coordinator.meta_data
        with LibraryFactory._meta_data_lock:
            if meta_data.unregister(coordinator) <= 0:
                meta_data.close()
                LibraryFactory._meta_data_pool.pop(meta_data.class_key)
