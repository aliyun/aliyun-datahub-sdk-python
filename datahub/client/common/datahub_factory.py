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


import threading
from datahub import DataHub


class DatahubFactory:

    _datahub_client_pool = dict()        # "endpoint:id:key:protocol:compress" --> client
    _datahub_lock = threading.Lock()

    @staticmethod
    def create_datahub_client(datahub_config):
        key = "{}:{}:{}:{}:{}".format(datahub_config.endpoint, datahub_config.access_id, datahub_config.access_key,
                                      datahub_config.protocol_type.value, datahub_config.compress_format.value)
        if key not in DatahubFactory._datahub_client_pool:
            with DatahubFactory._datahub_lock:
                if key not in DatahubFactory._datahub_client_pool:
                    DatahubFactory._datahub_client_pool[key] = DataHub(
                        access_id=datahub_config.access_id,
                        access_key=datahub_config.access_key,
                        endpoint=datahub_config.endpoint,
                        protocol_type=datahub_config.protocol_type,
                        compress_format=datahub_config.compress_format,
                        use_client=True
                    )
        return DatahubFactory._datahub_client_pool.get(key)
