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

import os
import configparser
from httmock import HTTMock

from datahub import DatahubProtocolType
from datahub.client.common.config import ConsumerConfig
from datahub.client.consumer.datahub_consumer import DatahubConsumer
from datahub.models import CompressFormat
from unit_consumer.unittest_util import gen_consumer_final_api


def get_configer():
    configer = configparser.ConfigParser()
    configer.read(os.path.join("unit.config"))

    endpoint = configer.get("datahub", "endpoint")
    access_id = configer.get("datahub", "access_id")
    access_key = configer.get("datahub", "access_key")
    project_name = configer.get("datahub", "project_name")
    topic_name = configer.get("datahub", "topic_name")
    sub_id = configer.get("datahub", "sub_id")

    protocol_type = DatahubProtocolType.JSON
    compress_format = CompressFormat.NONE

    consumer_config = ConsumerConfig(
        access_id=access_id,
        access_key=access_key,
        endpoint=endpoint,
        protocol_type=protocol_type,
        compress_format=compress_format
    )
    return project_name, topic_name, sub_id, consumer_config


class TestConsumer:

    def test_consumer_success(self):
        project_name, topic_name, sub_id, consumer_config = get_configer()

        def check(request):
            pass

        cnt, CHECK_NUM = 0, 200
        with HTTMock(gen_consumer_final_api(check)):
            try:
                consumer = DatahubConsumer(project_name, topic_name, sub_id, consumer_config)
                while True:
                    record = consumer.read(timeout=10)
                    if not record:
                        print("Error.")
                        continue

                    assert record.system_time == 1526292424292
                    assert record.values == 'iVBORw0KGgoAAAANSUhEUgAAB5FrTVeMB4wHjAeMBD3nAgEU'

                    cnt += 1
                    if cnt >= CHECK_NUM:
                        print("Read {} blob records success.".format(CHECK_NUM))
                        break
            finally:
                consumer.close()


if __name__ == "__main__":
    test = TestConsumer()

    test.test_consumer_success()
