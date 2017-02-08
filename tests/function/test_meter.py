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
import sys

import pytest
from six.moves import configparser

from datahub import DataHub
from datahub.exceptions import InvalidOperationException

current_path = os.path.split(os.path.realpath(__file__))[0]
root_path = os.path.join(current_path, '../..')

configer = configparser.ConfigParser()
configer.read(os.path.join(current_path, '../datahub.ini'))
access_id = configer.get('datahub', 'access_id')
access_key = configer.get('datahub', 'access_key')
endpoint = configer.get('datahub', 'endpoint')

project_name = configer.get('datahub', 'meter_test_project_name')
topic_name = configer.get('datahub', 'meter_test_topic_name')
shard_id = configer.get('datahub', 'meter_test_shard_id')

print("=======================================")
print("access_id: %s" % access_id)
print("access_key: %s" % access_key)
print("endpoint: %s" % endpoint)
print("=======================================\n\n")

if not access_id or not access_key or not endpoint:
    print("[access_id, access_key, endpoint] must be set in datahub.ini!")
    sys.exit(-1)

dh = DataHub(access_id, access_key, endpoint)


def clean_topic(datahub_client, project_name, force=False):
    topic_names = datahub_client.list_topic(project_name).topic_names
    for topic_name in topic_names:
        if force:
            clean_subscription(datahub_client, project_name, topic_name)
        datahub_client.delete_topic(project_name, topic_name)


def clean_project(datahub_client, force=False):
    project_names = datahub_client.list_project().project_names
    for project_name in project_names:
        if force:
            clean_topic(datahub_client, project_name)
        try:
            datahub_client.delete_project(project_name)
        except InvalidOperationException:
            pass


def clean_subscription(datahub_client, project_name, topic_name):
    subscriptions = datahub_client.list_subscription(project_name, topic_name, '', 1, 100).subscriptions
    for subscription in subscriptions:
        datahub_client.delete_subscription(project_name, topic_name, subscription.sub_id)


@pytest.mark.skipif(project_name == '' or topic_name == '' or shard_id == '', reason="meter test shard isn\'t set")
class TestMeter:

    def test_get_metering_info(self):
        result = dh.get_metering_info(project_name, topic_name, shard_id)
        print(result)
        assert result.active_time > 0
        assert result.storage >= 0


# run directly
if __name__ == '__main__':
    test = TestMeter()
    test.test_get_metering_info()
