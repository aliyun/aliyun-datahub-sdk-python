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

from __future__ import absolute_import

import os
import time
import json
import re
import traceback
from collections import Iterable

from .thirdparty import six
from .auth import AliyunAccount
from .utils import Logger, Path
from .errors import *

from .models import RestClient
from .models import Project, Projects
from .models import Topic, Topics
from .models import ShardAction, ShardState, Shards
from .models import Records
from .models import MeteringInfo
from .models import CursorType, Cursor

class DataHub(object):
    """
    Main entrance to DataHub.

    Convenient operations on DataHub objects are provided.
    Please refer to `DataHub docs <https://datahub.console.aliyun.com/intro/index.html>`_
    to see the details.

    Generally, basic operations such as ``create``, ``list``, ``delete``, ``update`` are provided for each DataHub object.
    Take the ``project`` as an example.

    To create an DataHub instance, access_id and access_key is required, and should ensure correctness,
    or ``SignatureNotMatch`` error will throw.
    :param access_id: Aliyun Access ID
    :param secret_access_key: Aliyun Access Key
    :param endpoint: Rest service URL

    :Example:

    >>> datahub = DataHub('**your access id**', '**your access key**', '**endpoint**')
    >>>
    >>> project = datahub.get_project('datahub_test')
    >>>
    >>> print project is None
    >>>
    """

    def __init__(self, access_id, access_key, endpoint=None, **kwds):
        """
        """
        self.account = kwds.pop('account', None)
        if self.account is None:
            self.account = AliyunAccount(access_id=access_id, access_key=access_key)
        self.endpoint = endpoint
        self.restclient = RestClient(self.account, self.endpoint, **kwds)

    def list_projects(self):
        """
        List all projects

        :return: projects in datahub server
        :rtype: generator

        .. seealso:: :class:`datahub.models.Projects`
        """
        projects = Projects()
        self.restclient.get(restmodel=projects)
        return projects

    def get_project(self, name):
        """
        Get a project by given name

        :param name: project name
        :return: the right project
        :rtype: :class:`datahub.models.Project`
        :raise: :class:`datahub.errors.NoSuchObjectException` if not exists

        .. seealso:: :class:`datahub.models.Project`
        """
        if not name:
            raise InvalidArgument('project name is empty')
        proj = Project(name=name)
        self.restclient.get(restmodel=proj)
        return proj

    def list_topics(self, project_name):
        """
        Get all topics of a project

        :param project_name: project name
        :return: all topics of the project
        :rtype: generator
        :raise: :class:`datahub.errors.NoSuchObjectException` if the project not exists

        .. seealso:: :class:`datahub.models.Topics`
        """
        if not project_name:
            raise InvalidArgument('project name is empty')
        topics = Topics(project_name=project_name)
        self.restclient.get(restmodel=topics)
        return topics

    def create_topic(self, topic):
        """
        Create topic

        :param topic: a object instance of :class:`datahub.models.Topic`
        :return: none
        """
        if not isinstance(topic, Topic):
            raise InvalidArgument('argument topic type must be datahub.models.Topic')
        self.restclient.post(restmodel=topic)

    def get_topic(self, name, project_name):
        """
        Get a topic

        :param name: topic name
        :param project_name: project name
        :return: topic object
        :rtype: :class:`datahub.models.Topic`
        :raise: :class:`datahub.errors.NoSuchObjectException` if the project or topic not exists

        .. seealso:: :class:`datahub.models.Topic`
        """
        if not name or not project_name:
            raise InvalidArgument('topic or project name is empty')
        topic = Topic(name=name, project_name=project_name)
        self.restclient.get(restmodel=topic)
        return topic

    def update_topic(self, name, project_name, life_cycle=0, comment=''):
        """
        Update topic info, only life cycle and comment can be modified.

        :param name: topic name
        :param project_name: project name
        :param life_cycle: life cycle of topic
        :param comment: topic comment
        :return: none
        :raise: :class:`datahub.errors.NoSuchObjectException` if the project or topic not exists
        """
        if 0 == life_cycle and '' == comment:
            return
        if not name or not project_name:
            raise InvalidArgument('topic or project name is empty')
        topic = Topic(name=name, project_name=project_name, life_cycle=life_cycle, comment=comment)
        self.restclient.put(restmodel=topic)

    def delete_topic(self, name, project_name):
        """
        Delete a topic

        :param name: topic name
        :param project_name: project name
        :return: none
        :raise: :class:`datahub.errors.NoSuchObjectException` if the project or topic not exists
        """
        if not name or not project_name:
            raise InvalidArgument('topic or project name is empty')
        topic = Topic(name=name, project_name=project_name)
        self.restclient.delete(restmodel=topic)

    def wait_shards_ready(self, project_name, topic_name, timeout=-1):
        """
        Wait all shard state in ``active`` or ``closed``.
        It always be invoked when create a topic, and will be blocked and unitl all
        shards state in ``active`` or ``closed`` or timeout .

        :param project_name: project name
        :param topic_name: topic name
        :param timeout: -1 means it will be blocked until all shards state in ``active`` or ``closed``, else will be wait timeout seconds
        :return: if all shards ready
        :rtype: boolean
        :raise: :class:`datahub.errors.NoSuchObjectException` if the project or topic not exists
        """
        if not topic_name or not project_name:
            raise InvalidArgument('topic or project name is empty')
        iCostTime = 0
        bNotReady = True
        bTimeout = False
        while bNotReady and not bTimeout:
            bNotReady = False
            shards = Shards(action=ShardAction.LIST, project_name=project_name, topic_name=topic_name)
            self.restclient.get(restmodel=shards)
            for shard in shards:
                if shard.state not in (ShardState.ACTIVE, ShardState.CLOSED):
                    Logger.logger.debug("project: %s, topic: %s, shard: %s state is %s, sleep 1s" %(project_name, topic_name, shard.shard_id, shard.state))
                    bNotReady = True
                    time.sleep(1)
                    iCostTime += 1
                    if timeout > 0 and iCostTime >= timeout:
                        bTimeout = True
                    break

        return not bNotReady

    def list_shards(self, project_name, topic_name):
        """
        List all shards of a topic

        :param project_name: project name
        :param topic_name: topic name
        :return: all shards
        :rtype: :class:`datahub.models.Shards`
        :raise: :class:`datahub.errors.NoSuchObjectException` if the project or topic not exists

        .. seealso:: :class:`datahub.models.Shards`
        """
        if not topic_name or not project_name:
            raise InvalidArgument('topic or project name is empty')
        shards = Shards(action=ShardAction.LIST, project_name=project_name, topic_name=topic_name)
        self.restclient.get(restmodel=shards)
        return shards

    def merge_shard(self, project_name, topic_name, shard_id, adj_shard_id):
        """
        Merge shards

        :param project_name: project name
        :param topic_name: topic name
        :param shard_id: shard id
        :param adj_shard_id: adjacent shard id
        :return: after merged shards
        :rtype: :class:`datahub.models.Shards`
        :raise: :class:`datahub.errors.NoSuchObjectException` if the shard not exists

        .. seealso:: :class:`datahub.models.Shards`
        """
        if not topic_name or not project_name:
            raise InvalidArgument('topic or project name is empty')
        if not shard_id or not adj_shard_id:
            raise InvalidArgument('shard id or adjacent shard id is empty')
        shards = Shards(action=ShardAction.MERGE, project_name=project_name, topic_name=topic_name)
        shards.set_mergeinfo(shard_id, adj_shard_id)
        self.restclient.post(restmodel=shards)
        return shards

    def split_shard(self, project_name, topic_name, shard_id, split_key):
        """
        Split shard

        :param project_name: project name
        :param topic_name: topic name
        :param shard_id: split shard id
        :param split_key: split key
        :return: after split shards
        :rtype: :class:`datahub.models.Shards`
        :raise: :class:`datahub.errors.NoSuchObjectException` if the shard not exists

        .. seealso:: :class:`datahub.models.Shards`
        """
        if not topic_name or not project_name:
            raise InvalidArgument('topic or project name is empty')
        if not shard_id or not split_key:
            raise InvalidArgument('shard id or split key is empty')
        shards = Shards(action=ShardAction.SPLIT, project_name=project_name, topic_name=topic_name)
        shards.set_splitinfo(shard_id, split_key)
        self.restclient.post(restmodel=shards)
        return shards

    def get_cursor(self, project_name, topic_name, type, shard_id, system_time=0):
        """
        Get cursor.
        When you invoke get_records first, you must be invoke it to get a cursor

        :param project_name: project name
        :param topic_name: topic name
        :param type: cursor type
        :param shard_id: shard id
        :param system_time: if type=CursorType.SYSTEM_TIME, it must be set
        :return: a cursor
        :rtype: :class:`datahub.models.Cursor`
        :raise: :class:`datahub.errors.NoSuchObjectException` if the shard not exists

        .. seealso:: :class:`datahub.models.CursorType`, :class:`datahub.models.Cursor`
        """
        if not topic_name or not project_name:
            raise InvalidArgument('topic or project name is empty')
        if not shard_id or not type:
            raise InvalidArgument('shard id or type is empty')
        cursor = Cursor(project_name=project_name, topic_name=topic_name, type=type, shard_id=shard_id)
        if CursorType.SYSTEM_TIME == type and system_time == 0:
            raise InvalidArgument('get SYSTEM_TIME cursor must provide system_time argument')
            cursor.system_time = system_time
        self.restclient.post(restmodel=cursor)
        return cursor

    def put_records(self, project_name, topic_name, record_list):
        """
        Put records to a topic

        :param project_name: project name
        :param topic_name: topic name
        :param record_list: record list
        :return: failed record indeies
        :rtype: list
        :raise: :class:`datahub.errors.NoSuchObjectException` if the topic not exists

        .. seealso:: :class:`datahub.models.Record`
        """
        if not topic_name or not project_name:
            raise InvalidArgument('topic or project name is empty')
        if not isinstance(record_list, list):
            raise InvalidArgument('record list must be a List')
        records = Records(project_name=project_name, topic_name=topic_name)
        records.record_list = record_list
        self.restclient.post(restmodel=records)
        return records.failed_indexs

    def get_records(self, topic, shard_id, cursor, limit_num=1):
        """
        Get records from a topic

        :param topic: a object instance of :class:`datahub.models.Topic`
        :param shard_id: shard id
        :param cursor: the cursor
        :return: record list, record num and next cursor
        :rtype: tuple
        :raise: :class:`datahub.errors.NoSuchObjectException` if the topic not exists

        .. seealso:: :class:`datahub.models.Topic`, :class:`datahub.models.Cursor`
        """
        if not shard_id:
            raise InvalidArgument('shard id is empty')
        if not isinstance(topic, Topic):
            raise InvalidArgument('argument topic type must be datahub.models.Topic')
        records = Records(project_name=topic.project_name, topic_name=topic.name, schema=topic.record_schema)
        records.shard_id = shard_id
        records.next_cursor = str(cursor)
        records.limit_num = limit_num
        self.restclient.post(restmodel=records)
        return (records.record_list, records.record_num, records.next_cursor)

    def get_meteringinfo(self, project_name, topic_name, shard_id):
        """
        Get a shard metering info

        :param project_name: project name
        :param topic_name: topic name
        :param shard_id: shard id
        :return: the shard metering info
        :rtype: :class:`datahub.models.MeteringInfo`
        :raise: :class:`datahub.errors.NoSuchObjectException` if the topic not exists

        .. seealso:: :class:`datahub.models.MeteringInfo`
        """
        if not project_name or not topic_name:
            raise InvalidArgument('project or topic name is empty')
        if not shard_id:
            raise InvalidArgument('shard id is empty')
        meteringInfo = MeteringInfo(project_name=project_name, topic_name=topic_name, shard_id=shard_id)
        self.restclient.post(restmodel=meteringInfo)
        return meteringInfo
