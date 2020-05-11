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

import abc
import json
from collections import OrderedDict
from enum import Enum

import six

from datahub.exceptions import DatahubException
from ..utils import to_str, bool_to_str, to_text

if six.PY3:
    long = int


class ConnectorType(Enum):
    """
    ConnectorType enum class, there are: ``SINK_ODPS``, ``SINK_ADS``, ``SINK_ES``, ``SINK_FC``, \
    ``SINK_MYSQL``, ``SINK_OSS``, ``SINK_OTS``, ``SINK_DATAHUB``
    """
    SINK_ODPS = 'sink_odps'
    SINK_ADS = 'sink_ads'
    SINK_ES = 'sink_es'
    SINK_FC = 'sink_fc'
    SINK_MYSQL = 'sink_mysql'
    SINK_OSS = 'sink_oss'
    SINK_OTS = 'sink_ots'
    SINK_DATAHUB = 'sink_datahub'


class ConnectorState(Enum):
    """
    ConnectorState enum class, there are: ``CONNECTOR_RUNNING``, ``CONNECTOR_STOPPED``
    """
    CONNECTOR_CREATED = 'CONNECTOR_CREATED'  # deprecated
    CONNECTOR_PAUSED = 'CONNECTOR_PAUSED'  # deprecated
    CONNECTOR_RUNNING = 'CONNECTOR_RUNNING'
    CONNECTOR_STOPPED = 'CONNECTOR_STOPPED'


class PartitionMode(Enum):
    """
    PartitionMode enum class, there are: ``USER_DEFINE``, ``SYSTEM_TIME``, ``EVENT_TIME``
    """
    USER_DEFINE = 'USER_DEFINE'
    SYSTEM_TIME = 'SYSTEM_TIME'
    EVENT_TIME = 'EVENT_TIME'


class AuthMode(Enum):
    """
    AuthMode enum class, there are: ``ak``, ``sts``
    """
    AK = 'ak'
    STS = 'sts'


class WriteMode(Enum):
    """
    WriteMode enum class, there are: ``put``, ``update``
    """
    PUT = 'PUT'
    UPDATE = 'UPDATE'


class ConnectorShardStatus(Enum):
    """
    ConnectorShardStatus enum class, there are: ``CONTEXT_PLANNED``, ``CONTEXT_EXECUTING``, ``CONTEXT_HANG``, \
    ``CONTEXT_PAUSED``, ``CONTEXT_FINISHED``, ``CONTEXT_DELETED``
    """
    CONTEXT_PLANNED = 'CONTEXT_PLANNED'
    CONTEXT_EXECUTING = 'CONTEXT_EXECUTING'
    CONTEXT_HANG = 'CONTEXT_HANG'
    CONTEXT_PAUSED = 'CONTEXT_PAUSED'
    CONTEXT_FINISHED = 'CONTEXT_FINISHED'
    CONTEXT_DELETED = 'CONTEXT_DELETED'


class ShardStatusEntry(object):
    """
    Connector shard status

    Members:
        current_sequence (:class:`int`): current sequence

        current_timestamp (:class:`int`) current timestamp

        done_time (:class:`int`) done timestamp

        last_error_message (:class:`str`): last error message

        state (:class:`datahub.models.connector.ConnectorShardStatus`): state

        update_time (:class:`int`): update time

        discard_count (:class:`int`): discard count

        worker_addr (:class:`str`): worker address
    """

    __slots__ = ('_current_sequence', '_current_timestamp', '_done_time', '_last_error_message',
                 '_state', '_update_time', '_discard_count', '_worker_addr')

    def __init__(self, current_sequence, current_timestamp, done_time, last_error_message, state,
                 update_time, discard_count, worker_addr):
        self._current_sequence = current_sequence
        self._current_timestamp = current_timestamp
        self._done_time = done_time
        self._last_error_message = last_error_message
        self._state = state
        self._update_time = update_time
        self._discard_count = discard_count
        self._worker_addr = worker_addr

    @property
    def current_sequence(self):
        return self._current_sequence

    @current_sequence.setter
    def current_sequence(self, value):
        self._current_sequence = value

    @property
    def current_timestamp(self):
        return self._current_timestamp

    @current_timestamp.setter
    def current_timestamp(self, value):
        self._current_timestamp = value

    @property
    def done_time(self):
        return self._done_time

    @done_time.setter
    def done_time(self, value):
        self._done_time = value

    @property
    def last_error_message(self):
        return self._last_error_message

    @last_error_message.setter
    def last_error_message(self, value):
        self._last_error_message = value

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state = value

    @property
    def update_time(self):
        return self._update_time

    @update_time.setter
    def update_time(self, value):
        self._update_time = value

    @property
    def discard_count(self):
        return self._discard_count

    @discard_count.setter
    def discard_count(self, value):
        self._discard_count = value

    @property
    def worker_addr(self):
        return self._worker_addr

    @worker_addr.setter
    def worker_addr(self, value):
        self._worker_addr = value

    @classmethod
    def from_dict(cls, dict_):
        current_sequence = dict_.get('CurrentSequence', -1)
        current_timestamp = dict_.get('CurrentTimestamp', -1)
        done_time = dict_.get('DoneTime', -1)
        last_error_message = dict_.get('LastErrorMessage', '')
        state = ConnectorShardStatus(dict_['State'])
        update_time = dict_.get('UpdateTime', -1)
        discard_count = dict_.get('DiscardCount', 0)
        worker_addr = dict_.get('WorkerAddress', '')
        return cls(current_sequence, current_timestamp, done_time, last_error_message, state, update_time,
                   discard_count, worker_addr)

    def to_json(self):
        return {
            'CurrentSequence': self._current_sequence,
            'CurrentTimestamp': self._current_timestamp,
            'DoneTime': self._done_time,
            'LastErrorMessage': self._last_error_message,
            'State': self._state.value,
            'UpdateTime': self._update_time,
            'DiscardCount': self._discard_count,
            'WorkerAddress': self._worker_addr
        }


class ConnectorOffset(object):
    """
    Connector offset

    Members:
        sequence (:class:`int`): sequence

        timestamp (:class:`int`): timestamp
    """
    __slots__ = ('_sequence', '_timestamp')

    def __init__(self, sequence=-1, timestamp=-1):
        self._sequence = sequence
        self._timestamp = timestamp

    @property
    def sequence(self):
        return self._sequence

    @sequence.setter
    def sequence(self, value):
        self._sequence = value

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, value):
        self._timestamp = value


@six.add_metaclass(abc.ABCMeta)
class ConnectorConfig(object):
    """
    Connector config class
    """

    @abc.abstractmethod
    def to_json(self):
        pass

    @classmethod
    @abc.abstractmethod
    def from_dict(cls, dict_):
        pass

    def __repr__(self):
        return to_str(self.to_json())


class OdpsConnectorConfig(ConnectorConfig):
    """
    Connector config for odps

    Members:
        project_name (:class:`str`): odps project name

        table_name (:class:`str`): odps table name

        odps_endpoint (:class:`str`): odps endpoint

        tunnel_endpoint (:class:`str`): tunnel endpoint

        access_id (:class:`str`): odps access id

        access_key (:class:`str`): odps access key

        partition_mode (:class:`datahub.models.connector.PartitionMode`): partition mode

        time_range (:class:`int`): time range

        partition_config(:class:`collections.OrderedDict`): partition config
    """

    __slots__ = ('_project_name', '_table_name', '_odps_endpoint', '_tunnel_endpoint', '_access_id',
                 '_access_key', '_partition_mode', '_time_range', '_partition_config')

    def __init__(self, project_name, table_name, odps_endpoint, tunnel_endpoint,
                 access_id, access_key, partition_mode, time_range, partition_config=None):
        if partition_config is None:
            partition_config = {}
        self._project_name = project_name
        self._table_name = table_name
        self._odps_endpoint = odps_endpoint
        self._tunnel_endpoint = tunnel_endpoint
        self._access_id = access_id
        self._access_key = access_key
        self._partition_mode = partition_mode
        self._time_range = time_range
        self._partition_config = partition_config

    @property
    def project_name(self):
        return self._project_name

    @project_name.setter
    def project_name(self, value):
        self._project_name = value

    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, value):
        self._table_name = value

    @property
    def odps_endpoint(self):
        return self._odps_endpoint

    @odps_endpoint.setter
    def odps_endpoint(self, value):
        self._odps_endpoint = value

    @property
    def tunnel_endpoint(self):
        return self._tunnel_endpoint

    @tunnel_endpoint.setter
    def tunnel_endpoint(self, value):
        self._tunnel_endpoint = value

    @property
    def access_id(self):
        return self._access_id

    @access_id.setter
    def access_id(self, value):
        self._access_id = value

    @property
    def access_key(self):
        return self._access_key

    @access_key.setter
    def access_key(self, value):
        self._access_key = value

    @property
    def partition_mode(self):
        return self._partition_mode

    @partition_mode.setter
    def partition_mode(self, value):
        self._partition_mode = value

    @property
    def time_range(self):
        return self._time_range

    @time_range.setter
    def time_range(self, value):
        self._time_range = value

    @property
    def partition_config(self):
        return self._partition_config

    @partition_config.setter
    def partition_config(self, value):
        self._partition_config = value

    def to_json(self):
        data = {
            "Project": self._project_name,
            "Table": self._table_name,
            "OdpsEndpoint": self._odps_endpoint,
            "TunnelEndpoint": self._tunnel_endpoint
        }
        if self._access_id:
            data["AccessId"] = self._access_id
        if self._access_key:
            data["AccessKey"] = self._access_key
        if self._partition_mode:
            data['PartitionMode'] = self._partition_mode.value
        if self._time_range and self._time_range > 0:
            data['TimeRange'] = self._time_range
        if self._partition_config:
            data['PartitionConfig'] = self._partition_config
        return data

    @classmethod
    def from_dict(cls, dict_):
        partition_config_list = json.loads(to_text(dict_.get('PartitionConfig', '{}')))
        partition_config = OrderedDict()
        for partition in partition_config_list:
            partition_config[partition['key']] = partition['value']
        return cls(dict_.get('Project', ''), dict_.get('Table', ''), dict_.get('OdpsEndpoint', ''),
                   dict_.get('TunnelEndpoint', ''), dict_.get('AccessId', ''), dict_.get('AccessKey', ''),
                   PartitionMode(dict_['PartitionMode']), int(dict_.get('TimeRange', -1)), partition_config)


class DatabaseConnectorConfig(ConnectorConfig):
    """
    Connector config for database

    Members:
        host (:class:`str`): host

        port (:class:`int`): port

        database (:class:`str`): database

        user (:class:`str`): user

        password (:class:`str`): password

        table (:class:`str`): table

        ignore (:class:`bool`): ignore insert error
    """

    __slots__ = ('_host', '_port', '_database', '_user', '_password', '_table', '_ignore')

    def __init__(self, host, port, database, user, password, table, ignore):
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password
        self._table = table
        self._ignore = ignore

    @property
    def host(self):
        return self._host

    @host.setter
    def host(self, value):
        self._host = value

    @property
    def port(self):
        return self._port

    @port.setter
    def port(self, value):
        self._port = value

    @property
    def database(self):
        return self._database

    @database.setter
    def database(self, value):
        self._database = value

    @property
    def user(self):
        return self._user

    @user.setter
    def user(self, value):
        self._user = value

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, value):
        self._password = value

    @property
    def table(self):
        return self._table

    @table.setter
    def table(self, value):
        self._table = value

    @property
    def ignore(self):
        return self._ignore

    @ignore.setter
    def ignore(self, value):
        self._ignore = value

    def to_json(self):
        data = {
            "Host": self._host,
            "Port": to_str(self._port),
            "Ignore": bool_to_str(self._ignore),
            "Database": self._database,
            "User": self._user,
            "Password": self._password,
            "Table": self._table,
        }
        return data

    @classmethod
    def from_dict(cls, dict_):
        host = dict_.get('Host', '')
        port = int(dict_.get('Port', '0'))
        database = dict_.get('Database', '')
        user = dict_.get('User', '')
        password = dict_.get('Password', '')
        table = dict_.get('Table', '')
        ignore = bool(dict_.get('Ignore', 'True'))

        return cls(host, port, database, user, password, table, ignore)


class EsConnectorConfig(ConnectorConfig):
    """
    Connector config for ElasticSearch

    Members:
        index (:class:`str`): index

        endpoint (:class:`str`): endpoint

        user (:class:`str`): user

        password (:class:`str`): password

        id_fields (:class:`list`): id fields

        type_fields (:class:`list`): type fields

        proxy_mode (:class:`bool`): proxy mode
    """

    __slots__ = ('_index', '_endpoint', '_user', '_password', '_id_fields',
                 '_type_fields', '_proxy_mode')

    def __init__(self, index, endpoint, user, password, id_fields, type_fields, proxy_mode):
        self._index = index
        self._endpoint = endpoint
        self._user = user
        self._password = password
        self._id_fields = id_fields
        self._type_fields = type_fields
        self._proxy_mode = proxy_mode

    @property
    def index(self):
        return self._index

    @index.setter
    def index(self, value):
        self._index = value

    @property
    def endpoint(self):
        return self._endpoint

    @endpoint.setter
    def endpoint(self, value):
        self._endpoint = value

    @property
    def user(self):
        return self._user

    @user.setter
    def user(self, value):
        self._user = value

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, value):
        self._password = value

    @property
    def id_fields(self):
        return self._id_fields

    @id_fields.setter
    def id_fields(self, value):
        self._id_fields = value

    @property
    def type_fields(self):
        return self._type_fields

    @type_fields.setter
    def type_fields(self, value):
        self._type_fields = value

    @property
    def proxy_mode(self):
        return self._proxy_mode

    @proxy_mode.setter
    def proxy_mode(self, value):
        self._proxy_mode = value

    def to_json(self):
        data = {
            "Index": self._index,
            "Endpoint": self._endpoint,
            "User": self._user,
            "Password": self._password,
            "IDFields": self._id_fields,
            "TypeFields": self._type_fields,
            "ProxyMode": bool_to_str(self._proxy_mode)
        }
        return data

    @classmethod
    def from_dict(cls, dict_):
        index = dict_.get('Index', '')
        endpoint = dict_.get('Endpoint', '')
        user = dict_.get('User', '')
        password = dict_.get('Password', '')
        id_fields = json.loads(dict_.get('IDFields', ''))
        type_fields = json.loads(dict_.get('TypeFields', ''))
        proxy_mode = bool(dict_.get('ProxyMode', 'False'))

        return cls(index, endpoint, user, password, id_fields, type_fields, proxy_mode)


class FcConnectorConfig(ConnectorConfig):
    """
    Connector config for FunctionCompute

    Members:
        endpoint (:class:`str`): endpoint

        service (:class:`str`): service

        func (:class:`str`): function

        auth_mode (:class:`datahub.models.connector.AuthMode`): auth mode

        access_id (:class:`str`): access id

        access_key (:class:`str`): access key
    """

    __slots__ = ('_endpoint', '_service', '_func', '_auth_mode', '_access_id', '_access_key')

    def __init__(self, endpoint, service, func, auth_mode, access_id='', access_key=''):
        self._endpoint = endpoint
        self._service = service
        self._func = func
        self._auth_mode = auth_mode
        self._access_id = access_id
        self._access_key = access_key

    @property
    def endpoint(self):
        return self._endpoint

    @endpoint.setter
    def endpoint(self, value):
        self._endpoint = value

    @property
    def service(self):
        return self._service

    @service.setter
    def service(self, value):
        self._service = value

    @property
    def func(self):
        return self._func

    @func.setter
    def func(self, value):
        self._func = value

    @property
    def auth_mode(self):
        return self._auth_mode

    @auth_mode.setter
    def auth_mode(self, value):
        self._auth_mode = value

    @property
    def access_id(self):
        return self._access_id

    @access_id.setter
    def access_id(self, value):
        self._access_id = value

    @property
    def access_key(self):
        return self._access_key

    @access_key.setter
    def access_key(self, value):
        self._access_key = value

    def to_json(self):
        data = {
            "Endpoint": self._endpoint,
            "Service": self._service,
            "Function": self._func,
            "AuthMode": self._auth_mode.value
        }
        if self._auth_mode == AuthMode.AK and self.access_id and self.access_key:
            data["AccessId"] = self.access_id
            data["AccessKey"] = self.access_key
        return data

    @classmethod
    def from_dict(cls, dict_):
        endpoint = dict_.get('Endpoint', '')
        service = dict_.get('Service', '')
        func = dict_.get('Function', '')
        auth_mode = AuthMode(dict_.get('AuthMode', 'sts'))
        access_id = dict_.get('AccessId', '')
        access_key = dict_.get('AccessKey', '')

        return cls(endpoint, service, func, auth_mode, access_id, access_key)


class OssConnectorConfig(ConnectorConfig):
    """
    Connector config for ObjectStoreService

    Members:
        endpoint (:class:`str`): endpoint

        bucket (:class:`str`): bucket

        prefix (:class:`str`): prefix

        time_format (:class:`str`): time format

        time_range (:class:`int`): time range

        auth_mode (:class:`datahub.models.connector.AuthMode`): auth mode

        access_id (:class:`str`): access id

        access_key (:class:`str`): access key
    """

    __slots__ = (
        '_endpoint', '_bucket', '_prefix', '_time_format', '_time_range', '_auth_mode', '_access_id', '_access_key')

    def __init__(self, endpoint, bucket, prefix, time_format, time_range, auth_mode, access_id='', access_key=''):
        self._endpoint = endpoint
        self._bucket = bucket
        self._prefix = prefix
        self._time_format = time_format
        self._time_range = time_range
        self._auth_mode = auth_mode
        self._access_id = access_id
        self._access_key = access_key

    @property
    def endpoint(self):
        return self._endpoint

    @endpoint.setter
    def endpoint(self, value):
        self._endpoint = value

    @property
    def bucket(self):
        return self._bucket

    @bucket.setter
    def bucket(self, value):
        self._bucket = value

    @property
    def prefix(self):
        return self._prefix

    @prefix.setter
    def prefix(self, value):
        self._prefix = value

    @property
    def time_format(self):
        return self._time_format

    @time_format.setter
    def time_format(self, value):
        self._time_format = value

    @property
    def time_range(self):
        return self._time_range

    @time_range.setter
    def time_range(self, value):
        self._time_range = value

    @property
    def auth_mode(self):
        return self._auth_mode

    @auth_mode.setter
    def auth_mode(self, value):
        self._auth_mode = value

    @property
    def access_id(self):
        return self._access_id

    @access_id.setter
    def access_id(self, value):
        self._access_id = value

    @property
    def access_key(self):
        return self._access_key

    @access_key.setter
    def access_key(self, value):
        self._access_key = value

    def to_json(self):
        data = {
            "Endpoint": self._endpoint,
            "Bucket": self._bucket,
            "Prefix": self._prefix,
            "TimeFormat": self._time_format,
            "TimeRange": self._time_range,
            "AuthMode": self._auth_mode.value
        }
        if self._auth_mode == AuthMode.AK and self.access_id and self.access_key:
            data["AccessId"] = self.access_id
            data["AccessKey"] = self.access_key
        return data

    @classmethod
    def from_dict(cls, dict_):
        endpoint = dict_.get('Endpoint', '')
        bucket = dict_.get('Bucket', '')
        prefix = dict_.get('Prefix', '')
        time_format = dict_.get('TimeFormat', '')
        time_range = int(dict_.get('TimeRange', '-1'))
        auth_mode = AuthMode(dict_.get('AuthMode', 'sts'))
        access_id = dict_.get('AccessId', '')
        access_key = dict_.get('AccessKey', '')

        return cls(endpoint, bucket, prefix, time_format, time_range, auth_mode, access_id, access_key)


class OtsConnectorConfig(ConnectorConfig):
    """
    Connector config for OpenTableStore

    Members:
        endpoint (:class:`str`): endpoint

        instance (:class:`str`): instance

        table (:class:`str`): table

        auth_mode (:class:`datahub.models.connector.AuthMode`): auth mode

        access_id (:class:`str`): access id

        access_key (:class:`str`): access key

        write_mode (:class:`datahub.models.connector.WriteMode`): write mode
    """

    __slots__ = ('_endpoint', '_instance', '_table', '_auth_mode', '_access_id', '_access_key', '_write_mode')

    def __init__(self, endpoint, instance, table, auth_mode, access_id='', access_key='', write_mode=WriteMode.PUT):
        self._endpoint = endpoint
        self._instance = instance
        self._table = table
        self._auth_mode = auth_mode
        self._access_id = access_id
        self._access_key = access_key
        self._write_mode = write_mode

    @property
    def endpoint(self):
        return self._endpoint

    @endpoint.setter
    def endpoint(self, value):
        self._endpoint = value

    @property
    def instance(self):
        return self._instance

    @instance.setter
    def instance(self, value):
        self._instance = value

    @property
    def table(self):
        return self._table

    @table.setter
    def table(self, value):
        self._table = value

    @property
    def auth_mode(self):
        return self._auth_mode

    @auth_mode.setter
    def auth_mode(self, value):
        self._auth_mode = value

    @property
    def access_id(self):
        return self._access_id

    @access_id.setter
    def access_id(self, value):
        self._access_id = value

    @property
    def access_key(self):
        return self._access_key

    @access_key.setter
    def access_key(self, value):
        self._access_key = value

    @property
    def write_mode(self):
        return self._write_mode

    @write_mode.setter
    def write_mode(self, value):
        self._write_mode = value

    def to_json(self):
        data = {
            "Endpoint": self._endpoint,
            "InstanceName": self._instance,
            "TableName": self._table,
            "AuthMode": self._auth_mode.value,
            "WriteMode": self._write_mode.value
        }
        if self._auth_mode == AuthMode.AK and self.access_id and self.access_key:
            data["AccessId"] = self.access_id
            data["AccessKey"] = self.access_key
        return data

    @classmethod
    def from_dict(cls, dict_):
        endpoint = dict_.get('Endpoint', '')
        instance = dict_.get('InstanceName', '')
        table = dict_.get('TableName', '')
        auth_mode = AuthMode(dict_.get('AuthMode', 'sts'))
        access_id = dict_.get('AccessId', '')
        access_key = dict_.get('AccessKey', '')
        write_mode = WriteMode(dict_.get('WriteMode', 'PUT'))

        return cls(endpoint, instance, table, auth_mode, access_id, access_key, write_mode)


class DataHubConnectorConfig(ConnectorConfig):
    """
    Connector config for DataHub

    Members:
        endpoint (:class:`str`): endpoint

        project (:class:`str`): project

        topic (:class:`str`): topic

        auth_mode (:class:`datahub.models.connector.AuthMode`): auth mode

        access_id (:class:`str`): access id

        access_key (:class:`str`): access key
    """

    __slots__ = ('_endpoint', '_project', '_topic', '_auth_mode', '_access_id', '_access_key')

    def __init__(self, endpoint, project, topic, auth_mode, access_id='', access_key=''):
        self._endpoint = endpoint
        self._project = project
        self._topic = topic
        self._auth_mode = auth_mode
        self._access_id = access_id
        self._access_key = access_key

    @property
    def endpoint(self):
        return self._endpoint

    @endpoint.setter
    def endpoint(self, value):
        self._endpoint = value

    @property
    def project(self):
        return self._project

    @project.setter
    def project(self, value):
        self._project = value

    @property
    def topic(self):
        return self._topic

    @topic.setter
    def topic(self, value):
        self._topic = value

    @property
    def auth_mode(self):
        return self._auth_mode

    @auth_mode.setter
    def auth_mode(self, value):
        self._auth_mode = value

    @property
    def access_id(self):
        return self._access_id

    @access_id.setter
    def access_id(self, value):
        self._access_id = value

    @property
    def access_key(self):
        return self._access_key

    @access_key.setter
    def access_key(self, value):
        self._access_key = value

    def to_json(self):
        data = {
            "Endpoint": self._endpoint,
            "Project": self._project,
            "Topic": self._topic,
            "AuthMode": self._auth_mode.value
        }
        if self._auth_mode == AuthMode.AK and self.access_id and self.access_key:
            data["AccessId"] = self.access_id
            data["AccessKey"] = self.access_key
        return data

    @classmethod
    def from_dict(cls, dict_):
        endpoint = dict_.get('Endpoint', '')
        project = dict_.get('Project', '')
        topic = dict_.get('Topic', '')
        auth_mode = AuthMode(dict_.get('AuthMode', 'sts'))
        access_id = dict_.get('AccessId', '')
        access_key = dict_.get('AccessKey', '')

        return cls(endpoint, project, topic, auth_mode, access_id, access_key)


connector_config_dict = {
    ConnectorType.SINK_ODPS: OdpsConnectorConfig,
    ConnectorType.SINK_ADS: DatabaseConnectorConfig,
    ConnectorType.SINK_ES: EsConnectorConfig,
    ConnectorType.SINK_FC: FcConnectorConfig,
    ConnectorType.SINK_MYSQL: DatabaseConnectorConfig,
    ConnectorType.SINK_OSS: OssConnectorConfig,
    ConnectorType.SINK_OTS: OtsConnectorConfig,
    ConnectorType.SINK_DATAHUB: DataHubConnectorConfig
}


def get_connector_builder_by_type(connector_type):
    builder = connector_config_dict.get(connector_type, None)
    if not builder:
        raise DatahubException('unsupported connector type')
    return builder
