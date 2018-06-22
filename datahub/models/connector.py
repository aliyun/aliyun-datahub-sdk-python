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
from enum import Enum

import six

from datahub.exceptions import DatahubException
from ..utils import to_str, bool_to_str

if six.PY3:
    long = int


class ConnectorType(Enum):
    """
    ConnectorType enum class, there are: ``SINK_ODPS``, ``SINK_ADS``, ``SINK_ES``, ``SINK_FC``, \
    ``SINK_MYSQL``, ``SINK_OSS``, ``SINK_OTS``
    """
    SINK_ODPS = 'sink_odps'
    SINK_ADS = 'sink_ads'
    SINK_ES = 'sink_es'
    SINK_FC = 'sink_fc'
    SINK_MYSQL = 'sink_mysql'
    SINK_OSS = 'sink_oss'
    SINK_OTS = 'sink_ots'


class ConnectorState(Enum):
    """
    ConnectorState enum class, there are: ``CONNECTOR_CREATED``, ``CONNECTOR_RUNNING``, ``CONNECTOR_PAUSED``
    """
    CONNECTOR_CREATED = 'CONNECTOR_CREATED'
    CONNECTOR_RUNNING = 'CONNECTOR_RUNNING'
    CONNECTOR_PAUSED = 'CONNECTOR_PAUSED'


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
                 access_id, access_key, partition_mode, time_range, partition_config):
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
            "TunnelEndpoint": self._tunnel_endpoint,
            "AccessId": self._access_id,
            "AccessKey": self._access_key
        }
        if self._partition_mode:
            data['PartitionMode'] = self._partition_mode.value
        if self._time_range:
            data['TimeRange'] = self._time_range
        if self._partition_config:
            data['PartitionConfig'] = self._partition_config
        return data

    @classmethod
    def from_dict(cls, dict_):
        access_id = dict_['AccessId'] if 'AccessId' in dict_ else ''
        access_key = dict_['AccessKey'] if 'AccessKey' in dict_ else ''
        partition_config = dict_['PartitionConfig'] if 'PartitionConfig' in dict_ else ''
        return cls(dict_['Project'], dict_['Table'], dict_['OdpsEndpoint'], dict_['TunnelEndpoint'],
                   access_id, access_key, PartitionMode(dict_['PartitionMode']), dict_['TimeRange'], partition_config)


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

        max_commit_size (:class:`int`): max commit size (KB)

        ignore (:class:`bool`): ignore insert error
    """

    __slots__ = ('_host', '_port', '_database', '_user', '_password', '_table', '_max_commit_size', '_ignore')

    def __init__(self, host, port, database, user, password, table, max_commit_size, ignore):
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password
        self._table = table
        self._max_commit_size = max_commit_size
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
    def max_commit_size(self):
        return self._max_commit_size

    @max_commit_size.setter
    def max_commit_size(self, value):
        self._max_commit_size = value

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
        if self._max_commit_size >= 0:
            data["MaxCommitSize"] = to_str(self._max_commit_size)
        return data

    @classmethod
    def from_dict(cls, dict_):
        host = dict_.get('Host', '')
        port = int(dict_.get('Port', '0'))
        database = dict_.get('Database', '')
        user = dict_.get('User', '')
        password = dict_.get('Password', '')
        table = dict_.get('Table', '')
        max_commit_size = long(dict_.get('MaxCommitSize', '-1'))
        ignore = bool(dict_.get('Ignore', 'True'))

        return cls(host, port, database, user, password, table, max_commit_size, ignore)


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

        max_commit_size (:class:`int`): max commit size

        proxy_mode (:class:`bool`): proxy mode
    """

    __slots__ = ('_index', '_endpoint', '_user', '_password', '_id_fields',
                 '_type_fields', '_max_commit_size', '_proxy_mode')

    def __init__(self, index, endpoint, user, password, id_fields, type_fields, max_commit_size, proxy_mode):
        self._index = index
        self._endpoint = endpoint
        self._user = user
        self._password = password
        self._id_fields = id_fields
        self._type_fields = type_fields
        self._max_commit_size = max_commit_size
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
    def max_commit_size(self):
        return self._max_commit_size

    @max_commit_size.setter
    def max_commit_size(self, value):
        self._max_commit_size = value

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
        if self._max_commit_size >= 0:
            data["MaxCommitSize"] = to_str(self._max_commit_size)
        return data

    @classmethod
    def from_dict(cls, dict_):
        index = dict_.get('Index', '')
        endpoint = dict_.get('Endpoint', '')
        user = dict_.get('User', '')
        password = dict_.get('Password', '')
        id_fields = dict_.get('IDFields', '')
        type_fields = dict_.get('TypeFields', '')
        max_commit_size = long(dict_.get('MaxCommitSize', '-1'))
        proxy_mode = bool(dict_.get('ProxyMode', 'False'))

        return cls(index, endpoint, user, password, id_fields, type_fields, max_commit_size, proxy_mode)


class FcConnectorConfig(ConnectorConfig):
    """
    Connector config for FunctionCompute

    Members:
        endpoint (:class:`str`): endpoint

        service (:class:`str`): service

        func (:class:`str`): function

        invocation_role (:class:`str`): invocation role

        batch_size (:class:`int`): batch size

        start_position (:class:`str`): start position

        start_timestamp (:class:`int`): start timestamp

        auth_mode (:class:`datahub.models.connector.AuthMode`): auth mode

        access_id (:class:`str`): access id

        access_key (:class:`str`): access key
    """

    __slots__ = ('_endpoint', '_service', '_func', '_invocation_role', '_batch_size',
                 '_start_position', '_start_timestamp', '_auth_mode', '_access_id', '_access_key')

    def __init__(self, endpoint, service, func, invocation_role, batch_size,
                 auth_mode, start_position, start_timestamp=0, access_id='', access_key=''):
        self._endpoint = endpoint
        self._service = service
        self._func = func
        self._invocation_role = invocation_role
        self._batch_size = batch_size
        self._start_position = start_position
        self._start_timestamp = start_timestamp
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
    def invocation_role(self):
        return self._invocation_role

    @invocation_role.setter
    def invocation_role(self, value):
        self._invocation_role = value

    @property
    def batch_size(self):
        return self._batch_size

    @batch_size.setter
    def batch_size(self, value):
        self._batch_size = value

    @property
    def start_position(self):
        return self._start_position

    @start_position.setter
    def start_position(self, value):
        self._start_position = value

    @property
    def start_timestamp(self):
        return self._start_timestamp

    @start_timestamp.setter
    def start_timestamp(self, value):
        self._start_timestamp = value

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
            "BatchSize": self._batch_size,
            "StartPosition": self._start_position,
            "AuthMode": self._auth_mode.value
        }
        if self._start_position == 'SYSTEM_TIME' and self._start_timestamp >= 0:
            data["StartTimestamp"] = self._start_timestamp
        if self._auth_mode == AuthMode.AK and self.access_id and self.access_key:
            data["AccessId"] = self.access_id
            data["AccessKey"] = self.access_key
        elif self._auth_mode == AuthMode.STS and self._invocation_role:
            data["InvocationRole"] = self._invocation_role
        return data

    @classmethod
    def from_dict(cls, dict_):
        endpoint = dict_.get('Endpoint', '')
        service = dict_.get('Service', '')
        func = dict_.get('Function', '')
        invocation_role = dict_.get('InvocationRole', '')
        batch_size = int(dict_.get('BatchSize', '-1'))
        start_position = dict_.get('StartPosition', '')
        start_timestamp = long(dict_.get('StartTimestamp', '-1'))
        auth_mode = AuthMode(dict_.get('AuthMode', 'sts'))
        access_id = dict_.get('AccessId', '')
        access_key = dict_.get('AccessKey', '')

        return cls(endpoint, service, func, invocation_role, batch_size, auth_mode,
                   start_position, start_timestamp, access_id, access_key)


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

    def __init__(self, endpoint, bucket, prefix, time_format, time_range, auth_mode, access_id, access_key):
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
    """

    __slots__ = ('_endpoint', '_instance', '_table', '_auth_mode', '_access_id', '_access_key')

    def __init__(self, endpoint, instance, table, auth_mode, access_id, access_key):
        self._endpoint = endpoint
        self._instance = instance
        self._table = table
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

    def to_json(self):
        data = {
            "Endpoint": self._endpoint,
            "InstanceName": self._instance,
            "TableName": self._table,
            "AuthMode": self._auth_mode.value
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

        return cls(endpoint, instance, table, auth_mode, access_id, access_key)


connector_config_dict = {
    ConnectorType.SINK_ODPS: OdpsConnectorConfig,
    ConnectorType.SINK_ADS: DatabaseConnectorConfig,
    ConnectorType.SINK_ES: EsConnectorConfig,
    ConnectorType.SINK_FC: FcConnectorConfig,
    ConnectorType.SINK_MYSQL: DatabaseConnectorConfig,
    ConnectorType.SINK_OSS: OssConnectorConfig,
    ConnectorType.SINK_OTS: OtsConnectorConfig
}


def get_connector_builder_by_type(connector_type):
    builder = connector_config_dict.get(connector_type, None)
    if not builder:
        raise DatahubException('unsupported connector type')
    return builder
