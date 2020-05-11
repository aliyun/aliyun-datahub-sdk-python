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

import json
import logging
import platform
import socket
from enum import Enum
from string import Template

import requests
import six
from requests.adapters import HTTPAdapter

from .exceptions import exception_handler, DatahubException
from .models.compress import CompressFormat, get_compressor
from .utils import gen_rfc822_date, to_text, to_binary
from .version import __version__, __datahub_client_version__

logger = logging.getLogger('datahub.rest')
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


class HTTPMethod(Enum):
    GET = 'GET'
    POST = 'POST'
    PUT = 'PUT'
    HEAD = 'HEAD'
    DELETE = 'DELETE'


class ContentType(Enum):
    HTTP_JSON = 'application/json'
    HTTP_PROTOBUF = 'application/x-protobuf'


class Headers(object):
    ACCEPT_ENCODING = "Accept-Encoding"
    AUTHORIZATION = "Authorization"
    CACHE_CONTROL = "Cache-Control"
    CHUNKED = "chunked"
    CLIENT_VERSION = 'x-datahub-client-version'
    CONTENT_DISPOSITION = "Content-Disposition"
    CONTENT_ENCODING = "Content-Encoding"
    CONTENT_LENGTH = "Content-Length"
    CONTENT_MD5 = "Content-MD5"
    CONTENT_TYPE = "Content-Type"
    DATE = "Date"
    ETAG = "ETag"
    EXPIRES = "Expires"
    HOST = "Host"
    LAST_MODIFIED = "Last-Modified"
    LOCATION = "Location"
    RANGE = "Range"
    RAW_SIZE = "x-datahub-content-raw-size"
    REQUEST_ACTION = "x-datahub-request-action"
    REQUEST_ID = "x-datahub-request-id"
    SECURITY_TOKEN = "x-datahub-security-token"
    TRANSFER_ENCODING = "Transfer-Encoding"
    USER_AGENT = "User-Agent"


class Path(object):
    PROJECTS = '/projects'
    PROJECT = '/projects/%s'
    TOPICS = '/projects/%s/topics'
    TOPIC = '/projects/%s/topics/%s'
    SHARDS = '/projects/%s/topics/%s/shards'
    SHARD = '/projects/%s/topics/%s/shards/%s'
    CONNECTORS = '/projects/%s/topics/%s/connectors'
    CONNECTOR = '/projects/%s/topics/%s/connectors/%s'
    DONE_TIME = '/projects/%s/topics/%s/connectors/%s?donetime'
    SUBSCRIPTIONS = '/projects/%s/topics/%s/subscriptions'
    SUBSCRIPTION = '/projects/%s/topics/%s/subscriptions/%s'
    OFFSETS = '/projects/%s/topics/%s/subscriptions/%s/offsets'


class CommonResponseResult(object):
    __slots__ = ('_status_code', '_request_id', '_error_code', '_error_msg')

    def __init__(self, resp, content):
        if resp is not None:
            try:
                self._status_code = resp.status_code
                self._request_id = resp.headers[Headers.REQUEST_ID]
                content = json.loads(to_text(content))
                self._error_code = content['ErrorCode']
                self._error_msg = content['ErrorMessage']
            except Exception:
                logger.error('Decode json message error, content: %s' % to_text(content))
                raise DatahubException(self._status_code, self._request_id, '',
                                       'Decode json message error, content: %s' % to_text(content))

    @property
    def status_code(self):
        return self._status_code

    @property
    def request_id(self):
        return self._request_id

    @property
    def error_code(self):
        return self._error_code

    @property
    def error_msg(self):
        return self._error_msg


def get_host_ip():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ip = '0.0.0.0'
    try:
        sock.connect(('8.8.8.8', 80))
        ip = sock.getsockname()[0]
    except socket.error as e:
        logger.error('can not get host ip, msg: %s' % e)
    finally:
        sock.close()
    return ip


def default_user_agent():
    os_version = platform.platform()
    py_version = platform.python_version()
    ip_addr = get_host_ip()
    ua_template = Template('$pydatahub_version $python_version $os_version $ip_addr')
    return ua_template.safe_substitute(pydatahub_version='pydatahub/%s' % __version__,
                                       python_version='python/%s' % py_version,
                                       os_version='%s' % os_version,
                                       ip_addr='ip/%s' % ip_addr)


class RestClient(object):
    """Restful client enhanced by URL building and request signing facilities.
    """

    def __init__(self, account, endpoint, user_agent=None, proxies=None, stream=False, retry_times=3, conn_timeout=5,
                 read_timeout=120, pool_connections=10, pool_maxsize=10, exception_handler_=exception_handler):
        if endpoint.endswith('/'):
            endpoint = endpoint[:-1]
        self._account = account
        self._endpoint = endpoint
        self._user_agent = user_agent or default_user_agent()
        self._proxies = proxies
        self._stream = stream
        self._retry_times = retry_times
        self._conn_timeout = conn_timeout
        self._read_timeout = read_timeout

        self._session = requests.Session()
        self._session.headers.update({Headers.ACCEPT_ENCODING: ''})

        # mount adapters with retry times
        adapter = HTTPAdapter(pool_connections=pool_connections, pool_maxsize=pool_maxsize,
                              max_retries=self._retry_times)
        self._session.mount('http://', adapter)
        self._session.mount('https://', adapter)

        # exception handler
        self._exception_handler = exception_handler_

    def __del__(self):
        self._session.close()

    @property
    def endpoint(self):
        return self._endpoint

    @endpoint.setter
    def endpoint(self, value):
        if value.endswith('/'):
            value = value[:-1]
        self._endpoint = value

    @property
    def account(self):
        return self._account

    @account.setter
    def account(self, value):
        self._account = value

    @property
    def user_agent(self):
        return self._user_agent

    @user_agent.setter
    def user_agent(self, value):
        self._user_agent = value

    @property
    def proxies(self):
        return self._proxies

    @proxies.setter
    def proxies(self, value):
        self._proxies = value

    @staticmethod
    def is_ok(resp):
        """
        return True if status code < 400
        """
        return resp.ok

    def __common_headers(self):
        headers = {
            Headers.CLIENT_VERSION: __datahub_client_version__,
            Headers.USER_AGENT: self._user_agent,
            Headers.CONTENT_TYPE: ContentType.HTTP_JSON.value,
            Headers.DATE: gen_rfc822_date()
        }
        if self._account.security_token:
            headers[Headers.SECURITY_TOKEN] = self._account.security_token
        return headers

    @staticmethod
    def __compress_content(content, compress_format):
        compressor = get_compressor(compress_format)
        if compressor:
            compressed = compressor.compress(to_binary(content))
            compress_headers = {
                Headers.ACCEPT_ENCODING: compress_format.value
            }
            if len(compressed) < len(content):
                compress_headers[Headers.RAW_SIZE] = to_text(len(content))
                compress_headers[Headers.CONTENT_ENCODING] = compress_format.value
                return compressed, compress_headers

            return content, compress_headers
        return content, {}

    @staticmethod
    def __decompress_response(response):
        content_encoding = response.headers.get(Headers.CONTENT_ENCODING, '')
        raw_size = int(response.headers.get(Headers.RAW_SIZE, '0'))
        compressor = get_compressor(content_encoding)

        if compressor:
            return compressor.decompress(to_binary(response.content), raw_size)
        return response.content

    def request(self, method, url, compress_format=CompressFormat.NONE, **kwargs):
        url = "%s%s" % (self._endpoint, url)

        # Construct user agent without handling the letter case.
        headers = self.__common_headers()
        extra_headers = kwargs.get('headers', {})
        extra_headers = dict((k, str(v)) for k, v in six.iteritems(extra_headers))
        headers.update(extra_headers)

        # Compress content and set headers
        if 'data' in kwargs:
            data, compress_headers = RestClient.__compress_content(kwargs['data'], compress_format)
            headers.update(compress_headers)
            kwargs['data'] = data
            headers[Headers.CONTENT_LENGTH] = to_text(len(data))

        kwargs['headers'] = headers

        req = requests.Request(method.value, url, **kwargs)
        prepared_req = self._session.prepare_request(req)

        self._account.sign_request(prepared_req)

        logger.debug('full request url: %s\nrequest headers:\n%s\nrequest body:\n%s'
                     % (prepared_req.url, prepared_req.headers, prepared_req.body))

        resp = self._session.send(prepared_req,
                                  stream=self._stream,
                                  timeout=(self._conn_timeout, self._read_timeout),
                                  proxies=self._proxies,
                                  verify=False)

        logger.debug('response.status_code: %d' % resp.status_code)
        logger.debug('response.headers: \n%s' % resp.headers)
        if not self._stream:
            logger.debug('response.content: %s\n' % resp.content)

        content = RestClient.__decompress_response(resp)

        # Automatically detect error
        if not RestClient.is_ok(resp) and self._exception_handler is not None:
            status_code = resp.status_code
            request_id = resp.headers.get(Headers.REQUEST_ID, '')
            try:
                content_data = json.loads(to_text(content))
                error_code = content_data['ErrorCode']
                error_msg = content_data['ErrorMessage']
            except Exception:
                logger.error('Decode json message error, content: %s' % to_text(content))
                raise DatahubException('Decode json message error, content: %s' % to_text(content),
                                       status_code, request_id, '')

            logger.error("status_code: %d, request_id: %s, error_code: %s, error_msg: %s"
                         % (status_code, request_id, error_code, error_msg))
            self._exception_handler.raise_exception(error_msg, status_code, request_id, error_code)

        return content

    def get(self, url, **kwargs):
        return self.request(HTTPMethod.GET, url, **kwargs)

    def post(self, url, **kwargs):
        return self.request(HTTPMethod.POST, url, **kwargs)

    def put(self, url, **kwargs):
        return self.request(HTTPMethod.PUT, url, **kwargs)

    def head(self, url, **kwargs):
        return self.request(HTTPMethod.HEAD, url, **kwargs)

    def delete(self, url, **kwargs):
        return self.request(HTTPMethod.DELETE, url, **kwargs)
