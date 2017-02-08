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
import sys
import json
import hmac
import base64
import requests
import datetime
import platform
from hashlib import sha1
from string import Template

from ..thirdparty import six
from ..utils import Logger, gen_rfc822_date, benchmark, counter
from .. import __version__, __datahub_client_version__

class HTTPMethod(object):
    GET = 'GET'
    POST = 'POST'
    PUT = 'PUT'
    HEAD = 'HEAD'
    DELETE = 'DELETE'

class Headers(object):
    AUTHORIZATION = "Authorization"
    CACHE_CONTROL = "Cache-Control"
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
    RANGE = "Range"
    LOCATION = "Location"
    TRANSFER_ENCODING = "Transfer-Encoding"
    CHUNKED = "chunked"
    ACCEPT_ENCODING = "Accept-Encoding"
    USER_AGENT = "User-Agent"

class CommonResponseResult(object):
    __slots__ = ('_status_code', '_request_id', '_error_code', '_error_msg')

    def __init__(self, resp=None):
        if None != resp:
            try:
                self._status_code = resp.status_code
                self._request_id = resp.headers['x-datahub-request-id']
                content = json.loads(resp.content)
                self._error_code = content['ErrorCode']
                self._error_msg = content['ErrorMessage']
            except Exception, e:
                raise e

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

    def parse(resp):
        try:
            self._status_code = resp.status_code
            self._request_id = resp.headers['x-datahub-request-id']
            content = json.loads(resp.content)
            self._error_code = content['ErrorCode']
            self._error_msg = content['ErrorMessage']
        except Exception, e:
            raise e

class RestModel(object):
    __slots__ = ('_name', '_comment', '_create_time', '_last_modify_time')

    def __init__(self, *args, **kwds):
        self._name = kwds['name'] if 'name' in kwds else ''
        self._comment = kwds['comment'] if 'comment' in kwds else ''
        self._create_time = kwds['create_time'] if 'create_time' in kwds else 0
        self._last_modify_time = kwds['last_modify_time'] if 'last_modify_time' in kwds else 0

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def comment(self):
        return self._comment

    @comment.setter
    def comment(self, value):
        self._comment = value

    @property
    def create_time(self):
        return self._create_time

    @property
    def last_modify_time(self):
        return self._last_modify_time

    def __str__(self):
        json_item = {
            "name": "%s" % self._name,
            "comment": "%s" % self._comment,
            "create_time": self._create_time,
            "last_modify_time": self._last_modify_time
        }
        return json.dumps(json_item)

    def __hash__(self):
        return hash((type(self), self._name, self._comment))

    def __eq__(self, other):
        if other is None:
            return False

        if not isinstance(other, type(self)):
            return False

        return self._name == other._name

    def throw_exception(self, response_result):
        raise NotImplementedError

    def resource(self):
        raise NotImplementedError

    def encode(self, method):
        raise NotImplementedError

    def decode(self, method, resp):
        raise NotImplementedError

def default_user_agent():
    os_version = platform.platform()
    py_version = platform.python_version()
    ua_template = Template('$pydatahub_version $python_version $os_version')
    return ua_template.safe_substitute(pydatahub_version='pydatahub/%s' % __version__,
                                       python_version='python/%s' % py_version,
                                       os_version='%s' % os_version)

class RestClient(object):
    """Restful client enhanced by URL building and request signing facilities.
    """
    def __init__(self, account, endpoint, user_agent = None, proxies = None, stream=False, retry_times=3, conn_timeout=5, read_timeout=120):
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
        self._session.headers.update({Headers.ACCEPT_ENCODING:''})

        # mount adapters with retry times
        adapter = requests.adapters.HTTPAdapter(max_retries=self._retry_times)
        self._session.mount('http://', adapter)
        self._session.mount('https://', adapter)

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

    def is_ok(self, resp):
        return resp.ok

    def request(self, method, restmodel, **kwargs):
        url = "%s%s" %(self._endpoint, restmodel.resource())
        Logger.logger.debug('get a request, method: %s, url: %s' %(method, url))

        # Construct user agent without handling the letter case.
        headers = kwargs.get('headers', {})
        headers = dict((k, str(v)) for k, v in six.iteritems(headers))
        headers['x-datahub-client-version'] = __datahub_client_version__
        headers[Headers.USER_AGENT] = self._user_agent
        headers[Headers.CONTENT_TYPE] = 'application/json'
        headers[Headers.DATE] = gen_rfc822_date()
        kwargs['headers'] = headers
        req = requests.Request(method, url, **kwargs)
        prepared_req = self._session.prepare_request(req)
        self._account.sign_request(prepared_req, self._endpoint)
        Logger.logger.debug('full request url: %s\nrequest headers:\n%s\nrequest body:\n%s' %(prepared_req.url, prepared_req.headers, prepared_req.body))

        resp = self._session.send(prepared_req, 
                stream=self._stream,
                timeout=(self._conn_timeout, self._read_timeout),
                verify=False)

        Logger.logger.debug('response.status_code: %d' % resp.status_code)
        Logger.logger.debug('response.headers: \n%s' % resp.headers)
        if not self._stream:
            Logger.logger.debug('response.content: %s\n' % (resp.content))
        # Automatically detect error
        if not self.is_ok(resp):
            resp_res = CommonResponseResult(resp)
            restmodel.throw_exception(resp_res)
        restmodel.decode(method, resp)

    def get(self, restmodel, **kwargs):
        kwargs.update(restmodel.encode(HTTPMethod.GET))
        self.request(HTTPMethod.GET, restmodel, **kwargs)

    def post(self, restmodel, **kwargs):
        kwargs.update(restmodel.encode(HTTPMethod.POST))
        self.request(HTTPMethod.POST, restmodel, **kwargs)

    def put(self, restmodel, **kwargs):
        kwargs.update(restmodel.encode(HTTPMethod.PUT))
        self.request(HTTPMethod.PUT, restmodel, **kwargs)

    def head(self, restmodel, **kwargs):
        kwargs.update(restmodel.encode(HTTPMethod.HEAD))
        self.request(HTTPMethod.HEAD, restmodel, **kwargs)

    def delete(self, restmodel, **kwargs):
        kwargs.update(restmodel.encode(HTTPMethod.DELETE))
        self.request(HTTPMethod.DELETE, restmodel, **kwargs)
