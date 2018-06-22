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

from collections import OrderedDict

import six
from six.moves.urllib.parse import urlparse, unquote

from .core import Account, AccountType
from ..rest import Headers
from ..utils import hmac_sha1

try:
    from urllib.parse import parse_qsl
except ImportError:
    from urlparse import parse_qsl

# ============================ log ============================
import logging

logger = logging.getLogger('datahub.account')
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


class AliyunAccount(Account):
    """
    Aliyun account implement base from :class:`datahub.auth.Account`
    """

    __slots__ = '_access_id', '_access_key'

    def __init__(self, *args, **kwargs):
        super(AliyunAccount, self).__init__(*args, **kwargs)
        self._access_id = kwargs.get('access_id', '')
        self._access_key = kwargs.get('access_key', '')

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

    def get_type(self):
        """
        Get account type.

        :return: the account type
        :rtype: :class:`datahub.auth.AccountType`
        """
        return AccountType.ALIYUN

    @staticmethod
    def _build_canonical_str(url_components, req):
        # Build signing string
        lines = [req.method, req.headers[Headers.CONTENT_TYPE], req.headers[Headers.DATE], ]

        headers_to_sign = dict()

        # req headers
        headers = req.headers
        for k, v in six.iteritems(headers):
            k = k.lower()
            if k.startswith('x-datahub-'):
                headers_to_sign[k] = v

        # url params
        if url_components.query:
            params_list = sorted(parse_qsl(url_components.query, True),
                                 key=lambda it: it[0])
            params = dict(params_list)
            for k, v in params.items():
                if k.startswith('x-datahub-'):
                    headers_to_sign[k] = v

        headers_to_sign = OrderedDict([(k, headers_to_sign[k])
                                       for k in sorted(headers_to_sign)])
        logger.debug('headers to sign: %s' % headers_to_sign)

        for k, v in six.iteritems(headers_to_sign):
            lines.append('%s:%s' % (k, v))

        lines.append(url_components.path)
        return '\n'.join(lines)

    def sign_request(self, request):
        """
        Generator signature for request.

        :param request: request object
        :return: none
        """
        url = request.path_url
        url_components = urlparse(unquote(url))
        canonical_str = self._build_canonical_str(url_components, request)
        logger.debug('canonical string: ' + canonical_str)

        sign = hmac_sha1(self._access_key, canonical_str)

        auth_str = 'DATAHUB %s:%s' % (self._access_id, sign)
        request.headers[Headers.AUTHORIZATION] = auth_str
