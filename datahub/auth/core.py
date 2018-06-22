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


class AccountType(object):
    """
    Account type.

    Only Support 'aliyun' type now.
    """
    ALIYUN = 'aliyun'


class Account(object):
    """
    Base Account Class.

    .. seealso:: :class:`datahub.auth.AliyunAccount`
    """

    def __init__(self, *args, **kwargs):
        pass

    def get_type(self):
        """
        Get account type, subclass must be provided.

        :return: the account type
        :rtype: :class:`datahub.auth.AccountType`
        """
        raise NotImplementedError("subclass must provide getType method")

    def sign_request(self, request):
        """
        Generator signature for request, subclass must be provided.

        :param request: request object
        :return: none
        """
        raise NotImplementedError("subclass must provide getType method")
