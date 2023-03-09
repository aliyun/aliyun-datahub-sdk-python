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


class DatahubException(Exception):
    """
    There was an base exception class that occurred while handling your request to datahub server.
    """

    def __init__(self, error_msg, status_code=-1, request_id=None, error_code=None):
        super(DatahubException, self).__init__(error_msg)
        self.status_code = status_code
        self.request_id = request_id
        self.error_code = error_code
        self.error_msg = error_msg

    def __str__(self):
        return "status_code: %d, request_id: %s, error_code: %s, error_msg: %s" \
               % (self.status_code, self.request_id, self.error_code, self.error_msg)

    # A long list of server defined exceptions


class ResourceExistException(DatahubException):
    """
    The exception is raised while Datahub Object that you are creating is already exist.
    """

    def __init__(self, error_msg, status_code=-1, request_id=None, error_code=None):
        super(ResourceExistException, self).__init__(error_msg, status_code, request_id, error_code)


class ResourceNotFoundException(DatahubException):
    """
    The exception is raised while Datahub Object that you are handling is not exist.
    """

    def __init__(self, error_msg, status_code=-1, request_id=None, error_code=None):
        super(ResourceNotFoundException, self).__init__(error_msg, status_code, request_id, error_code)


class InvalidParameterException(DatahubException):
    """
    The exception is raised while that your handling request parameter is invalid.
    """

    def __init__(self, error_msg, status_code=-1, request_id=None, error_code=None):
        super(InvalidParameterException, self).__init__(error_msg, status_code, request_id, error_code)


class InvalidOperationException(DatahubException):
    """
    The operation of shard is not support yet.
    """

    def __init__(self, error_msg, status_code=-1, request_id=None, error_code=None):
        super(InvalidOperationException, self).__init__(error_msg, status_code, request_id, error_code)


class OffsetResetException(InvalidOperationException):
    """
    The offset is reset.
    """

    def __init__(self, error_msg, status_code=-1, request_id=None, error_code=None):
        super(OffsetResetException, self).__init__(error_msg, status_code, request_id, error_code)


class LimitExceededException(DatahubException):
    """
    Too many request.
    """

    def __init__(self, error_msg, status_code=-1, request_id=None, error_code=None):
        super(LimitExceededException, self).__init__(error_msg, status_code, request_id, error_code)


class InternalServerException(DatahubException):
    """
    The Datahub server occurred error.
    """

    def __init__(self, error_msg, status_code=-1, request_id=None, error_code=None):
        super(InternalServerException, self).__init__(error_msg, status_code, request_id, error_code)


class AuthorizationFailedException(DatahubException):
    """
    The authorization failed error.
    """

    def __init__(self, error_msg, status_code=-1, request_id=None, error_code=None):
        super(AuthorizationFailedException, self).__init__(error_msg, status_code, request_id, error_code)


class NoPermissionException(DatahubException):
    """
    The operation without permission.
    """

    def __init__(self, error_msg, status_code=-1, request_id=None, error_code=None):
        super(NoPermissionException, self).__init__(error_msg, status_code, request_id, error_code)


class ShardSealedException(DatahubException):
    """
    The Shard has been sealed.
    """

    def __init__(self, error_msg, status_code=-1, request_id=None, error_code=None):
        super(ShardSealedException, self).__init__(error_msg, status_code, request_id, error_code)


class InvalidCursorException(DatahubException):
    """
    The cursor is invalid.
    """

    def __init__(self, error_msg, status_code=-1, request_id=None, error_code=None):
        super(InvalidCursorException, self).__init__(error_msg, status_code, request_id, error_code)


class SubscriptionOfflineException(DatahubException):
    """
    The cursor is invalid.
    """

    def __init__(self, error_msg, status_code=-1, request_id=None, error_code=None):
        super(SubscriptionOfflineException, self).__init__(error_msg, status_code, request_id, error_code)


class TimeoutException(DatahubException):
    """
    Timeout.
    """

    def __init__(self, error_msg, status_code=-1, request_id=None, error_code=None):
        super(TimeoutException, self).__init__(error_msg, status_code, request_id, error_code)


class ExceptionHandler(object):
    """
    The handler to throw exception according to error code
    """
    error_code_dict = {
        'InvalidUriSpec': InvalidParameterException,
        'InvalidParameter': InvalidParameterException,
        'InvalidSchema': InvalidParameterException,
        'InvalidCursor': InvalidCursorException,
        'InvalidSubscription': InvalidParameterException,
        'NoSuchProject': ResourceNotFoundException,
        'NoSuchTopic': ResourceNotFoundException,
        'NoSuchShard': ResourceNotFoundException,
        'NoSuchConnector': ResourceNotFoundException,
        'ProjectAlreadyExist': ResourceExistException,
        'TopicAlreadyExist': ResourceExistException,
        'ConnectorAlreadyExist': ResourceExistException,
        'InvalidShardOperation': ShardSealedException,
        'InvalidOperation': InvalidOperationException,
        'OperationDenied': InvalidOperationException,
        'LimitExceeded': LimitExceededException,
        'MalformedRecord': InvalidParameterException,
        'InternalServerError': InternalServerException,
        'Unauthorized': AuthorizationFailedException,
        'NoPermission': NoPermissionException,
        'SubscriptionOffline': SubscriptionOfflineException,
        'OffsetReseted': OffsetResetException,
        'OffsetSessionClosed': InvalidOperationException,
        'OffsetSessionChanged': InvalidOperationException,
        'ListSubscriptionOutofRange': InvalidParameterException,
    }

    def raise_exception(self, error_msg, status_code, request_id, error_code):
        raise self.error_code_dict.get(error_code, DatahubException)(error_msg, status_code, request_id, error_code)


exception_handler = ExceptionHandler()
