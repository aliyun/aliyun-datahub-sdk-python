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

from enum import Enum

from ..utils import to_str


class SubscriptionState(Enum):
    """
    Subscription state, there are: ``INACTIVE``, ``ACTIVE``
    """
    INACTIVE = 0
    ACTIVE = 1


class Subscription(object):
    """
    subscription info

    Members:
        comment (:class:`str`): subscription description

        create_time (:class:`int`): create time

        is_owner (:class:`bool`): owner or not

        last_modify_time (:class:`int`): last modify time

        state (:class:`datahub.models.SubscriptionState`): subscription state

        sub_id (:class:`str`): subscription id

        topic_name (:class:`str`): topic name

        type (:class:`int`): type
    """
    __slots__ = ('_comment', '_create_time', '_is_owner', '_last_modify_time',
                 '_state', '_sub_id', '_topic_name', '_type')

    def __init__(self, comment, create_time, is_owner, last_modify_time,
                 state, sub_id, topic_name, sub_type):
        self._comment = comment
        self._create_time = create_time
        self._is_owner = is_owner
        self._last_modify_time = last_modify_time
        self._state = state
        self._sub_id = sub_id
        self._topic_name = topic_name
        self._type = sub_type

    @property
    def comment(self):
        return self._comment

    @comment.setter
    def comment(self, value):
        self._comment = value

    @property
    def create_time(self):
        return self._create_time

    @create_time.setter
    def create_time(self, value):
        self._create_time = value

    @property
    def is_owner(self):
        return self._is_owner

    @is_owner.setter
    def is_owner(self, value):
        self._is_owner = value

    @property
    def last_modify_time(self):
        return self._last_modify_time

    @last_modify_time.setter
    def last_modify_time(self, value):
        self._last_modify_time = value

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state = value

    @property
    def sub_id(self):
        return self._sub_id

    @sub_id.setter
    def sub_id(self, value):
        self._sub_id = value

    @property
    def topic_name(self):
        return self._topic_name

    @topic_name.setter
    def topic_name(self, value):
        self._topic_name = value

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        self._type = value

    @classmethod
    def from_dict(cls, dict_):
        return cls(dict_['Comment'], dict_['CreateTime'], dict_['IsOwner'], dict_['LastModifyTime'],
                   SubscriptionState(dict_['State']), dict_['SubId'], dict_['TopicName'], dict_['Type'])

    def to_json(self):
        return {
            'Comment': self._comment,
            'CreateTime': self._create_time,
            'IsOwner': self._is_owner,
            'LastModifyTime': self._last_modify_time,
            'State': self._state.value,
            'SubId': self._sub_id,
            'TopicName': self._topic_name,
            'Type': self._type
        }

    def __repr__(self):
        return to_str(self.to_json())


class OffsetBase(object):
    """
    offset base class

    Members:
        sequence (:class:`int`): sequence

        timestamp (:class:`int`): timestamp
    """
    __slots__ = ('_sequence', '_timestamp')

    def __init__(self, sequence, timestamp):
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

    @classmethod
    def from_dict(cls, dict_):
        return cls(dict_['Sequence'], dict_['Timestamp'])

    def to_json(self):
        return {
            "Sequence": self._sequence,
            "Timestamp": self._timestamp
        }

    def __repr__(self):
        return to_str(self.to_json())


class OffsetWithVersion(OffsetBase):
    """
    offset with version class

    Members:
        sequence (:class:`int`): sequence

        timestamp (:class:`int`): timestamp

        version (:class:`int`): version
    """
    __slots__ = ('_sequence', '_timestamp', '_version')

    def __init__(self, sequence, timestamp, version):
        super(OffsetWithVersion, self).__init__(sequence, timestamp)
        self._version = version

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, value):
        self._version = value

    @classmethod
    def from_dict(cls, dict_):
        return cls(dict_['Sequence'], dict_['Timestamp'], dict_['Version'])

    def to_json(self):
        return {
            "Sequence": self._sequence,
            "Timestamp": self._timestamp,
            "Version": self._version
        }


class OffsetWithSession(OffsetWithVersion):
    """
    offset with session class

    Members:
        sequence (:class:`int`): sequence

        timestamp (:class:`int`): timestamp

        version (:class:`int`): version

        session_id (:class:`int`): session id
    """
    __slots__ = ('_sequence', '_timestamp', '_version', '_session_id')

    def __init__(self, sequence, timestamp, version, session_id):
        super(OffsetWithSession, self).__init__(sequence, timestamp, version)
        self._session_id = session_id

    @property
    def session_id(self):
        return self._session_id

    @session_id.setter
    def session_id(self, value):
        self._session_id = value

    @classmethod
    def from_dict(cls, dict_):
        return cls(dict_['Sequence'], dict_['Timestamp'], dict_['Version'], dict_['SessionId'])

    def to_json(self):
        return {
            "Sequence": self._sequence,
            "Timestamp": self._timestamp,
            "Version": self._version,
            "SessionId": self._session_id
        }
