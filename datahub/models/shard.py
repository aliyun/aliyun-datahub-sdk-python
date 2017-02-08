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


class ShardState(Enum):
    """
    Shard state, there are: ``OPENING``, ``ACTIVE``, ``CLOSED``, ``CLOSING``
    """
    OPENING = 'OPENING'
    ACTIVE = 'ACTIVE'
    CLOSED = 'CLOSED'
    CLOSING = 'CLOSING'


class ShardBase(object):
    """
    Shard base info

    Members:
        shard_id (:class:`str`): shard id

        begin_hash_key (:class:`str`): begin hash key

        end_hash_key (:class:`str`): end hash key
    """

    __slots__ = ('_shard_id', '_begin_hash_key', '_end_hash_key')

    def __init__(self, shard_id, begin_hash_key, end_hash_key):
        self._shard_id = shard_id
        self._begin_hash_key = begin_hash_key
        self._end_hash_key = end_hash_key

    @property
    def shard_id(self):
        return self._shard_id

    @shard_id.setter
    def shard_id(self, value):
        self._shard_id = value

    @property
    def begin_hash_key(self):
        return self._begin_hash_key

    @begin_hash_key.setter
    def begin_hash_key(self, value):
        self._begin_hash_key = value

    @property
    def end_hash_key(self):
        return self._end_hash_key

    @end_hash_key.setter
    def end_hash_key(self, value):
        self._end_hash_key = value

    @classmethod
    def from_dict(cls, dict_):
        return cls(dict_.get('ShardId', ''), dict_.get('BeginHashKey', ''), dict_.get('EndHashKey', ''))

    def to_json(self):
        return {
            'ShardId': self._shard_id,
            'BeginHashKey': self._begin_hash_key,
            'EndHashKey': self._end_hash_key
        }

    def __repr__(self):
        return to_str(self.to_json())


class ShardContext(object):
    """
    Shard context

    Members:
        shard_id (:class:`str`): shard id

        start_sequence (:class:`str`): start sequence

        end_sequence (:class:`str`): end sequence

        current_sequence (:class:`str`): current sequence
    """
    __slots__ = ('_shard_id', '_start_sequence', '_end_sequence', '_current_sequence')

    def __init__(self, shard_id, start_sequence, end_sequence, current_sequence):
        self._shard_id = shard_id
        self._start_sequence = start_sequence
        self._end_sequence = end_sequence
        self._current_sequence = current_sequence

    @property
    def shard_id(self):
        return self._shard_id

    @shard_id.setter
    def shard_id(self, value):
        self._shard_id = value

    @property
    def start_sequence(self):
        return self._start_sequence

    @start_sequence.setter
    def start_sequence(self, value):
        self._start_sequence = value

    @property
    def end_sequence(self):
        return self._end_sequence

    @end_sequence.setter
    def end_sequence(self, value):
        self._end_sequence = value

    @property
    def current_sequence(self):
        return self._current_sequence

    @current_sequence.setter
    def current_sequence(self, value):
        self._current_sequence = value

    def to_json(self):
        return {
            'ShardId': self._shard_id,
            'StartSequence': self._start_sequence,
            'EndSequence': self._end_sequence,
            'CurrentSequence': self.current_sequence
        }

    @classmethod
    def from_dict(cls, dict_):
        return cls(dict_.get('ShardId', ''), dict_.get('StartSequence', -1),
                   dict_.get('EndSequence', -1), dict_.get('CurrentSequence', -1))

    def __repr__(self):
        return to_str(self.to_json())


class Shard(ShardBase):
    """
    Shard info

    Members:
        shard_id (:class:`str`): shard id

        begin_hash_key (:class:`str`): begin hash key

        end_hash_key (:class:`str`): end hash key

        state (:class:`datahub.models.ShardState`): shard state

        closed_time (:class:`str`): closed time

        parent_shard_ids (list): parent shard ids

        left_shard_id (:class:`str`): left shard id

        right_shard_id (:class:`str`): right shard id
    """
    __slots__ = (
        '_shard_id', '_state', '_closed_time', '_begin_hash_key', '_end_hash_key', '_parent_shard_ids',
        '_left_shard_id', '_right_shard_id')

    def __init__(self, shard_id, begin_hash_key, end_hash_key, state, closed_time, parent_shard_ids, left_shard_id,
                 right_shard_id):
        super(Shard, self).__init__(shard_id, begin_hash_key, end_hash_key)
        self._state = state
        self._closed_time = closed_time
        self._parent_shard_ids = parent_shard_ids
        self._left_shard_id = left_shard_id
        self._right_shard_id = right_shard_id

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state = value

    @property
    def closed_time(self):
        return self._closed_time

    @closed_time.setter
    def closed_time(self, value):
        self._closed_time = value

    @property
    def parent_shard_ids(self):
        return self._parent_shard_ids

    @parent_shard_ids.setter
    def parent_shard_ids(self, value):
        self._parent_shard_ids = value

    @property
    def left_shard_id(self):
        return self._left_shard_id

    @left_shard_id.setter
    def left_shard_id(self, value):
        self._left_shard_id = value

    @property
    def right_shard_id(self):
        return self._right_shard_id

    @right_shard_id.setter
    def right_shard_id(self, value):
        self._right_shard_id = value

    def to_json(self):
        return {
            "ShardId": self._shard_id,
            "State": self._state.value,
            "ClosedTime": self._closed_time,
            "BeginHashKey": self._begin_hash_key,
            "EndHashKey": self._end_hash_key,
            "ParentShardIds": self._parent_shard_ids,
            "LeftShardId": self._left_shard_id,
            "RightShardId": self._right_shard_id
        }

    @classmethod
    def from_dict(cls, dict_):
        return cls(dict_.get('ShardId', ''), dict_.get('BeginHashKey', ''), dict_.get('EndHashKey', ''),
                   ShardState(dict_.get('State', '')), dict_.get('ClosedTime', ''), dict_.get('ParentShardIds', []),
                   dict_.get('LeftShardId', ''), dict_.get('RightShardId', ''))

    def __repr__(self):
        return to_str(self.to_json())
