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

"""Protobuf serialization, replaces cprotobuf.internal.encode_data."""

from datahub.proto.datahub_pb2 import (
    PutRecordsRequest,
    PutBinaryRecordsRequest,
    GetRecordsRequest,
    RecordEntry,
    RecordData,
    FieldData,
    RecordAttributes,
    StringPair,
    BinaryRecordEntry,
)


def _build_record_entry(msg, d):
    if 'shard_id' in d:
        msg.shard_id = d['shard_id']
    if 'hash_key' in d:
        msg.hash_key = d['hash_key']
    if 'partition_key' in d:
        msg.partition_key = d['partition_key']
    if 'cursor' in d:
        msg.cursor = d['cursor']
    if 'next_cursor' in d:
        msg.next_cursor = d['next_cursor']
    if 'sequence' in d:
        msg.sequence = d['sequence']
    if 'system_time' in d:
        msg.system_time = d['system_time']
    if 'attributes' in d:
        _build_record_attributes(msg.attributes, d['attributes'])
    if 'data' in d:
        _build_record_data(msg.data, d['data'])


def _build_record_data(msg, d):
    if 'data' in d:
        for fd_dict in d['data']:
            fd = msg.data.add()
            if 'value' in fd_dict:
                val = fd_dict['value']
                if isinstance(val, (list, tuple)):
                    fd.value = b''.join(val)
                else:
                    fd.value = val


def _build_record_attributes(msg, d):
    if 'attributes' in d:
        for sp_dict in d['attributes']:
            sp = msg.attributes.add()
            if 'key' in sp_dict:
                sp.key = sp_dict['key']
            if 'value' in sp_dict:
                sp.value = sp_dict['value']


def _build_binary_record_entry(msg, d):
    if 'cursor' in d:
        msg.cursor = d['cursor']
    if 'next_cursor' in d:
        msg.next_cursor = d['next_cursor']
    if 'sequence' in d:
        msg.sequence = d['sequence']
    if 'system_time' in d:
        msg.system_time = d['system_time']
    if 'serial' in d:
        msg.serial = d['serial']
    if 'data' in d:
        msg.data = d['data']


def encode_proto(proto_class, d):
    """Serialize a dict to protobuf bytes, replacing cprotobuf encode_data."""
    if proto_class == GetRecordsRequest:
        msg = GetRecordsRequest()
        if 'cursor' in d:
            msg.cursor = d['cursor']
        if 'limit' in d:
            msg.limit = d['limit']
        return msg.SerializeToString()

    elif proto_class == PutRecordsRequest:
        msg = PutRecordsRequest()
        if 'records' in d:
            for rec_dict in d['records']:
                entry = msg.records.add()
                _build_record_entry(entry, rec_dict)
        return msg.SerializeToString()

    elif proto_class == PutBinaryRecordsRequest:
        msg = PutBinaryRecordsRequest()
        if 'records' in d:
            for rec_dict in d['records']:
                entry = msg.records.add()
                _build_binary_record_entry(entry, rec_dict)
        return msg.SerializeToString()

    else:
        raise ValueError('Unsupported proto class: %s' % proto_class)
