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

from __future__ import absolute_import, print_function

import hmac
import struct
from base64 import b64encode
from hashlib import sha1

import crcmod.predefined
import six

from .converters import to_binary, to_str


def hmac_sha1(secret, data):
    try:
        key_bytes = bytes(secret, 'latin-1')
        data_bytes = bytes(data, 'latin-1')
    except TypeError:
        return b64encode(hmac.new(secret, data, sha1).digest())
    return b64encode(hmac.new(key_bytes, data_bytes, sha1).digest())


def pb_message_wrap(pb_data):
    crc32c = crcmod.predefined.mkCrcFun('crc-32c')
    crc = crc32c(to_binary(pb_data)) & 0xffffffff
    return to_binary('DHUB') + struct.pack('>I', crc) + struct.pack('>I', len(pb_data)) + pb_data


def unwrap_pb_frame(pb_frame):
    crc32c = crcmod.predefined.mkCrcFun('crc-32c')
    binary = to_binary(pb_frame)
    crc = binary[4:8]
    pb_str = pb_frame[12:] if six.PY3 else to_str(pb_frame[12:])
    compute_crc = struct.pack('>I', crc32c(pb_str) & 0xffffffff)
    return crc, compute_crc, pb_str
