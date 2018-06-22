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
import struct
import zlib
from enum import Enum

import lz4.block
import six

from ..exceptions import DatahubException


class CompressFormat(Enum):
    """
    CompressFormat enum class, there are: ``NONE``, ``LZ4``, ``ZLIB``, ``DEFLATE``
    """
    NONE = ''
    LZ4 = 'lz4'
    ZLIB = 'zlib'
    DEFLATE = 'deflate'


@six.add_metaclass(abc.ABCMeta)
class Compressor(object):
    """
    Abstract Compressor class
    """

    @abc.abstractmethod
    def compress(self, data):
        pass

    @abc.abstractmethod
    def decompress(self, data, raw_size=-1):
        pass

    @abc.abstractmethod
    def compress_format(self):
        pass


class Lz4Compressor(Compressor):
    """
    Lz4 compressor
    """

    def compress(self, data):
        return lz4.block.compress(data, store_size=False)

    def decompress(self, data, raw_size=-1):
        size_header = struct.pack('<I', raw_size)
        return lz4.block.decompress(size_header + data)

    def compress_format(self):
        return CompressFormat.LZ4


class ZlibCompressor(Compressor):
    """
    Zlib compressor
    """

    def compress(self, data):
        return zlib.compress(data)

    def decompress(self, data, raw_size=-1):
        return zlib.decompress(data)

    def compress_format(self):
        return CompressFormat.ZLIB


class DeflateCompressor(Compressor):
    """
    Deflate compressor
    """

    def compress(self, data):
        return zlib.compress(data)

    def decompress(self, data, raw_size=-1):
        return data

    def compress_format(self):
        return CompressFormat.DEFLATE


lz4_compressor = Lz4Compressor()
zlib_compressor = ZlibCompressor()
deflate_compressor = DeflateCompressor()

_compressor_dict = {
    CompressFormat.NONE: None,
    CompressFormat.LZ4: lz4_compressor,
    CompressFormat.ZLIB: zlib_compressor,
    CompressFormat.DEFLATE: deflate_compressor
}


def get_compressor(compress_format):
    try:
        compress_format = CompressFormat(compress_format)
    except ValueError as e:
        raise DatahubException(e)
    return _compressor_dict.get(compress_format, None)
