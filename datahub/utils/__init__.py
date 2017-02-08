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
import re
import sys
import time
import traceback
import types
import warnings
import datetime
from base64 import b64encode
from hashlib import sha1

from ..thirdparty import six
from .log import Logger
from .path import Path
from .config import Configer

def deprecated(msg, cond=None):
    def _decorator(func):
        """This is a decorator which can be used to mark functions
        as deprecated. It will result in a warning being emmitted
        when the function is used."""
        def _new_func(*args, **kwargs):
            warn_msg = "Call to deprecated function %s." % func.__name__
            if isinstance(msg, six.string_types):
                warn_msg += ' ' + msg
            if cond is None or cond():
                warnings.warn(warn_msg, category=DeprecationWarning)
            return func(*args, **kwargs)
        _new_func.__name__ = func.__name__
        _new_func.__doc__ = func.__doc__
        _new_func.__dict__.update(func.__dict__)
        return _new_func

    if isinstance(msg, (types.FunctionType, types.MethodType)):
        return _decorator(msg)
    return _decorator

def benchmark(func):
    """
    装饰器打印一个函数的执行时间
    """
    def wrapper(*args, **kwargs):
        s = time.time()
        res = func(*args, **kwargs)
        e = time.time()
        Logger.logger.info("{0} cost {1}ms".format(func.__name__, (e-s)*1000))
        return res
    return wrapper

def counter(func):
    """
    记录并打印一个函数的执行次数
    """
    def wrapper(*args, **kwargs):
        wrapper.count = wrapper.count + 1
        res = func(*args, **kwargs)
        Logger.logger.info("{0} has been used: {1}x".format(func.__name__, wrapper.count))
        return res
    wrapper.count = 0
    return wrapper

def hmac_sha1(secret, data):
    return b64encode(hmac.new(secret, data, sha1).digest())

def md5_hexdigest(data):
    return md5(data).hexdigest()

def rshift(val, n):
    return val >> n if val >= 0  else \
        (val+0x100000000) >> n

def camel_to_underline(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def underline_to_capitalized(name):
    return "".join([s[0].upper() + s[1:len(s)] for s in name.strip('_').split('_')])

def underline_to_camel(name):
    parts = name.split('_')
    return parts[0] + ''.join(v.title() for v in parts[1:])

def long_to_int(value):
    if value & 0x80000000:
        return int(-((value ^ 0xFFFFFFFF) + 1))
    else:
        return int(value)

def int_to_uint(v):
    if v < 0:
        return int(v + 2**32)
    return v

def long_to_uint(value):
    v = long_to_int(value)
    return int_to_uint(v)

def stringify_expt():
    lines = traceback.format_exception(*sys.exc_info())
    return '\n'.join(lines)

def indent(text, n_spaces):
    if n_spaces <= 0:
        return text
    block = ' ' * n_spaces
    return '\n'.join((block + it) if len(it) > 0 else it
                     for it in text.split('\n'))

def gen_rfc822_date():
    GMT_FORMAT = '%a, %d %b %Y %H:%M:%S GMT'
    datestr = datetime.datetime.utcnow().strftime(GMT_FORMAT)
    return datestr

def to_timestamp(dt):
    return int(time.mktime(dt.timetuple()))

def to_milliseconds(dt):
    return int((time.mktime(dt.timetuple()) + dt.microsecond/1000000.0) * 1000)

def to_datetime(milliseconds):
    seconds = int(milliseconds / 1000)
    microseconds = milliseconds % 1000 * 1000
    return datetime.datetime.fromtimestamp(seconds).replace(microsecond=microseconds)

def to_binary(text, encoding='utf-8'):
    if text is None:
        return text
    if isinstance(text, six.text_type):
        return text.encode(encoding)
    elif isinstance(text, (six.binary_type, bytearray)):
        return bytes(text)
    else:
        return str(text).encode(encoding) if six.PY3 else str(text)

def to_text(binary, encoding='utf-8'):
    if binary is None:
        return binary
    if isinstance(binary, (six.binary_type, bytearray)):
        return binary.decode(encoding)
    elif isinstance(binary, six.text_type):
        return binary
    else:
        return str(binary) if six.PY3 else str(binary).decode(encoding)

def to_str(text, encoding='utf-8'):
    return to_text(text, encoding=encoding) if six.PY3 else to_binary(text, encoding=encoding)

def is_lambda(f):
    lam = lambda: 0
    return isinstance(f, type(lam)) and f.__name__ == lam.__name__

def str_to_kv(str, typ=None):
    d = dict()
    for pair in str.split(','):
        k, v = pair.split(':', 1)
        if typ:
            v = typ(v)
        d[k] = v
    return d

def is_namedtuple(obj):
    return isinstance(obj, tuple) and hasattr(obj, '_fields')

def str_to_bool(s):
    if isinstance(s, bool):
        return s
    s = s.lower().strip()
    if s == 'true':
        return True
    elif s == 'false':
        return False
    else:
        raise ValueError

def bool_to_str(s):
    return str(s).lower()

