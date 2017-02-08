#!/usr/bin/env python
# -*- coding: UTF-8 -*-
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
import logging
import logging.handlers
from .path import Path

class Singleton(object):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_inst'):
            cls._inst = super(Singleton, cls).__new__(cls, *args, **kwargs)
        return cls._inst

class Logger(Singleton):

    LOG_LEVEL = {"DEBUG": logging.DEBUG, "INFO": logging.INFO, "WARNING": logging.WARNING, "ERROR": logging.ERROR, "CRITICAL": logging.CRITICAL}

    loglevel = os.environ.get('PYDATAHUB_LOGLEVEL')

    logger = logging.getLogger('pydatahub')
    logger.setLevel(LOG_LEVEL[loglevel] if loglevel in LOG_LEVEL else logging.ERROR)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(name)s: %(message)s'))
    logger.addHandler(handler)

    @classmethod
    def init(cls, name, path=None, level=logging.DEBUG):
        _path = Path('.') if path is None else path
        if not isinstance(_path, Path):
            _path = Path(_path)
        log_file = _path / '%s.log' % name
        Logger.logger = logging.getLogger(name)
        Logger.logger.setLevel(level)
        _rtfhandler = logging.handlers.RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=10)
        _rtfhandler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] [%(pathname)s:%(lineno)d] %(message)s'))
        Logger.logger.addHandler(_rtfhandler)

