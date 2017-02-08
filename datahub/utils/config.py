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

import ConfigParser

class Configer(object):
    def __init__(self, config_file):
        self.__config_parser = ConfigParser.ConfigParser()
        self.__config_parser.read(config_file)

    def get(self, section, key, default_value = ''):
        return self.__config_parser.has_option(section, key) and self.__config_parser.get(section, key) or default_value

    def items(self, section):
        return self.__config_parser.items(section)

    def options(self, section):
        return self.__config_parser.options(section)

    def sections(self):
        return self.__config_parser.sections()
