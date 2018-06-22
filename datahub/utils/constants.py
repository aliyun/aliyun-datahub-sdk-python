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


class ErrorMessage(object):
    INVALID_PROJECT_NAME = 'project name should start with letter, only contains [a-zA-Z0-9_], 3 < length < 32'
    INVALID_TOPIC_NAME = 'topic name should start with letter, only contains [a-zA-Z0-9_], 1 < length < 128'
    MISSING_RECORD_SCHEMA = 'missing record schema for tuple record type'
    INVALID_RECORD_SCHEMA_TYPE = 'record schema parameter must be type of RecordSchema'
    MISSING_BLOB_RECORD_DATA = 'Blob Record blob data or values is missing'
    MISSING_TUPLE_RECORD_SCHEMA = 'TUPLE Record fields or schema is missing'
    MISSING_SYSTEM_TIME = 'get SYSTEM_TIME cursor must provide invalid system_time parameter'
    MISSING_SEQUENCE = 'get SEQUENCE cursor must provide invalid sequence parameter'
    WAIT_SHARD_TIMEOUT = 'wait shards ready timeout'
    PARAMETER_EMPTY = '%s could not be empty'
    PARAMETER_NOT_POSITIVE = '%s must be positive'
    PARAMETER_NEGATIVE = '%s could not be negative'
    INVALID_TYPE = '%s must be type of %s'
