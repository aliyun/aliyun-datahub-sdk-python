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

import argparse
import time

from datahub import DataHub
from datahub.models import CursorType


class Timer(object):
    def __init__(self, verbose=False):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        self.msecs = self.secs * 1000  # millisecs
        if self.verbose:
            print('elapsed time: %f ms' % self.msecs)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('access_id', help='account access id')
    parser.add_argument('access_key', help='account access key')
    parser.add_argument('endpoint', help='datahub server endpoint')
    parser.add_argument('--batch', help='batch record num', type=int, default=100)
    parser.add_argument('--round', help='round num', type=int, default=10000)
    parser.add_argument('--project', help='project name', default='py_perf_test_project')
    parser.add_argument('--topic', help='topic name', default='py_perf_test_topic')
    parser.add_argument('--retry_times', help='request retry nums', type=int, default=3)
    parser.add_argument('--conn_timeout', help='connect timeout', type=int, default=5)
    parser.add_argument('--read_timeout', help='read timeout', type=int, default=120)
    parser.add_argument('--stream', help='read timeout', action="store_true")
    parser.add_argument('--protobuf', help='protobuf mode', type=bool, default=False)
    args = parser.parse_args()
    print("=============configuration=============")
    print("access_id:%s" % args.access_id)
    print("access_key:%s" % args.access_key)
    print("endpoint:%s" % args.endpoint)
    print("project:%s" % args.project)
    print("topic:%s" % args.topic)
    print("retry_times:%d" % args.retry_times)
    print("conn_timeout:%d" % args.conn_timeout)
    print("read_timeout:%d" % args.read_timeout)
    print("batch record num:%d" % args.batch)
    print("round num:%d" % args.round)
    print("stream:%s" % args.stream)
    print("protobuf:%s" % args.protobuf)
    print("=======================================\n\n")

    dh = DataHub(args.access_id, args.access_key, args.endpoint, retry_times=args.retry_times,
                 conn_timeout=args.conn_timeout, read_timeout=args.read_timeout)
    # project = Project(name=args.project, comment='perf project for python sdk')
    # dh.create_project(project)
    # print "create project %s success!" % args.project
    # print "=======================================\n\n"

    topic_result = dh.get_topic(args.project, args.topic)
    print("get topic %s success! detail:\n%s" % (args.topic, str(topic_result)))
    print("=======================================\n\n")

    cursor_result = dh.get_cursor(args.project, args.topic, '0', CursorType.OLDEST)
    print("get topic %s oldest cursor success! detail:\n%s" % (args.topic, cursor_result.cursor))
    print("=======================================\n\n")

    read_request_count = 0
    read_suc_reord_count = 0
    cursor = cursor_result.cursor

    with Timer() as t:
        for i in range(0, args.round):
            record_result = dh.get_tuple_records(topic_result.project_name, topic_result.topic_name, '0',
                                                 topic_result.record_schema, cursor, args.batch)
            read_request_count += 1
            read_suc_reord_count += record_result.record_count
            if record_result.record_count == 0:
                break
            cursor = record_result.next_cursor

    print("===============result==================")
    print("read_request_count:%d, %f/s" % (read_request_count, (1000.0 * read_request_count) / t.msecs))
    print("read_suc_reord_count:%d, %f/s" % (read_suc_reord_count, (1000.0 * read_suc_reord_count) / t.msecs))
    print("=> elapsed time: %fs" % t.secs)

