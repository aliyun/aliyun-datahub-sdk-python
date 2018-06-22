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

from datahub import DataHub, RestClient
from datahub.exceptions import ResourceExistException
from datahub.implement import DataHubJson
from datahub.models import RecordSchema, TupleRecord, RecordType, Field, FieldType, Result, RequestParams


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
    parser.add_argument('--file', help='record file')
    parser.add_argument('--batch', help='batch record num', type=int, default=100)
    parser.add_argument('--round', help='round num', type=int, default=10000)
    parser.add_argument('--project', help='project name', default='py_perf_test_project')
    parser.add_argument('--topic', help='topic name', default='py_perf_test_topic')
    parser.add_argument('--retry_times', help='request retry nums', type=int, default=3)
    parser.add_argument('--conn_timeout', help='connect timeout', type=int, default=5)
    parser.add_argument('--read_timeout', help='read timeout', type=int, default=120)
    parser.add_argument('--stream', help='read timeout', action="store_true")
    parser.add_argument('--protobuf', help='protobuf mode', type=bool, default=False)
    parser.add_argument('--size', help='record size', type=int, default=10)
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

    dh = DataHub(args.access_id, args.access_key, args.endpoint, stream=args.stream, retry_times=args.retry_times,
                 conn_timeout=args.conn_timeout, read_timeout=args.read_timeout)
    # project = Project(name=args.project, comment='perf project for python sdk')
    # dh.create_project(project)
    # print "create project %s success!" % args.project
    # print "=======================================\n\n"

    data = 'a' * args.size
    if args.file:
        with open(args.file, 'r') as f:
            data = f.read()

    record_schema = RecordSchema()
    #    record_schema.add_field(Field('bigint_field', FieldType.BIGINT))
    record_schema.add_field(Field('string_field', FieldType.STRING))
    #    record_schema.add_field(Field('double_field', FieldType.DOUBLE))
    #    record_schema.add_field(Field('bool_field', FieldType.BOOLEAN))
    #    record_schema.add_field(Field('time_field', FieldType.TIMESTAMP))

    try:
        dh.create_project(args.project, 'perf test')
    except ResourceExistException as e:
        print("project %s already exist!" % args.project)
        print("=======================================\n\n")

    try:
        dh.create_tuple_topic(args.project, args.topic, 1, 7, record_schema, 'perf test')
    except ResourceExistException as e:
        print("topic %s already exist!" % args.topic)
        print("=======================================\n\n")

    write_request_count = 0
    write_suc_reord_count = 0
    write_fail_record_count = 0

    with Timer() as t:
        for i in range(0, args.round):
            records = []
            for j in range(0, args.batch):
                record = TupleRecord(schema=record_schema)
                #                record['bigint_field'] = 2
                record.set_value('string_field', data)
                #                record['double_field'] = 10.02
                #                record['bool_field'] = False
                #                record['time_field'] = 1455869335000011
                record.shard_id = '0'
                records.append(record)
            put_result = dh.put_records(args.project, args.topic, records)

            write_request_count += 1
            write_suc_reord_count += (len(records) - put_result.failed_record_count)
            write_fail_record_count += put_result.failed_record_count
            # print "%d put %d records, fail %d" %(write_request_count, len(records), len(fail_records))

    print("===============result==================")
    print("write_request_count:%d, %f/s" % (write_request_count, (1000.0 * write_request_count) / t.msecs))
    print("write_suc_reord_count:%d, %f/s" % (write_suc_reord_count, (1000.0 * write_suc_reord_count) / t.msecs))
    print("write_fail_record_count:%d" % write_fail_record_count)
    print("=> elasped time: %fms" % t.msecs)

