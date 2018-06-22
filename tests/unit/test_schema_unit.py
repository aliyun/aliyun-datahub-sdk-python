#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# 'License'); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from datahub.exceptions import InvalidParameterException
from datahub.models import RecordSchema, FieldType, Field


class TestSchema:

    def test_build_schema_success(self):
        record_schema_0 = RecordSchema.from_lists(
            ['bigint_field', 'string_field', 'double_field', 'bool_field', 'event_time1'],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP],
            [False, True, True, True, True]
        )

        record_schema_1 = RecordSchema([])
        record_schema_1.add_field(Field('bigint_field', FieldType.BIGINT, False))
        record_schema_1.add_field(Field('string_field', FieldType.STRING))
        record_schema_1.add_field(Field('double_field', FieldType.DOUBLE))
        record_schema_1.add_field(Field('bool_field', FieldType.BOOLEAN))
        record_schema_1.add_field(Field('event_time1', FieldType.TIMESTAMP))

        fields = []
        fields.append(Field('bigint_field', FieldType.BIGINT, False))
        fields.append(Field('string_field', FieldType.STRING))
        fields.append(Field('double_field', FieldType.DOUBLE))
        fields.append(Field('bool_field', FieldType.BOOLEAN))
        fields.append(Field('event_time1', FieldType.TIMESTAMP))

        record_schema_2 = RecordSchema(fields)

        for index in range(0, len(record_schema_0.field_list)):
            assert record_schema_0.field_list[index].name == record_schema_1.field_list[index].name
            assert record_schema_0.field_list[index].type == record_schema_1.field_list[index].type
            assert record_schema_0.field_list[index].allow_null == record_schema_1.field_list[index].allow_null

            assert record_schema_0.field_list[index].name == record_schema_2.field_list[index].name
            assert record_schema_0.field_list[index].type == record_schema_2.field_list[index].type
            assert record_schema_0.field_list[index].allow_null == record_schema_2.field_list[index].allow_null

    def test_build_schema_with_invalid_type(self):
        try:
            record_schema_0 = RecordSchema.from_lists(
                ['bigint_field', 'string_field', 'double_field', 'bool_field', 'event_time1'],
                ['int', FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])
        except InvalidParameterException:
            pass
        else:
            raise Exception('build schema success with wrong filed type!')

        try:
            record_schema_1 = RecordSchema()
            record_schema_1.add_field(Field('string_field', 'str'))
        except InvalidParameterException:
            pass
        else:
            raise Exception('build schema success with wrong filed type!')

        try:
            fields = []
            fields.append(Field('bigint_field', FieldType.BIGINT))
            fields.append(Field('string_field', FieldType.STRING))
            fields.append(Field('double_field', FieldType.DOUBLE))
            fields.append(Field('bool_field', FieldType.BOOLEAN))
            fields.append(Field('event_time1', 'time'))
        except InvalidParameterException:
            pass
        else:
            raise Exception('build schema success with wrong filed type!')


if __name__ == '__main__':
    test = TestSchema()
    test.test_build_schema_success()
    test.test_build_schema_with_invalid_type()
