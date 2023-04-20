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

import json
from enum import Enum

import six

from .. import utils
from ..exceptions import InvalidParameterException
from ..utils import ErrorMessage, check_type, to_str


class FieldType(Enum):
    """
    Field Types, datahub support 5 types of field, there are:
        ``TINYINT``, ``SMALLINT``, ``INTEGER``, ``BIGINT``, ``STRING``,
        ``BOOLEAN``, ``TIMESTAMP``, ``FLOAT``, ``DOUBLE``, ``DECIMAL``
    """
    TINYINT = 'tinyint'
    SMALLINT = 'smallint'
    INTEGER = 'integer'
    BIGINT = 'bigint'
    STRING = 'string'
    BOOLEAN = 'boolean'
    TIMESTAMP = 'timestamp'
    FLOAT = 'float'
    DOUBLE = 'double'
    DECIMAL = 'decimal'


class Field(object):
    """
    Field

    Members:
        name (:class:`str`): field name

        type (:class:`str`): field type

        comment (:class:`str`): field comment
    """

    __slots__ = ('_name', '_type', '_comment', '_allow_null')

    def __init__(self, name, field_type, comment="", allow_null=True):
        if not check_type(field_type, FieldType):
            raise InvalidParameterException(ErrorMessage.INVALID_TYPE % ('field_type', FieldType.__name__))
        self._name = utils.to_str(name)
        self._type = field_type
        self._comment = comment
        self._allow_null = allow_null

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        self._type = value

    @property
    def allow_null(self):
        return self._allow_null

    @allow_null.setter
    def allow_null(self, value):
        self._allow_null = value

    @property
    def comment(self):
        return self._comment

    @comment.setter
    def comment(self, value):
        self._comment = value

    def to_json(self):
        data = {
            "name": self._name,
            "type": self._type.value,
            "comment": self._comment
        }
        if not self._allow_null:
            data["notnull"] = not self._allow_null
        return data

    @classmethod
    def from_json(cls, field_json):
        allow_null = not field_json['notnull'] if 'notnull' in field_json else True
        comment = field_json['comment'] if 'comment' in field_json else ""
        return cls(field_json['name'], FieldType(field_json['type'].lower()), comment, allow_null)

    def __repr__(self):
        return '<field {0}, type {1}, comment {2}, allow_null {3}>'.format(self._name, self._type.name.lower(), self._comment, self._allow_null)


class RecordSchema(object):
    """
    Record schema class, Tuple type Topic will use it.

    Members:
        fields (list): fields

    :Example:

    >>> schema = RecordSchema.from_lists( \
        ['bigint_field'  , 'string_field'  , 'double_field'  , 'bool_field'     , 'time_field'], \
        [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP] \
    )
    >>>

    .. seealso:: :class:`datahub.models.FieldType`
    """

    def __init__(self, field_list=None):
        self._field_list = field_list if field_list else []
        self._field_dict = {}

        duplicates = set()
        for field in self._field_list:
            if field.name in self._field_dict:
                duplicates.add(field.name)
            else:
                self._field_dict[field.name] = field

        if duplicates:
            raise InvalidParameterException('Duplicate field names: %s' % ', '.join(duplicates))

    @property
    def field_list(self):
        return self._field_list

    def add_field(self, field):
        if field.name not in self._field_dict:
            self._field_list.append(field)
            self._field_dict[field.name] = field
        else:
            raise InvalidParameterException('Field name %s already exists' % field.name)

    def get_field(self, index_or_name):
        if isinstance(index_or_name, six.integer_types):
            return self._get_field_by_index(index_or_name)
        return self._get_field_by_name(utils.to_str(index_or_name))

    def _get_field_by_index(self, index):
        if not 0 <= index < len(self._field_list):
            raise InvalidParameterException('Index %d out of range' % index)
        return self._field_list[index]

    def _get_field_by_name(self, name):
        if name not in self._field_dict:
            raise InvalidParameterException('Field name %s does not exists' % name)
        return self._field_dict[name]

    def to_json(self):
        return {
            'fields': [field.to_json() for field in self._field_list]
        }

    def to_json_string(self):
        return json.dumps(self.to_json())

    @classmethod
    def from_lists(cls, names, types, comments=None, allow_nulls=None):
        if len(names) != len(types) or (comments and len(comments) != len(names)) or (allow_nulls and len(allow_nulls) != len(names)):
            raise InvalidParameterException('Length of lists are not equal')
        field_list = []
        for index in range(0, len(names)):
            allow_null = allow_nulls[index] if allow_nulls else True
            comment = comments[index] if comments else ""
            field_list.append(Field(names[index], types[index], comment, allow_null))
        return cls(field_list=field_list)

    @classmethod
    def from_json(cls, fields_json):
        field_list = []
        for field in fields_json['fields']:
            field_list.append(Field.from_json(field))
        return cls(field_list=field_list)

    @classmethod
    def from_json_str(cls, fields_jsonstr):
        return cls.from_json(json.loads(fields_jsonstr))

    def __repr__(self):
        buf = six.StringIO()

        name_space = 2 * max(len(field.name) for field in self._field_list) if self._field_list else 0
        type_space = 2 * max(len(field.type.value) for field in self._field_list) if self._field_list else 0
        allow_null_space = 8

        buf.write('RecordSchema {\n')
        field_strs = []
        for field in self._field_list:
            field_strs.append('{0}{1}{2}'.format(
                field.name.ljust(name_space),
                field.type.value.ljust(type_space),
                to_str(field.allow_null).ljust(allow_null_space)
            ))
        buf.write(utils.indent('\n'.join(field_strs), 2))
        buf.write('\n')
        buf.write('}\n')

        return buf.getvalue()
