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

import re
import abc
import base64
from enum import Enum

import six

from . import types as _types
from .schema import RecordSchema, FieldType
from ..exceptions import InvalidParameterException
from ..utils import ErrorMessage, indent, to_str, bool_to_str, to_binary


class RecordType(Enum):
    """
    Record type, there are two type: ``TUPLE`` and ``BLOB``
    """
    BLOB = 'BLOB'
    TUPLE = 'TUPLE'


@six.add_metaclass(abc.ABCMeta)
class Record(object):
    """
    Base Record class
    """
    __slots__ = ('_values', '_shard_id', '_hash_key', '_partition_key', '_attributes', '_sequence', '_system_time',
                 '_record_key', '_batch_size', '_batch_index')

    encode = 0

    def __init__(self):
        self._values = None
        self._shard_id = ''
        self._hash_key = ''
        self._partition_key = ''
        self._attributes = dict()
        self._sequence = 0
        self._system_time = 0
        self._record_key = None
        self._batch_size = 0
        self._batch_index = 0

    @property
    def values(self):
        return self._values

    @property
    def shard_id(self):
        return self._shard_id

    @shard_id.setter
    def shard_id(self, value):
        self._shard_id = value

    @property
    def hash_key(self):
        return self._hash_key

    @hash_key.setter
    def hash_key(self, value):
        self._hash_key = value

    @property
    def partition_key(self):
        return self._partition_key

    @partition_key.setter
    def partition_key(self, value):
        self._partition_key = value

    @property
    def attributes(self):
        return self._attributes

    @attributes.setter
    def attributes(self, value):
        self._attributes = value

    @property
    def sequence(self):
        return self._sequence

    @sequence.setter
    def sequence(self, value):
        self._sequence = value

    @property
    def system_time(self):
        return self._system_time

    @system_time.setter
    def system_time(self, value):
        self._system_time = value

    @property
    def record_key(self):
        return self._record_key

    @record_key.setter
    def record_key(self, value):
        self._record_key = value

    @property
    def batch_size(self):
        return self._batch_size

    @batch_size.setter
    def batch_size(self, value):
        self._batch_size = value

    @property
    def batch_index(self):
        return self._batch_index

    @batch_index.setter
    def batch_index(self, value):
        self._batch_index = value

    def get_attribute(self, key):
        if key not in self._attributes:
            return None
        return self._attributes[key]

    def put_attribute(self, key, value):
        if key is None or value is None:
            raise InvalidParameterException("key/value can not be None")
        self._attributes[key] = value

    def get_offset(self):
        return self._sequence, self._system_time

    @abc.abstractmethod
    def get_type(self):
        pass

    @abc.abstractmethod
    def encode_values(self):
        pass

    @abc.abstractmethod
    def decode_values(self):
        pass

    @abc.abstractmethod
    def encode_pb_record_data(self):
        pass

    def to_json(self):
        data = {
            "Data": self.encode_values(),
            "Sequence": self._sequence,
            "SystemTime": self._system_time,
            "BatchIndex": self._batch_index
        }
        if self._partition_key:
            data["PartitionKey"] = self._partition_key
        if self._hash_key:
            data["HashKey"] = self._hash_key
        if self._shard_id:
            data["ShardId"] = self._shard_id
        if self._attributes:
            data["Attributes"] = self._attributes
        return data

    def to_pb_record_entry(self):
        pb_record_entry = {
            'data': self.encode_pb_record_data()
        }
        if self._partition_key:
            pb_record_entry['partition_key'] = self._partition_key
        if self._hash_key:
            pb_record_entry['hash_key'] = self._hash_key
        if self._shard_id:
            pb_record_entry['shard_id'] = self._shard_id
        if self._attributes:
            pb_record_entry['attributes'] = {
                'attributes': [{
                    'key': k,
                    'value': v
                } for k, v in self._attributes.items()]
            }
        return pb_record_entry

    def __repr__(self):
        return to_str(self.to_json())


class BlobRecord(Record):
    """
    Blob type record class

    Members:
        blob_data (:class:`str`): blob data
    """

    def __init__(self, blob_data=None, values=None):
        super(BlobRecord, self).__init__()
        if blob_data:
            self._blob_data = to_binary(blob_data)
        elif values is not None:
            self._values = values
            self._blob_data = base64.b64decode(self._values)
        else:
            raise InvalidParameterException(ErrorMessage.MISSING_BLOB_RECORD_DATA)

    @property
    def blob_data(self):
        return self._blob_data

    def get_type(self):
        return RecordType.BLOB

    def encode_values(self):
        if not self._values:
            self._values = bytes.decode(base64.b64encode(self._blob_data))
        return self._values

    def decode_values(self):
        return self._blob_data

    def encode_pb_record_data(self):
        return {
            'data': [{'value': self._blob_data}]
        }


class TupleRecord(Record):
    """
    Tuple type record class

    Members:
        field_list (:class:`list`): fields

        name_indices (:class:`list`): values

    :Example:

    >>> schema = RecordSchema.from_lists(['name', 'id'], [FieldType.STRING, FieldType.STRING])
    >>> record = TupleRecord(schema=schema, values=['test', 'test2'])
    >>> record.values[0:2]
    >>> ['test', 'test2']
    >>> record.set_value('name', 'test1')
    >>> record.values[0]
    >>> 'test1'
    >>> record.set_value(0, 'test3')
    >>> record.get_value(0)
    >>> 'test3'
    >>> record.get_value('name')
    >>> 'test3'
    >>> len(record.values)
    2
    >>> record.has_field('name')
    True
    """

    __slots__ = ('_field_list', '_name_indices')

    def __init__(self, field_list=None, schema=None, values=None):
        super(TupleRecord, self).__init__()
        self._field_list = field_list
        if schema is not None:
            self._field_list = schema.field_list
        if self._field_list is None or len(self._field_list) == 0:
            raise InvalidParameterException(ErrorMessage.MISSING_TUPLE_RECORD_SCHEMA)

        self._values = [None, ] * len(self._field_list)
        if values is not None:
            if len(values) != len(self._field_list):
                raise InvalidParameterException('The values set to records are against the schema, '
                                                'expect len %s, got len %s' % (len(self._field_list), len(values)))
            self._set_values(values)

        self._name_indices = dict((field.name, index) for index, field in enumerate(self._field_list))

    @property
    def field_list(self):
        return self._field_list

    @field_list.setter
    def field_list(self, value):
        self._field_list = value

    @property
    def values(self):
        return tuple(self._values)

    @values.setter
    def values(self, value):
        if len(value) != len(self._field_list):
            raise InvalidParameterException('The values set to records are against the schema, '
                                            'expect len %s, got len %s' % (len(self._field_list), len(value)))
        self._set_values(value)

    @property
    def name_indices(self):
        return self._name_indices

    @name_indices.setter
    def name_indices(self, value):
        self._name_indices = value

    def set_value(self, index_or_name, value):
        if isinstance(index_or_name, six.integer_types):
            self._set_value_by_index(index_or_name, value)
        else:
            self._set_value_by_name(to_str(index_or_name), value)

    def get_value(self, index_or_name):
        if isinstance(index_or_name, six.integer_types):
            return self._get_value_by_index(index_or_name)
        return self._get_value_by_name(to_str(index_or_name))

    def has_field(self, field_name):
        return field_name in self._name_indices

    def get_type(self):
        return RecordType.TUPLE

    def encode_values(self):
        new_values = []
        index = 0
        for val in self._values:
            if FieldType.BOOLEAN == self._field_list[index].type:
                new_values.append(bool_to_str(val))
            elif FieldType.DOUBLE == self._field_list[index].type:
                double_str = re.sub(r'0+e', 'e', '%.16e' % val) if val is not None else None
                new_values.append(to_str(double_str))
            else:
                new_values.append(to_str(val))
            index += 1
        return new_values

    def decode_values(self):
        pass

    def encode_pb_record_data(self):
        pb_record_data = {
            'data': []
        }
        index = 0
        for val in self._values:
            if FieldType.BOOLEAN == self._field_list[index].type:
                pb_record_data['data'].append({'value': to_binary(bool_to_str(val))})
            else:
                pb_record_data['data'].append({'value': to_binary(val)})
            index += 1
        return pb_record_data

    def _set_values(self, values):
        for index, value in enumerate(values):
            if index >= len(self._field_list):
                break
            self._set_value_by_index(index, value)

    def _set_value_by_index(self, index, value):
        field = self._field_list[index]
        if not field.allow_null and value is None:
            raise InvalidParameterException('Filed with index %d can not be none' % index)
        val = _types.validate_value(value, field)
        self._values[index] = val

    def _set_value_by_name(self, name, value):
        self._set_value_by_index(self._name_indices[name], value)

    def _get_value_by_index(self, index):
        if not 0 <= index < len(self._values):
            raise InvalidParameterException('Index %d out of range' % index)
        return self._values[index]

    def _get_value_by_name(self, name):
        if name not in self._name_indices:
            raise InvalidParameterException('Field name %s does not exists' % name)
        return self.values[self._name_indices[name]]

    def __repr__(self):
        buf = six.StringIO()

        name_space = 2 * max(len(field.name) for field in self._field_list) if self._field_list else 0
        type_space = 2 * max(len(field.type.value) for field in self._field_list) if self._field_list else 0
        value_space = 2 * max(len(to_str(value if value is not None else "None")) for value in self._values) if self._values else 0

        buf.write('TupleRecord {\n')
        buf.write('  Values {\n')

        field_strs = ['    {0}{1}{2}'.format(
            '*name*'.ljust(name_space),
            '*type*'.ljust(type_space),
            '*value*'.ljust(value_space)
        )]
        for index, field in enumerate(self._field_list):
            field_strs.append('    {0}{1}{2}'.format(
                field.name.ljust(name_space),
                field.type.value.ljust(type_space),
                to_str(self._values[index] if self._values[index] is not None else "None").ljust(value_space)
            ))
        buf.write(indent('\n'.join(field_strs), 2))
        buf.write('\n')
        buf.write('  }\n')

        if self._attributes:
            attribute_key_space = 2 * max(len(to_str(key)) for key in self._attributes) if self._attributes else 0
            attribute_value_space = 2 * max(len(to_str(self._attributes[key])) for key in self._attributes) if self._attributes else 0

            buf.write('    Attributes {\n')
            field_strs = ['    {0}{1}'.format(
                '*key*'.ljust(attribute_key_space),
                '*value*'.ljust(attribute_value_space)
            )]
            for key in self._attributes:
                field_strs.append('    {0}{1}'.format(
                    to_str(key).ljust(attribute_key_space),
                    to_str(self._attributes[key]).ljust(attribute_value_space)
                ))
            buf.write(indent('\n'.join(field_strs), 2))
            buf.write('\n')
            buf.write('  }\n')

        buf.write('}\n')

        return buf.getvalue()


class FailedRecord(object):
    """
    Failed record info

    Members:
        comment (:class:`str`): subscription description

        create_time (:class:`int`): create time

        is_owner (:class:`bool`): owner or not

        last_modify_time (:class:`int`): last modify time

        state (:class:`datahub.models.SubscriptionState`): subscription state

        sub_id (:class:`str`): subscription id

        topic_name (:class:`str`): topic name

        type (:class:`int`): type
    """

    __slots__ = ('_index', '_error_code', '_error_message')

    def __init__(self, index, error_code, error_message):
        self._index = index
        self._error_code = error_code
        self._error_message = error_message

    @property
    def index(self):
        return self._index

    @index.setter
    def index(self, value):
        self._index = value

    @property
    def error_code(self):
        return self._error_code

    @error_code.setter
    def error_code(self, value):
        self._error_code = value

    @property
    def error_message(self):
        return self._error_message

    @error_message.setter
    def error_message(self, value):
        self._error_message = value

    def to_json(self):
        return {
            'Index': self._index,
            'ErrorCode': self._error_code,
            'ErrorMessage': self._error_message
        }

    @classmethod
    def from_pb_message(cls, failed_record_pb):
        index = failed_record_pb.index
        error_code = failed_record_pb.error_code
        error_message = failed_record_pb.error_message
        return cls(index, error_code, error_message)

    def __repr__(self):
        return to_str(self.to_json())
