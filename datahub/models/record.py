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
import base64

from ..thirdparty import six
from .rest import HTTPMethod, RestModel
from . import types as _types
from .. import utils
from .. import errors

class FieldType(object):
    """
    Field Types, datahub support 5 types of field, there are: ``BIGINT``, ``STRING``, ``BOOLEAN``, ``TIMESTAMP``, ``DOUBLE``
    """
    BIGINT = 'bigint'
    STRING = 'string'
    BOOLEAN = 'boolean'
    TIMESTAMP = 'timestamp'
    DOUBLE = 'double'

class Field(object):
    """
    Field class
    """
    def __init__(self, name=None, typo=None):
        self.name = utils.to_str(name)
        self.type = _types.validate_data_type(typo)

    def __repr__(self):
        return '<field {0}, type {1}>'.format(self.name, self.type.name.lower())

    def __hash__(self):
        return hash((type(self), self.name, self.type))

class Schema(object):
    """
    Base Schema class

    .. seealso:: :class:`datahub.models.RecordSchema`
    """
    def __init__(self, names, types):
        self._init(names, types)

    def _init(self, names, types):
        if not isinstance(names, list):
            names = list(names)
        self.names = names
        self.types = [_types.validate_data_type(t) for t in types]

        self._name_indexes = dict((n, i) for i, n in enumerate(self.names))

        if len(self._name_indexes) < len(self.names):
            duplicates = [n for n in self._name_indexes if self.names.count(n) > 1]
            raise ValueError('Duplicate field names: %s' % ', '.join(duplicates))

    def __len__(self):
        return len(self.names)

    def __contains__(self, name):
        return utils.to_str(name) in self._name_indexes

    def __hash__(self):
        return hash((type(self), tuple(self.names), tuple(self.types)))

    def __eq__(self, other):
        if not isinstance(other, Schema):
            return False
        return self.names == other.names and self.types == self.types

    def get_type(self, name):
        return self.types[self._name_indexes[utils.to_str(name)]]

class RecordSchema(Schema):
    """
    Record schema class, Tuple type Topic will use it.

    :Example:

    >>> schema = RecordSchema.from_lists(['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'], [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])
    >>>

    .. seealso:: :class:`datahub.models.FieldType`
    """
    def __init__(self, fields=None):
        self._fields = fields

        if self._fields:
            super(RecordSchema, self).__init__(*zip(*[(f.name, f.type) for f in self._fields]))
        else:
            self._fields = []
            super(RecordSchema, self).__init__([], [])

    def __len__(self):
        return super(RecordSchema, self).__len__()

    def __contains__(self, name):
        return super(RecordSchema, self).__contains__(name)

    def __eq__(self, other):
        if not isinstance(other, RecordSchema):
            return False

        return super(RecordSchema, self).__eq__(other)

    def __hash__(self):
        return hash((type(self), tuple(self.names), tuple(self.types)))

    def __getitem__(self, item):
        if isinstance(item, six.integer_types):
            n_fields = len(self._name_indexes)
            if item < n_fields:
                return self._fields[item]
            else:
                raise IndexError('Index out of range')
        elif isinstance(item, six.string_types):
            item = utils.to_str(item)
            if item in self._name_indexes:
                idx = self._name_indexes[item]
                return self[idx]
            else:
                raise ValueError('Unknown field name: %s' % item)
        elif isinstance(item, (list, tuple)):
            return [self[it] for it in item]
        else:
            raise ValueError('Invalid argument')

    @property
    def fields(self):
        return self._fields

    def get_fields(self):
        return self._fields

    def add_field(self, field):
        self._fields.append(field)
        super(RecordSchema, self).__init__(*zip(*[(f.name, f.type) for f in self._fields]))

    def get_field(self, name):
        index = self._name_indexes.get(utils.to_str(name))
        if index is None:
            raise ValueError('Field %s does not exists' % name)
        return self._fields[index]

    def get_type(self, name):
        if name in self._name_indexes:
            return super(RecordSchema, self).get_type(name)
        raise ValueError('Field does not exist: %s' % name)

    def update(self, fields):
        self._fields = fields

        names = map(lambda f: f.name, self._fields)
        types = map(lambda f: f.type, self._fields)

        self._init(names, types)

    def to_ignorecase_schema(self):
        flds = [Field(f.name.lower(), f.type) for f in self._fields]
        return type(self)(fields=flds)

    def to_json_string(self):
        fields = {}
        names = map(lambda f: f.name, self._fields)
        types = map(lambda f: f.type, self._fields)
        fields['fields'] = [{"name":"%s" % name, "type":"%s" % str(typo)} for name, typo in zip(names, types)] 
        return json.dumps(fields)

    def __str__(self):
        buf = six.StringIO()

        name_space = 2 * max(len(field.name) for field in self._fields)
        type_space = 2 * max(len(repr(field.type)) for field in self._fields)

        buf.write('RecordSchema {\n')
        field_strs = []
        for field in self._fields:
            field_strs.append('{0}{1}'.format(
                (field.name).ljust(name_space),
                repr(field.type).ljust(type_space)
            ))
        buf.write(utils.indent('\n'.join(field_strs), 2))
        buf.write('\n')
        buf.write('}\n')

        return buf.getvalue()

    @classmethod
    def from_lists(cls, names, types):
        fields = [Field(name=name, typo=typo) for name, typo in zip(names, types)]
        return cls(fields=fields)

    @classmethod
    def from_dict(cls, fields_dict):
        lkeys = lambda x: x.keys()
        lvalues = lambda x: x.values()
        if six.PY3:
            lkeys = lambda x: list(x.keys())
            lvalues = lambda x: list(x.values())
        fields = lkeys(fields_dict)
        fields_types = lvalues(fields_dict)
        return cls.from_lists(fields, fields_types)

    @classmethod
    def from_json(cls, fileds_json):
        fields = [Field(name=f['name'], typo=f['type']) for f in fileds_json['fields']]
        return cls(fields=fields)

    @classmethod
    def from_jsonstring(cls, fields_jsonstr):
        return cls.from_json(json.loads(fields_jsonstr))

class RecordType(object):
    """
    Record type, there are two type: ``Tuple`` and ``Blob``
    """
    BLOB = 'BLOB'
    TUPLE = 'TUPLE'

class Record(object):
    """
    Base Record class
    """
    __slots__ = ('_values', '_shard_id', '_hash_key', '_partition_key', '_attributes')

    def __init__(self):
        self._values = None
        self._shard_id = ''
        self._hash_key = ''
        self._partition_key = ''
        self._attributes = dict()

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
        return partition_key

    @partition_key.setter
    def partition_key(self, value):
        self._partition_key = value

    @property
    def attributes(self):
        return self._attributes

    def get_attribute(self, key):
        if not key in self._attributes:
            return None
        return self._attributes[key]

    def put_attribute(self, key, value):
        self._attributes[key] = value

    def get_type(self):
        raise NotImplementedError

    def encode_values(self):
        raise NotImplementedError

    def decode_values(self):
        raise NotImplementedError

    def to_json(self):
        return {
            "Data": self.encode_values(),
            "ShardId": self._shard_id,
            "HashKey": self._hash_key,
            "PartitionKey": self._partition_key,
            "Attributes": self._attributes
        }

    def __str__(self):
        return json.dumps(self.to_json())

class BlobRecord(Record):
    """
    Blob type record class
    """

    __slots__ = ('_blobdata')

    def __init__(self, blobdata=None, values=None):
        super(BlobRecord, self).__init__()
        if blobdata:
            self._blobdata = blobdata
            self._values = base64.b64encode(utils.to_str(self._blobdata))
        elif values:
            self._values = values
            self._blobdata = base64.b64decode(self._values)
        else:
            raise ValueError('Blob Record blobdata or values should not be provided')

    @property
    def blobdata(self):
        return self._blobdata

    def get_type(self):
        return RecordType.BLOB

    def encode_values(self):
        return self._values

    def decode_values(self):
        return self._blobdata

class TupleRecord(Record):
    """
    Tuple type record class

    :Example:

    >>> schema = RecordSchema.from_lists(['name', 'id'], ['string', 'string'])
    >>> record = TupleRecord(schema=schema, values=['test', 'test2'])
    >>> record[0] = 'test'
    >>> record[0]
    >>> 'test'
    >>> record['name']
    >>> 'test'
    >>> record[0:2]
    >>> ('test', 'test2')
    >>> record[0, 1]
    >>> ('test', 'test2')
    >>> record['name', 'id']
    >>> for field in record:
    >>>     print(field)
    ('name', u'test')
    ('id', u'test2')
    >>> len(record)
    2
    >>> 'name' in record
    True
    """

    __slots__ = ('_fields', '_name_indexes')

    def __init__(self, fields=None, schema=None, values=None):
        super(TupleRecord, self).__init__()
        self._fields = fields or schema.fields
        if self._fields is None:
            raise ValueError('TUPLE Record fields or schema should not be provided')

        self._values = [None, ] * len(self._fields)
        if values is not None:
            self._sets(values)
        self._name_indexes = dict((col.name, i) for i, col in enumerate(self._fields))

    def _get(self, i):
        return self._values[i]

    def _set(self, i, value):
        data_type = self._fields[i].type
        val = _types.validate_value(value, data_type)
        self._values[i] = val

    def _sets(self, values):
        if len(values) != len(self._fields):
            raise ValueError('The values set to records are against the schema, '
                             'expect len %s, got len %s' % (len(self._fields), len(values)))
        [self._set(i, value) for i, value in enumerate(values)]

    def __getitem__(self, item):
        if isinstance(item, six.string_types):
            return getattr(self, item)
        elif isinstance(item, (list, tuple)):
            return [self[it] for it in item]
        return self._values[item]

    def __setitem__(self, key, value):
        if isinstance(key, six.string_types):
            setattr(self, key, value)
        else:
            self._set(key, value)

    def __getattr__(self, item):
        if item == '_name_indexes':
            return object.__getattribute__(self, item)
        if hasattr(self, '_name_indexes') and item in self._name_indexes:
            i = self._name_indexes[item]
            return self._values[i]
        return object.__getattribute__(self, item)

    def __setattr__(self, key, value):
        if hasattr(self, '_name_indexes') and key in self._name_indexes:
            i = self._name_indexes[key]
            self._set(i, value)
        else:
            object.__setattr__(self, key, value)

    def get_by_name(self, name):
        return getattr(self, name)

    def set_by_name(self, name, value):
        return setattr(self, name, value)

    def __len__(self):
        return len(self._fields)

    def __contains__(self, item):
        return item in self._name_indexes

    def __iter__(self):
        for i, col in enumerate(self._fields):
            yield (col.name, self[i])

    def __hash__(self):
        return hash((type(self), tuple(self._fields), tuple(self._values)))

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False

        return self._values == other._values

    def get_type(self):
        return RecordType.TUPLE

    def encode_values(self):
        new_values = []
        for val in self._values:
            new_values.append(utils.to_str(val).lower())
        return new_values

    def decode_values(self):
        pass


class Records(RestModel):
    """
    Records class, will be used by get_records interface
    """
    __slots__ = ('_project_name', '_topic_name', '_schema', '_shard_id', '_next_cursor', '_limit_num', '_record_list', '_failed_indexs')

    def __init__(self, *args, **kwds):
        super(Records, self).__init__(*args, **kwds)
        self._project_name = kwds['project_name'] if 'project_name' in kwds else ''
        self._topic_name = kwds['topic_name'] if 'topic_name' in kwds else ''
        self._schema = kwds['schema'] if 'schema' in kwds and isinstance(kwds['schema'], RecordSchema) else None
        self._shard_id = ''
        self._next_cursor = ''
        self._limit_num = 1
        self._record_list = []

    @property
    def project_name(self):
        return self._project_name

    @project_name.setter
    def project_name(self, value):
        self._project_name = value

    @property
    def topic_name(self):
        return self._topic_name

    @topic_name.setter
    def topic_name(self, value):
        self._topic_name = value

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        if not isinstance(value, RecordSchema):
            raise ValueError('argument must RecordSchema')
        self._schema = value

    @property
    def shard_id(self):
        return self._shard_id

    @shard_id.setter
    def shard_id(self, value):
        self._shard_id = value

    @property
    def record_list(self):
        return self._record_list

    @record_list.setter
    def record_list(self, value):
        if not isinstance(value, list):
            raise ValueError('value assigned to record list must List')
        self._record_list = value

    @property
    def next_cursor(self):
        return self._next_cursor

    @next_cursor.setter
    def next_cursor(self, value):
        self._next_cursor = value

    @property
    def limit_num(self):
        return self._limit_num

    @limit_num.setter
    def limit_num(self, value):
        self._limit_num = value

    def __len__(self):
        return len(self._record_list)

    @property
    def record_num(self):
        return len(self._record_list)

    @property
    def failed_num(self):
        return len(self._failed_indexs)

    @property
    def failed_indexs(self):
        return self._failed_indexs

    @failed_indexs.setter
    def failed_indexs(self, value):
        if not isinstance(value, list):
            raise ValueError('failed indexs must be List')
        self._failed_indexs = value

    def append(self, record):
        self._record_list.append(record)

    def extend(self, records):
        self._record_list.extend(records)

    def __setitem__(self, index, record):
        if index < 0  or index > len(self._record_list) - 1:
            raise ValueError('index out range')
        self._record_list[index] = record

    def __getitem__(self, index):
        if index < 0  or index > len(self._record_list) - 1:
            raise ValueError('index out range')
        return self._record_list[index]

    def __str__(self):
        recordsjson = {}
        recordsjson['Records'] = []
        for record in self._record_list:
            recordsjson['Records'].append(record.to_json())
        return json.dumps(recordsjson)

    def throw_exception(self, response_result):
        if 'NoSuchProject' == response_result.error_code or 'NoSuchTopic' == response_result.error_code or 'NoSuchShard' == response_result.error_code:
            raise errors.NoSuchObjectException(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)
        elif 'InvalidShardOperation' == response_result.error_code:
            raise errors.InvalidShardOperationException(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)
        elif 'MalformedRecord' == response_result.error_code:
            raise errors.MalformedRecordException(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)
        elif 'LimitExceeded' == response_result.error_code:
            raise errors.LimitExceededException(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)
        elif 'InvalidParameter' == response_result.error_code or 'InvalidCursor' == response_result.error_code:
            raise errors.InvalidParameterException(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)
        elif response_result.status_code >= 500:
            raise errors.ServerInternalError(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)
        else:
            raise errors.DatahubException(response_result.status_code, response_result.request_id, response_result.error_code, response_result.error_msg)

    def resource(self):
        if not self._project_name or not self._topic_name:
            raise ValueError('project and topic name must not be empty')
        if not self._shard_id:
            return "/projects/%s/topics/%s/shards" %(self._project_name, self._topic_name)
        else:
            return "/projects/%s/topics/%s/shards/%s" %(self._project_name, self._topic_name, self._shard_id)

    def encode(self, method):
        ret = {}
        data = {}
        if not self._shard_id:
            data['Action'] = 'pub'
            data['Records'] = []
            if len(self._record_list) == 0:
                raise ValueError('record list is empty')
            for record in self._record_list:
                data['Records'].append(record.to_json())
        else:
            data['Action'] = 'sub'
            if not self._next_cursor:
                raise ValueError('cursor must be set')
            data['Cursor'] = self._next_cursor
            data['Limit'] = self._limit_num
        ret['data'] = json.dumps(data)
        return ret

    def decode(self, method, resp):
        if HTTPMethod.POST == method:
            content = json.loads(resp.content)
            if not self._shard_id:
                failed_indexs = []
                for failed_index in content['FailedRecords']:
                    failed_indexs.append(failed_index['Index'])
                self.failed_indexs = failed_indexs
            else:
                self.next_cursor = content['NextCursor']
                for item in content['Records']:
                    record = None
                    data = item["Data"]
                    if isinstance(data, six.string_types):
                        record = BlobRecord(values=data)
                    else:
                        record = TupleRecord(schema=self._schema, values=data)
                    if 'Attributes' in item:
                        for attr in item['Attributes']:
                            record.put_attribute(attr, item['Attributes'][attr])
                    record.shard_id = self._shard_id
                    self._record_list.append(record)

