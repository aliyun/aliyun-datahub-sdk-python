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


import crcmod.predefined
from .binary_record import BinaryRecord
from .batch_binary_record import BatchBinaryRecord
from .record_header import RECORD_HEADER_SIZE, RecordHeader
from .batch_header import BATCH_HEAD_SIZE, BatchHeader
from ..models.compress import *
from ..models import BlobRecord, TupleRecord, RecordSchema


class BatchSerializer:
    """
    Batch serializer
    """

    @staticmethod
    def serialize(compress_type, schema_object, record_list):
        batch = BatchBinaryRecord()

        # TupleRecord/BlobRecord to BinaryRecord
        binary_records = [BatchSerializer.convert_to_binary_record(record, schema_object) for record in record_list]

        for record in binary_records:
            batch.add_record(record)
        return batch.serialize(compress_type)

    @staticmethod
    def deserialize(init_schema, schema_object, byte_data):
        # bytes --> BatchBinaryRecord
        batch_records = BatchSerializer.convert_byte_to_batch_record(init_schema, schema_object, byte_data)
        # BatchBinaryRecord --> list of TupleRecord/BlobRecord
        record_list = [BatchSerializer.convert_to_record(record, init_schema) for record in batch_records.records]
        return record_list

    # =======================
    # serialize
    # =======================
    @staticmethod
    def convert_to_binary_record(record, schema_object):
        if isinstance(record, BlobRecord):
            binary_record = BinaryRecord(schema=None, version_id=-1)
            binary_record.set_field(0, record.blob_data)
        else:
            schema = RecordSchema(record.field_list)
            version_id = 0
            if schema_object.schema_register:
                version_id_new = int(schema_object) if isinstance(schema_object, str) else schema_object.schema_register.get_version_id(schema_object.project, schema_object.topic, schema)
                if version_id_new:
                    version_id = version_id_new
            binary_record = BinaryRecord(schema=schema, version_id=version_id)
            for i in range(len(schema.field_list)):
                value = record.get_value(i)
                binary_record.set_field(i, value)
        if record.attributes:
            for key, val in record.attributes.items():
                binary_record.add_attribute(key, val)
        return binary_record

    # =======================
    # deserialize
    # =======================

    # BinaryRecord --> TupleRecord/BlobRecord
    @staticmethod
    def convert_to_record(binary_record, init_schema):
        if init_schema is None:             # BLOB
            # set blob data
            blob_data = binary_record.get_field(0)
            record = BlobRecord(blob_data=blob_data)
        else:                               # TUPLE
            # set tuple data
            record = TupleRecord(field_list=None, schema=binary_record.schema, values=None)
            for i in range(binary_record.field_cnt):
                record.set_value(i, value=binary_record.get_field(i))
        # set attribute
        attr_map = binary_record.get_attribute()
        for key, val in attr_map.items():
            record.put_attribute(key, val)
        return record

    @staticmethod
    def convert_byte_to_batch_record(init_schema, schema_object, byte_data):
        # Deserialize the batch header
        batch_header_byte = byte_data[:BATCH_HEAD_SIZE]
        batch_header = BatchHeader.deserialize(batch_header_byte)

        # Check crc
        crc32c = crcmod.predefined.mkCrcFun('crc-32c')
        compute_crc32 = crc32c(byte_data[BATCH_HEAD_SIZE:]) & 0xffffffff
        if batch_header.crc32 != compute_crc32:
            raise DatahubException("Check crc fail. expect: {}, real: {}".format(batch_header.crc32, compute_crc32))

        # Check length
        if batch_header.length != len(byte_data):
            raise DatahubException(
                "Check batch header length fail. expect: {}, real: {}".format(batch_header.length, len(byte_data)))

        # Decompress
        compress_type = CompressFormat.get_compress(batch_header.attributes & 0x03)
        all_binary_buffer = byte_data[BATCH_HEAD_SIZE:]
        data_decompressor = get_compressor(compress_type)
        all_binary_buffer = data_decompressor.decompress(all_binary_buffer, batch_header.raw_size)

        # deserialize to list of BinaryRecord
        batch_records = BatchBinaryRecord()
        next_pos = 0
        for index in range(batch_header.record_count):
            # deserializer record header first
            record_header = RecordHeader.deserialize(all_binary_buffer[next_pos: next_pos + RECORD_HEADER_SIZE])
            total_size = record_header.total_size

            binary_record = BatchSerializer.convert_byte_to_binary_record(init_schema, schema_object, all_binary_buffer[next_pos:next_pos + total_size], record_header)
            next_pos += total_size
            batch_records.add_record(binary_record)
        return batch_records

    @staticmethod
    def convert_byte_to_binary_record(init_schema, schema_object, binary_records_buffer, record_header):
        schema = init_schema
        if schema_object.schema_register:
            schema_new = RecordSchema.from_json_str(schema_object) if isinstance(schema_object, str) else schema_object.schema_register.get_schema(schema_object.project, schema_object.topic, record_header.schema_version)
            if schema_new:
                schema = schema_new

        record = BinaryRecord.deserialize(schema, binary_records_buffer, record_header)
        return record



