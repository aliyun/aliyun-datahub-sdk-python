# coding: utf-8
from cprotobuf import ProtoEntity, Field
# file: record.proto
class StringPair(ProtoEntity):
    key             = Field('string',	1)
    value           = Field('string',	2)

class FieldData(ProtoEntity):
    value           = Field('bytes',	1, required=False)

class RecordAttributes(ProtoEntity):
    attributes      = Field(StringPair,	1, repeated=True)

class RecordData(ProtoEntity):
    data            = Field(FieldData,	1, repeated=True)

class FailedRecord(ProtoEntity):
    index           = Field('int32',	1)
    error_code      = Field('string',	2, required=False)
    error_message   = Field('string',	3, required=False)

class RecordEntry(ProtoEntity):
    shard_id        = Field('string',	1, required=False)
    hash_key        = Field('string',	2, required=False)
    partition_key   = Field('string',	3, required=False)
    cursor          = Field('string',	4, required=False)
    next_cursor     = Field('string',	5, required=False)
    sequence        = Field('int64',	6, required=False)
    system_time     = Field('int64',	7, required=False)
    attributes      = Field(RecordAttributes,	8, required=False)
    data            = Field(RecordData,	9)

class GetRecordsResponse(ProtoEntity):
    next_cursor     = Field('string',	1)
    record_count    = Field('int32',	2)
    start_sequence  = Field('int64',	3, required=False)
    records         = Field(RecordEntry,	4, repeated=True)

class GetRecordsRequest(ProtoEntity):
    cursor          = Field('string',	1)
    limit           = Field('int32',	2, required=False, default=1)

class PutRecordsResponse(ProtoEntity):
    failed_count    = Field('int32',	1, required=False)
    failed_records  = Field(FailedRecord,	2, repeated=True)

class PutRecordsRequest(ProtoEntity):
    records         = Field(RecordEntry,	1, repeated=True)





