
import decimal
import os
import sys
import time

from six.moves import configparser

from datahub import DataHub
from datahub.core import DatahubProtocolType
from datahub.exceptions import ResourceExistException
from datahub.models import RecordSchema, FieldType, TupleRecord, BlobRecord, CursorType, CompressFormat


current_path = os.path.split(os.path.realpath(__file__))[0]
root_path = os.path.join(current_path, '../..')

configer = configparser.ConfigParser()
configer.read(os.path.join(current_path, '../datahub.ini'))
access_id = configer.get('datahub', 'access_id')
access_key = configer.get('datahub', 'access_key')
endpoint = configer.get('datahub', 'endpoint')

print("=======================================")
print("access_id: %s" % access_id)
print("access_key: %s" % access_key)
print("endpoint: %s" % endpoint)
print("=======================================\n\n")

if not access_id or not access_key or not endpoint:
    print("[access_id, access_key, endpoint] must be set in datahub.ini!")
    sys.exit(-1)

dh_batch         = DataHub(access_id, access_key, endpoint, protocol_type=DatahubProtocolType.BATCH, compress_format=CompressFormat.NONE)
dh_batch_lz4     = DataHub(access_id, access_key, endpoint, protocol_type=DatahubProtocolType.BATCH, compress_format=CompressFormat.LZ4)
dh_batch_zlib    = DataHub(access_id, access_key, endpoint, protocol_type=DatahubProtocolType.BATCH, compress_format=CompressFormat.ZLIB)
dh_batch_deflate = DataHub(access_id, access_key, endpoint, protocol_type=DatahubProtocolType.BATCH, compress_format=CompressFormat.DEFLATE)


def clean_topic(datahub_client, project_name, force=False):
    topic_names = datahub_client.list_topic(project_name).topic_names
    for topic_name in topic_names:
        if force:
            clean_subscription(datahub_client, project_name, topic_name)
        datahub_client.delete_topic(project_name, topic_name)


def clean_subscription(datahub_client, project_name, topic_name):
    subscriptions = datahub_client.list_subscription(project_name, topic_name, '', 1, 100).subscriptions
    for subscription in subscriptions:
        datahub_client.delete_subscription(project_name, topic_name, subscription.sub_id)


class TestRecordBatch:

    def test_put_get_tuple_records_batch(self):
        project_name = "records_batch_project"
        tuple_topic_name = "records_batch_tuple_topic"

        record_schema = RecordSchema.from_lists(
            ['field1', 'field2', 'field3', 'field_int', 'field_float', 'field_bigint', 'field_double', 'field_timestamp', 'field_string', 'field_decimal'],
            [FieldType.BOOLEAN, FieldType.TINYINT, FieldType.SMALLINT, FieldType.INTEGER, FieldType.FLOAT, FieldType.BIGINT, FieldType.DOUBLE, FieldType.TIMESTAMP, FieldType.STRING, FieldType.DECIMAL]
        )

        try:
            dh_batch.create_project(project_name, "")
        except ResourceExistException:
            pass

        try:
            try:
                dh_batch.create_tuple_topic(project_name, tuple_topic_name, 3, 7, record_schema, "")
                dh_batch.wait_shards_ready(project_name, tuple_topic_name)
            except ResourceExistException:
                pass

            # ======================= put record =======================
            tuple_records = []
            for i in range(1):
                record1 = TupleRecord(schema=record_schema,
                                      values=[True, 1, -2, 3, -4.5, 5, -6.6, 7, "xxxyyyzzz", decimal.Decimal("3.1415926535897932384626433832795028841971")])
                record1.shard_id = "0"
                record1.put_attribute("test_key", "test_value")
                # record1.hash_key = 'testHashKey'
                # record1.partition_key = 'testPartitionKey'
                tuple_records.append(record1)
            dh_batch.put_records_by_shard(project_name, tuple_topic_name, "0", tuple_records)

            time.sleep(1)
            # ======================= get record =======================
            cursor = dh_batch.get_cursor(project_name, tuple_topic_name, "0", CursorType.LATEST).cursor
            get_record_result = dh_batch.get_tuple_records(project_name, tuple_topic_name, "0", record_schema, cursor, 30)
            assert get_record_result.record_count == 1
            get_record = get_record_result.records[0]
            assert get_record.values[0] == True
            assert get_record.values[1] == 1
            assert get_record.values[2] == -2
            assert get_record.values[3] == 3
            assert get_record.values[4] == -4.5
            assert get_record.values[5] == 5
            assert get_record.values[6] == -6.6
            assert get_record.values[7] == 7
            assert get_record.values[8] == "xxxyyyzzz"
            assert get_record.values[9] == decimal.Decimal("3.1415926535897932384626433832795028841971")
            assert get_record.attributes == {"test_key": "test_value"}

        except Exception as e:
            print("Error. Exception: ", e)
        finally:
            clean_topic(dh_batch, project_name)
            dh_batch.delete_project(project_name)

    def test_put_get_tuple_records_batch_lz4(self):
        project_name = "records_batch_project"
        tuple_topic_name = "records_batch_tuple_topic"

        record_schema = RecordSchema.from_lists(
            ['field1', 'field2', 'field3', 'field_int', 'field_float', 'field_bigint', 'field_double', 'field_timestamp', 'field_string', 'field_decimal'],
            [FieldType.BOOLEAN, FieldType.TINYINT, FieldType.SMALLINT, FieldType.INTEGER, FieldType.FLOAT, FieldType.BIGINT, FieldType.DOUBLE, FieldType.TIMESTAMP, FieldType.STRING, FieldType.DECIMAL]
        )

        try:
            dh_batch_lz4.create_project(project_name, "")
        except ResourceExistException:
            pass

        try:
            try:
                dh_batch_lz4.create_tuple_topic(project_name, tuple_topic_name, 3, 7, record_schema, "")
                dh_batch_lz4.wait_shards_ready(project_name, tuple_topic_name)
            except ResourceExistException:
                pass

            # ======================= put record =======================
            tuple_records = []
            for i in range(1):
                record1 = TupleRecord(schema=record_schema,
                                      values=[True, 1, -2, 3, -4.5, 5, -6.6, 7, "xxxyyyzzz", decimal.Decimal("3.1415926535897932384626433832795028841971")])
                record1.shard_id = "0"
                record1.put_attribute("test_key", "test_value")
                # record1.hash_key = 'testHashKey'
                # record1.partition_key = 'testPartitionKey'
                tuple_records.append(record1)
            dh_batch_lz4.put_records_by_shard(project_name, tuple_topic_name, "0", tuple_records)

            time.sleep(1)
            # ======================= get record =======================
            cursor = dh_batch_lz4.get_cursor(project_name, tuple_topic_name, "0", CursorType.LATEST).cursor
            get_record_result = dh_batch_lz4.get_tuple_records(project_name, tuple_topic_name, "0", record_schema, cursor, 30)
            assert get_record_result.record_count == 1
            get_record = get_record_result.records[0]
            assert get_record.values[0] == True
            assert get_record.values[1] == 1
            assert get_record.values[2] == -2
            assert get_record.values[3] == 3
            assert get_record.values[4] == -4.5
            assert get_record.values[5] == 5
            assert get_record.values[6] == -6.6
            assert get_record.values[7] == 7
            assert get_record.values[8] == "xxxyyyzzz"
            assert get_record.values[9] == decimal.Decimal("3.1415926535897932384626433832795028841971")
            assert get_record.attributes == {"test_key": "test_value"}

        except Exception as e:
            print("Error. Exception: ", e)
        finally:
            clean_topic(dh_batch_lz4, project_name)
            dh_batch_lz4.delete_project(project_name)

    def test_put_get_tuple_records_batch_zlib(self):
        project_name = "records_batch_project"
        tuple_topic_name = "records_batch_tuple_topic"

        record_schema = RecordSchema.from_lists(
            ['field1', 'field2', 'field3', 'field_int', 'field_float', 'field_bigint', 'field_double', 'field_timestamp', 'field_string', 'field_decimal'],
            [FieldType.BOOLEAN, FieldType.TINYINT, FieldType.SMALLINT, FieldType.INTEGER, FieldType.FLOAT, FieldType.BIGINT, FieldType.DOUBLE, FieldType.TIMESTAMP, FieldType.STRING, FieldType.DECIMAL]
        )

        try:
            dh_batch_zlib.create_project(project_name, "")
        except ResourceExistException:
            pass

        try:
            try:
                dh_batch_zlib.create_tuple_topic(project_name, tuple_topic_name, 3, 7, record_schema, "")
                dh_batch_zlib.wait_shards_ready(project_name, tuple_topic_name)
            except ResourceExistException:
                pass

            # ======================= put record =======================
            tuple_records = []
            for i in range(1):
                record1 = TupleRecord(schema=record_schema,
                                      values=[True, 1, -2, 3, -4.5, 5, -6.6, 7, "xxxyyyzzz", decimal.Decimal("3.1415926535897932384626433832795028841971")])
                record1.shard_id = "0"
                record1.put_attribute("test_key", "test_value")
                # record1.hash_key = 'testHashKey'
                # record1.partition_key = 'testPartitionKey'
                tuple_records.append(record1)
            dh_batch_zlib.put_records_by_shard(project_name, tuple_topic_name, "0", tuple_records)

            time.sleep(1)
            # ======================= get record =======================
            cursor = dh_batch_zlib.get_cursor(project_name, tuple_topic_name, "0", CursorType.LATEST).cursor
            get_record_result = dh_batch_zlib.get_tuple_records(project_name, tuple_topic_name, "0", record_schema, cursor, 30)
            assert get_record_result.record_count == 1
            get_record = get_record_result.records[0]
            assert get_record.values[0] == True
            assert get_record.values[1] == 1
            assert get_record.values[2] == -2
            assert get_record.values[3] == 3
            assert get_record.values[4] == -4.5
            assert get_record.values[5] == 5
            assert get_record.values[6] == -6.6
            assert get_record.values[7] == 7
            assert get_record.values[8] == "xxxyyyzzz"
            assert get_record.values[9] == decimal.Decimal("3.1415926535897932384626433832795028841971")
            assert get_record.attributes == {"test_key": "test_value"}

        except Exception as e:
            print("Error. Exception: ", e)
        finally:
            clean_topic(dh_batch_zlib, project_name)
            dh_batch_zlib.delete_project(project_name)

    def test_put_get_tuple_records_batch_deflate(self):
        project_name = "records_batch_project"
        tuple_topic_name = "records_batch_tuple_topic"

        record_schema = RecordSchema.from_lists(
            ['field1', 'field2', 'field3', 'field_int', 'field_float', 'field_bigint', 'field_double', 'field_timestamp', 'field_string', 'field_decimal'],
            [FieldType.BOOLEAN, FieldType.TINYINT, FieldType.SMALLINT, FieldType.INTEGER, FieldType.FLOAT, FieldType.BIGINT, FieldType.DOUBLE, FieldType.TIMESTAMP, FieldType.STRING, FieldType.DECIMAL]
        )

        try:
            dh_batch_deflate.create_project(project_name, "")
        except ResourceExistException:
            pass

        try:
            try:
                dh_batch_deflate.create_tuple_topic(project_name, tuple_topic_name, 3, 7, record_schema, "")
                dh_batch_deflate.wait_shards_ready(project_name, tuple_topic_name)
            except ResourceExistException:
                pass

            # ======================= put record =======================
            tuple_records = []
            for i in range(1):
                record1 = TupleRecord(schema=record_schema,
                                      values=[True, 1, -2, 3, -4.5, 5, -6.6, 7, "xxxyyyzzz", decimal.Decimal("3.1415926535897932384626433832795028841971")])
                record1.shard_id = "0"
                record1.put_attribute("test_key", "test_value")
                # record1.hash_key = 'testHashKey'
                # record1.partition_key = 'testPartitionKey'
                tuple_records.append(record1)
            dh_batch_deflate.put_records_by_shard(project_name, tuple_topic_name, "0", tuple_records)

            time.sleep(1)
            # ======================= get record =======================
            cursor = dh_batch_deflate.get_cursor(project_name, tuple_topic_name, "0", CursorType.LATEST).cursor
            get_record_result = dh_batch_deflate.get_tuple_records(project_name, tuple_topic_name, "0", record_schema, cursor, 30)
            assert get_record_result.record_count == 1
            get_record = get_record_result.records[0]
            assert get_record.values[0] == True
            assert get_record.values[1] == 1
            assert get_record.values[2] == -2
            assert get_record.values[3] == 3
            assert get_record.values[4] == -4.5
            assert get_record.values[5] == 5
            assert get_record.values[6] == -6.6
            assert get_record.values[7] == 7
            assert get_record.values[8] == "xxxyyyzzz"
            assert get_record.values[9] == decimal.Decimal("3.1415926535897932384626433832795028841971")
            assert get_record.attributes == {"test_key": "test_value"}

        except Exception as e:
            print("Error. Exception: ", e)
        finally:
            clean_topic(dh_batch_deflate, project_name)
            dh_batch_deflate.delete_project(project_name)


    def test_put_get_blob_records_batch(self):
        project_name = "records_batch_project"
        blob_topic_name = "records_batch_blob_topic"
        
        try:
            dh_batch.create_project(project_name, "")
        except ResourceExistException:
            pass
        
        try:
            try:
                dh_batch.create_blob_topic(project_name, blob_topic_name, 3, 7, "")
                dh_batch.wait_shards_ready(project_name, blob_topic_name)
            except ResourceExistException:
                pass

            # ======================= put record =======================
            blob_records = []

            for i in range(1):
                with open(os.path.join(root_path, "tests/resources/datahub.png"), "rb") as f:
                    data = f.read()
                # data = b'test_blob_data-0'
                record = BlobRecord(blob_data=data)
                record.shard_id = "0"
                record.put_attribute("test_key", "test_value")
                blob_records.append(record)

            dh_batch.put_records_by_shard(project_name, blob_topic_name, "0", blob_records)

            time.sleep(1)
            # ======================= get record =======================
            cursor = dh_batch.get_cursor(project_name, blob_topic_name, "0", CursorType.LATEST).cursor
            get_record_result = dh_batch.get_blob_records(project_name, blob_topic_name, "0", cursor, 30)
            assert get_record_result.record_count == 1
            get_record = get_record_result.records[0]
            assert get_record.blob_data == data
            assert get_record.attributes == {"test_key": "test_value"}
        except Exception as e:
            print("Error. Exception: ", e)
        finally:
            clean_topic(dh_batch, project_name)
            dh_batch.delete_project(project_name)

    def test_put_get_blob_records_batch_lz4(self):
        project_name = "records_batch_project"
        blob_topic_name = "records_batch_blob_topic"

        try:
            dh_batch_lz4.create_project(project_name, "")
        except ResourceExistException:
            pass

        try:
            try:
                dh_batch_lz4.create_blob_topic(project_name, blob_topic_name, 3, 7, "")
                dh_batch_lz4.wait_shards_ready(project_name, blob_topic_name)
            except ResourceExistException:
                pass

            # ======================= put record =======================
            blob_records = []

            for i in range(1):
                with open(os.path.join(root_path, "tests/resources/datahub.png"), "rb") as f:
                    data = f.read()
                # data = b'test_blob_data-0'
                record = BlobRecord(blob_data=data)
                record.shard_id = "0"
                record.put_attribute("test_key", "test_value")
                blob_records.append(record)

            dh_batch_lz4.put_records_by_shard(project_name, blob_topic_name, "0", blob_records)

            time.sleep(1)
            # ======================= get record =======================
            cursor = dh_batch_lz4.get_cursor(project_name, blob_topic_name, "0", CursorType.LATEST).cursor
            get_record_result = dh_batch_lz4.get_blob_records(project_name, blob_topic_name, "0", cursor, 30)
            assert get_record_result.record_count == 1
            get_record = get_record_result.records[0]
            assert get_record.blob_data == data
            assert get_record.attributes == {"test_key": "test_value"}
        except Exception as e:
            print("Error. Exception: ", e)
        finally:
            clean_topic(dh_batch_lz4, project_name)
            dh_batch_lz4.delete_project(project_name)

    def test_put_get_blob_records_batch_zlib(self):
        project_name = "records_batch_project"
        blob_topic_name = "records_batch_blob_topic"

        try:
            dh_batch_zlib.create_project(project_name, "")
        except ResourceExistException:
            pass

        try:
            try:
                dh_batch_zlib.create_blob_topic(project_name, blob_topic_name, 3, 7, "")
                dh_batch_zlib.wait_shards_ready(project_name, blob_topic_name)
            except ResourceExistException:
                pass

            # ======================= put record =======================
            blob_records = []

            for i in range(1):
                with open(os.path.join(root_path, "tests/resources/datahub.png"), "rb") as f:
                    data = f.read()
                # data = b'test_blob_data-0'
                record = BlobRecord(blob_data=data)
                record.shard_id = "0"
                record.put_attribute("test_key", "test_value")
                blob_records.append(record)

            dh_batch_zlib.put_records_by_shard(project_name, blob_topic_name, "0", blob_records)

            time.sleep(1)
            # ======================= get record =======================
            cursor = dh_batch_zlib.get_cursor(project_name, blob_topic_name, "0", CursorType.LATEST).cursor
            get_record_result = dh_batch_zlib.get_blob_records(project_name, blob_topic_name, "0", cursor, 30)
            assert get_record_result.record_count == 1
            get_record = get_record_result.records[0]
            assert get_record.blob_data == data
            assert get_record.attributes == {"test_key": "test_value"}
        except Exception as e:
            print("Error. Exception: ", e)
        finally:
            clean_topic(dh_batch_zlib, project_name)
            dh_batch_zlib.delete_project(project_name)

    def test_put_get_blob_records_batch_deflate(self):
        project_name = "records_batch_project"
        blob_topic_name = "records_batch_blob_topic"

        try:
            dh_batch_deflate.create_project(project_name, "")
        except ResourceExistException:
            pass

        try:
            try:
                dh_batch_deflate.create_blob_topic(project_name, blob_topic_name, 3, 7, "")
                dh_batch_deflate.wait_shards_ready(project_name, blob_topic_name)
            except ResourceExistException:
                pass

            # ======================= put record =======================
            blob_records = []

            for i in range(1):
                with open(os.path.join(root_path, "tests/resources/datahub.png"), "rb") as f:
                    data = f.read()
                # data = b'test_blob_data-0'
                record = BlobRecord(blob_data=data)
                record.shard_id = "0"
                record.put_attribute("test_key", "test_value")
                blob_records.append(record)

            dh_batch_deflate.put_records_by_shard(project_name, blob_topic_name, "0", blob_records)

            time.sleep(1)
            # ======================= get record =======================
            cursor = dh_batch_deflate.get_cursor(project_name, blob_topic_name, "0", CursorType.LATEST).cursor
            get_record_result = dh_batch_deflate.get_blob_records(project_name, blob_topic_name, "0", cursor, 30)
            assert get_record_result.record_count == 1
            get_record = get_record_result.records[0]
            assert get_record.blob_data == data
            assert get_record.attributes == {"test_key": "test_value"}
        except Exception as e:
            print("Error. Exception: ", e)
        finally:
            clean_topic(dh_batch_deflate, project_name)
            dh_batch_deflate.delete_project(project_name)


if __name__ == "__main__":
    test_record_batch = TestRecordBatch()
    test_record_batch.test_put_get_tuple_records_batch()
    test_record_batch.test_put_get_tuple_records_batch_lz4()
    test_record_batch.test_put_get_tuple_records_batch_zlib()
    test_record_batch.test_put_get_tuple_records_batch_deflate()

    test_record_batch.test_put_get_blob_records_batch()
    test_record_batch.test_put_get_blob_records_batch_lz4()
    test_record_batch.test_put_get_blob_records_batch_zlib()
    test_record_batch.test_put_get_blob_records_batch_deflate()
