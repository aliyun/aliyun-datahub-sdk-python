import configparser
import os.path
import threading

from datahub import DataHub
from datahub.client.consumer.datahub_consumer import DatahubConsumer
import time

from datahub.models import TupleRecord, BlobRecord, RecordSchema, FieldType, OffsetBase

CONSUMER_NUM = 1
THREAD_NUM_EACH_CONSUMER = 1

configer = configparser.ConfigParser()
# configer.read(filenames=os.path.join("../datahub.ini.template"))

configer.read(os.path.join('../../examples/datahub.config'))
endpoint = configer.get("datahub", "endpoint")
access_id = configer.get("datahub", "access_id")
access_key = configer.get("datahub", "access_key")
project_name = configer.get("datahub", "project_name")
topic_name = configer.get("datahub", "topic_name")
shard_id = '0'
subscription_id = configer.get("datahub", "sub_id")

dh = DataHub(access_id, access_key, endpoint)

schema = RecordSchema.from_lists(
    ['field1', 'field2', 'field3', 'field_int', 'field_float', 'field_bigint', 'field_double', 'field_timestamp', 'field_string', 'field_decimal'],
    [FieldType.BOOLEAN, FieldType.TINYINT, FieldType.SMALLINT, FieldType.INTEGER, FieldType.FLOAT, FieldType.BIGINT, FieldType.DOUBLE, FieldType.TIMESTAMP, FieldType.STRING, FieldType.DECIMAL])

def reset_offset():
    # 0. 重置点位
    blob_list_shard = dh.list_shard(project_name, topic_name)
    reset_offset ={str(shard.shard_id): OffsetBase(-1, -1) for shard in blob_list_shard.shards}
    dh.reset_subscription_offset(project_name, topic_name, subscription_id, reset_offset)

def tuple_producer(project, topic):
    for i in range(22):
        dh.wait_shards_ready(project, topic)

        tuple_records = []
        num = 11
        for j in range(num):
            record1 = TupleRecord(schema=schema, values=[True, 1, 2, 3, 4., 5, 6., 7, "zzzxxx", 8.999999])
            record1.shard_id = "0"
            record1.put_attribute("name", "test-tuple-producer")
            tuple_records.append(record1)
        dh.put_records_by_shard(project, topic, shard_id, tuple_records)
        print("{} put {} tuple records to shard {}".format(threading.currentThread().name, num, j))
        # time.sleep(10)


def blob_producer(project, topic):
    for i in range(22):
        dh.wait_shards_ready(project, topic)

        blob_records = []
        num = 11
        for j in range(num):
            record1 = BlobRecord(blob_data=b'test-data-producer')
            record1.shard_id = "0"
            record1.put_attribute("name", "test_blob_producer")
            blob_records.append(record1)
        dh.put_records_by_shard(project, topic, shard_id, blob_records)
        print("{} put {} blob records to shard {}".format(threading.currentThread().name, num, j))
        # time.sleep(10)


def multi_consumer_multi_thread(project, topic, sub_id, consumer_num=1, thread_num=1):
    def consume(consumer):
        # 2. 开始消费，只需要循环调用read()接口即可
        try:
            record_cnt = 0
            while True:
                try:
                    record_data = consumer.read(timeout=3)
                    if not record_data:
                        time.sleep(1)
                        continue
                    # 3. 已读到数据，处理
                    # TODO: deal with record data
                    record_cnt += 1
                    print("Thread {} - has read {} records; record = {}".format(threading.currentThread().name, record_cnt, record_data))
                    # ack the record manually if auto_ack_offset is False
                    # record_data.record_key.ack()
                    # time.sleep(0.1)
                except Exception as e:
                    print("Read data fail. Exception: ", e)
                    break
        finally:
            # 4. 关闭资源
            dh_blob_consumer.close()

    consumers = []
    for consumer_index in range(consumer_num):
        # 1. 创建 DatahubConsumer
        # dh_blob_consumer = DatahubConsumer(project, topic, sub_id, configer, shard_ids=["0"])
        dh_blob_consumer = DatahubConsumer(project, topic, sub_id, configer)
        consumers.append(dh_blob_consumer)

        threads = []
        for thread_index in range(thread_num):
            threads.append(threading.Thread(target=consume, args=(consumers[consumer_index], ),
                                            name="Thread-{}-{}".format(consumer_index, thread_index)))
            threads[thread_index].start()


if __name__ == "__main__":
    reset_offset()

    # threading.Thread(target=tuple_producer, args=(project_name, topic_name, )).start()
    # threading.Thread(target=blob_producer, args=(project_name, topic_name, )).start()

    multi_consumer_multi_thread(project_name, topic_name, subscription_id, CONSUMER_NUM, THREAD_NUM_EACH_CONSUMER)