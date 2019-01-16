.. _tutorial-record:

*************
数据发布/订阅
*************

发布数据
========

向某个topic下发布数据记录时，每条数据记录需要指定该topic下的一个shard, 因此一般需要通过 ``list_shard`` 接口查看下当前topic下的shard列表。

* list_shard接口获取topic下的所有shard

.. code-block:: python

    shards_result = dh.list_shard(project_name, topic_name)

返回结果是一个ListShardResult对象，它的shards成员是一个List，其中每个元素是一个Shard对象，可以获取shard_id，state，begin_hash_key，end_hash_key等信息。


详细定义：
:ref:`Shard`, :ref:`Results`

* put_records接口向一个topic发布数据

.. code-block:: python

    put_result = dh.put_records(project_name, topic_name, records)

其中传入参数records是一个List对象，每个元素为一个record，但是必须为相同类型的record，即Tuple类型或者Blob类型。
返回结果是一个PutDataRecordResult对象，包含failed_record_count和failed_records两个成员，failed_records是一个FailedRecord类型的List，每个FailedRecord对象包含index，error_code，error_massage三种信息。

详细定义：
:ref:`Record`, :ref:`Results`

写入Tuple类型Record示例
-----------------------

.. code-block:: python

    from datahub.models import RecordSchema, FieldType, Field

    try:
        # block等待所有shard状态ready
        dh.wait_shards_ready(project_name, topic_name)
    
        topic_result = dh.get_topic(project_name, topic_name)
        print(topic_result)
        print("=======================================\n\n")
    
        shard_result = dh.list_shard(project_name, topic_name)
        for shard in shard_result.shards:
            print(shard)
        print("=======================================\n\n")

        record_schema = RecordSchema.from_lists(
            ['bigint_field',   'string_field',   'double_field',   'bool_field',      'time_field'       ],
            [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP]
        )

        # 建议使用 put_records_by_shard
        records = []

        record0 = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
        record0.put_attribute('AK', '47')
        records.append(record0)

        record1 = TupleRecord(schema=record_schema)
        record1.values = [1, 'yc1', 10.01, True, 1455869335000000]
        records.append(record1)

        record2 = TupleRecord(schema=record_schema)
        record2.set_value(0, 3)
        record2.set_value(1, 'yc3')
        record2.set_value('double_field', 10.03)
        record2.set_value('bool_field', False)
        record2.set_value('time_field', 1455869335000
        records.append(record2)

        dh.put_records_by_shard(project_name, topic_name, shards[0].shard_id, records)

        # records = []

        # record0 = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
        # record0.shard_id = shards[0].shard_id
        # record0.put_attribute('AK', '47')
        # records.append(record0)

        # record1 = TupleRecord(schema=record_schema)
        # record1.values = [1, 'yc1', 10.01, True, 1455869335000000]
        # record1.shard_id = shards[1].shard_id
        # records.append(record1)

        # record2 = TupleRecord(schema=record_schema)
        # record2.set_value(0, 3)
        # record2.set_value(1, 'yc3')
        # record2.set_value('double_field', 10.03)
        # record2.set_value('bool_field', False)
        # record2.set_value('time_field', 1455869335000013)
        # record2.shard_id = shards[2].shard_id
        # records.append(record2)
    
        # put_result = dh.put_records(project_name, topic_name, records)

        print("put tuple %d records" % len(records))
        print("failed records: \n%s" % put_result)
        # failed_indexs如果非空最好对failed record再进行重试
        print("=======================================\n\n")
    except DatahubException as e:
        print traceback.format_exc()
        sys.exit(-1)


订阅数据
========

订阅一个topic下的数据，同样需要指定对应的shard，同时需要指定读取游标位置，通过 ``get_cursor`` 接口获取

* 获取Cursor，可以通过四种方式获取：OLDEST, LATEST, SEQUENCE, SYSTEM_TIME

  - OLDEST: 表示获取的cursor指向当前有效数据中时间最久远的record

  - LATEST: 表示获取的cursor指向当前最新的record

  - SEQUENCE: 表示获取的cursor指向该序列的record

  - SYSTEM_TIME: 表示获取的cursor指向该时间之后接收到的第一条record

.. code-block:: python

    cursor_result = dh.get_cursor(project_name, topic_name, shard_id, CursorType.OLDEST)
    cursor_result = dh.get_cursor(project_name, topic_name, shard_id, CursorType.LATEST)
    cursor_result = dh.get_cursor(project_name, topic_name, shard_id, CursorType.SEQUENCE, sequence)
    cursor_result = dh.get_cursor(project_name, topic_name, shard_id, CursorType.SYSTEM_TIME, system_time)

get_cursor接口返回类型是GetCursorResult类型的对象，它的成员cursor用于get_data_record接口读取指定位置的数据

从指定shard读取数据，需要指定从哪个cursor开始读，并指定读取的上限数据条数，如果从cursor到shard结尾少于Limit条数的数据，则返回实际的条数的数据。

.. code-block:: python

    dh.get_blob_records(project_name, topic_name, shard_id, cursor, limit_num)
    dh.get_tuple_records(project_name, topic_name, shard_id, record_schema, cursor, limit_num)

消费Tuple类型Record示例
-----------------------

.. code-block:: python

    try:
        topic_result = dh.get_topic(project_name, topic_name)
        print(topic_result)
    
        cursor_result = dh.get_cursor(project_name, topic_name, '0', CursorType.OLDEST)
        cursor = cursor_result.cursor
        while True:
            get_result = dh.get_tuple_records(project_name, topic_name, '0', topic_result.record_schema, cursor, 10)
            for record in get_result.records:
                print(record)
            if 0 == get_result.record_count:
                time.sleep(1)
            cursor = get_result.next_cursor
    
    except DatahubException as e:
        print traceback.format_exc()
        sys.exit(-1)

