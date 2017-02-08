.. _record:

*************
数据发布/订阅
*************

发布数据
========

向某个topic下发布数据记录时，每条数据记录需要指定该topic下的一个shard, 因此一般需要通过 ``list_shard`` 接口查看下当前topic下的shard列表

* list_shards接口获取topic下的所有shard

.. code-block:: python

    shards = dh.list_shards(project_name, topic_name)

返回结果是一个List对象，每个元素是一个shard，可以获取shard_id，state状态，begin_hash_key，end_hash_key等信息

* put_records接口向一个topic发布数据

.. code-block:: python

    failed_indexs = dh.put_records(project_name, topic_name, records)

其中传入参数records是一个List对象，每个元素为一个record，但是必须为相同类型的record，即Tuple类型或者Blob类型，返回结果为写入失败记录的数组下标

写入Tuple类型Record示例
-----------------------

.. code-block:: python

    try:
        # block等待所有shard状态ready
        dh.wait_shards_ready(project_name, topic_name)
        print "shards all ready!!!"
        print "=======================================\n\n"
    
        topic = dh.get_topic(topic_name, project_name)
        print "get topic suc! topic=%s" % str(topic)
        if topic.record_type != RecordType.TUPLE:
            print "topic type illegal!"
            sys.exit(-1)
        print "=======================================\n\n"
    
        shards = dh.list_shards(project_name, topic_name)
        for shard in shards:
            print shard
        print "=======================================\n\n"
    
        records = []
    
        record0 = TupleRecord(schema=topic.record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
        record0.shard_id = shards[0].shard_id
        record0.put_attribute('AK', '47')
        records.append(record0)
    
        record1 = TupleRecord(schema=topic.record_schema)
        record1['bigint_field'] = 2
        record1['string_field'] = 'yc2'
        record1['double_field'] = 10.02
        record1['bool_field'] = False
        record1['time_field'] = 1455869335000011
        record1.shard_id = shards[1].shard_id
        records.append(record1)
    
        record2 = TupleRecord(schema=topic.record_schema)
        record2['bigint_field'] = 3
        record2['string_field'] = 'yc3'
        record2['double_field'] = 10.03
        record2['bool_field'] = False
        record2['time_field'] = 1455869335000013
        record2.shard_id = shards[2].shard_id
        records.append(record2)
    
        failed_indexs = dh.put_records(project_name, topic_name, records)
        print "put tuple %d records, failed list: %s" %(len(records), failed_indexs)
        # failed_indexs如果非空最好对failed record再进行重试
        print "=======================================\n\n"
    except DatahubException, e:
        print traceback.format_exc()
        sys.exit(-1)
    else:
        sys.exit(-1)

订阅数据
========

订阅一个topic下的数据，同样需要指定对应的shard，同时需要指定读取游标位置，通过 ``get_cursor`` 接口获取

* 获取Cursor，可以通过三种方式获取：OLDEST, LATEST, SYSTEM_TIME
  - OLDEST: 表示获取的cursor指向当前有效数据中时间最久远的record
  - LATEST: 表示获取的cursor指向当前最新的record
  - SYSTEM_TIME: 表示获取的cursor指向该时间之后接收到的第一条record

.. code-block:: python

    cursor = dh.get_cursor(project_name, topic_name, CursorType.OLDEST, shard_id)

通过get_cursor接口获取用于读取指定位置之后数据的cursor

从指定shard读取数据，需要指定从哪个Cursor开始读，并指定读取的上限数据条数，如果从Cursor到shard结尾少于Limit条数的数据，则返回实际的条数的数据。 

.. code-block:: python

    dh.get_records(topic, shard_id, cursor, 10)

消费Tuple类型Record示例
-----------------------

.. code-block:: python

    try:
        topic = dh.get_topic(topic_name, project_name)
        print "get topic suc! topic=%s" % str(topic)
        if topic.record_type != RecordType.TUPLE:
            print "topic type illegal!"
            sys.exit(-1)
        print "=======================================\n\n"
    
        cursor = dh.get_cursor(project_name, topic_name, CursorType.OLDEST, '0')
        while True:
            (record_list, record_num, next_cursor) = dh.get_records(topic, '0', cursor, 10)
            for record in record_list:
                print record
            if 0 == record_num:
                time.sleep(1)
            cursor = next_cursor
    
    except DatahubException, e:
        print traceback.format_exc()
        sys.exit(-1)
    else:
        sys.exit(-1)

