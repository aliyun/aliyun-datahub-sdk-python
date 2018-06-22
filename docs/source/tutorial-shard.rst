.. _tutorial-shard:

*************
shard操作
*************

Shard表示对一个Topic进行数据传输的并发通道，每个Shard会有对应的ID。每个Shard会有多种状态: Opening - 启动中，Active - 启动完成可服务。每个Shard启用以后会占用一定的服务端资源，建议按需申请Shard数量。

列出shard
-----------

* list_shard接口列出topic中所有的shard信息

.. code-block:: python

    shards_result = dh.list_shard(project_name, topic_name)

list_shard返回的结果是ListShardResult对象，其中包含shards成员，是Shard对象的list，Shard对象包含shard_id, begin_hash_key, end_hash_key, state等多个信息。

详细定义：
:ref:`shard`, :ref:`Results`

合并shard
-----------

* merge_shard接口合并两个相邻的shard

.. code-block:: python

    merge_result = dh.merge_shard(project_name, topic_name, shard_id, adj_shard_id)

传入两个相邻的shard id，合并成功返回MergeShardResult对象，其中包含新生成的shard_id, begin_hash_key, end_hash_key三个成员。

详细定义：
:ref:`shard`, :ref:`Results`

分裂shard
-----------

* split_shard接口根据所给的split key将指定的shard分裂为2个相邻的shard

.. code-block:: python

    split_result = dh.split_shard(project_name, topic_name, shard_id)
    split_result = dh.split_shard(project_name, topic_name, shard_id, split_key)

split_shard返回的结果是SplitShardResult对象，其中包含成员new_shards，是一个ShardBase对象的list，ShardBase对象只包含shard_id, begin_hash_key, end_hash_key三个信息。
如果不指定split_key，会自动查询该shard的hash key的范围，为split_key指定一个中间值进行分裂。

详细定义：
:ref:`shard`, :ref:`Results`