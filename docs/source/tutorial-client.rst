.. _tutorial-client:

*********************
high-level client操作
*********************

High-level client提供了更便利的数据发布和订阅功能。

发布数据
---------------------

* 创建DatahubProducer

.. code-block:: python

    producer_config = ProducerConfig(access_id, access_key, endpoint)
    datahub_producer = DatahubProducer(project_name, topic_name, producer_config)

详细定义：
:ref:`producer`

* 数据发布方式

 同步发布

.. code-block:: python

    shard_id = datahub_producer.write(records)

接口传入的records是用户希望写入的数据，返回的是当前records写入的shard_id。

 异步发布

.. code-block:: python

    result = datahub_producer.write_async(records)

接口传入的records是用户发布的数据，返回的result是异步写入的结果。

订阅数据
--------------------

* 创建DatahubConsumer

.. code-block:: python

    consumer_config = ConsumerConfig(access_id, access_key, endpoint)
    datahub_consumer = DatahubConsumer(project_name, topic_name, sub_id, consumer_config)

详细定义：
:ref:`consumer`

* 数据订阅方式

.. code-block:: python

    record = datahub_consumer.read(timeout=60)

接口传入的参数timeout是订阅数据超时时间，返回的record是消费的数据。
