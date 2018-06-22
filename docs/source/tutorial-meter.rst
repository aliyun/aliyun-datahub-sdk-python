.. _tutorial-meter:

*************
meter操作
*************

metering info是对shard的资源占用情况的统计信息，一小时更新一次

获取metering info
======================

* get_metering_info接口获取指定shard的统计信息

.. code-block:: python

    result = dh.get_metering_info(project_name, topic_name, shard_id)

get_metering_info返回的结果是GetMeteringInfoResult对象，包含active_time, storage两个成员。

详细定义：
:ref:`Results`