.. _tutorial-offset:

*************
offset操作
*************

一个subscription创建后，初始状态是未消费的，要使用subscription服务提供的点位存储功能，需要进行一些offset操作

初始化offset
--------------

* init_and_get_subscription_offset接口初始化offset，是开始消费的第一步

.. code-block:: python

    init_result = dh.init_and_get_subscription_offset(project_name, topic_name, sub_id, shard_id)
    init_result = dh.init_and_get_subscription_offset(project_name, topic_name, sub_id, shard_ids)

最后一个参数可以是一个shard_id，也可以是shard_id的list，指定要初始化的shard
init_and_get_subscription_offset返回的是InitAndGetSubscriptionOffsetResult对象，包含offsets成员，是一个OffsetWithSession对象，
其中包含成员sequence, timestamp, version, session_id。
sequence和timestamp就是点位信息，第一次初始化的session_id为0，之后每次初始化都会+1

详细定义：
:ref:`Results`

获取offset
-----------

* get_subscription_offset能够获取订阅的offset信息

.. code-block:: python

    offset_result = dh.get_subscription_offset(project_name, topic_name, sub_id, shard_id)
    offset_result = dh.get_subscription_offset(project_name, topic_name, sub_id, shard_ids)

最后一个参数可以是一个shard_id，也可以是shard_id的list，指定要初始化的shard
get_subscription_offset返回的是GetSubscriptionOffsetResult对象，包含OffsetWithVersion对象的list。
OffsetWithVersion类是OffsetWithSession的父类，只包含sequence, timestamp, version

详细定义：
:ref:`offset`, :ref:`Results`

更新offset
-----------

* update_subscription_offset接口能够更新offset

.. code-block:: python

    offsets1 = {
        '0': OffsetWithSession(sequence0, timestamp0, version0, session_id0),
        '1': OffsetWithSession(sequence1, timestamp1, version1, session_id1)
    }

    offsets2 = {
        '0': {
            'Sequence': 0,
            'Timestamp': 0,
            'Version': 0,
            'SessionId': 0
        },
        '1': {
            'Sequence': 1,
            'Timestamp': 1,
            'Version': 1,
            'SessionId': 1
        }
    }

    dh.update_subscription_offset(project_name, topic_name, sub_id, offsets1)
    dh.update_subscription_offset(project_name, topic_name, sub_id, offsets2)


参数offsets是一个dict，其中的key是shard_id，value可以是OffsetWithSession对象，也可以是一个dict，如果version和session_id发生变化，就会更新失败。
当错误信息指出version发生变化，可以通过get_subscription_offset接口获取最新的version信息，继续消费。
当错误信息指出session_id发生变化，就只能再次使用init_and_get_subscription_offset初始化offset信息，再继续消费。

详细定义：
:ref:`offset`

重置offset
-----------

* reset_subscription_offset接口能够重置offset信息并更新version

.. code-block:: python

    offsets1 = {
        '0': OffsetWithSession(sequence0, timestamp0, version0, session_id0),
        '1': OffsetWithSession(sequence1, timestamp1, version1, session_id1)
    }

    offsets2 = {
        '0': {
            'Sequence': 0,
            'Timestamp': 0,
            'Version': 0,
            'SessionId': 0
        },
        '1': {
            'Sequence': 1,
            'Timestamp': 1,
            'Version': 1,
            'SessionId': 1
        }
    }

    offsets3 = {
        '0': OffsetBase(sequence0, timestamp0),
        '1': OffsetBase(sequence1, timestamp1)
    }

    offsets4 = {
        '0': {
            'Sequence': 0,
            'Timestamp': 0
        },
        '1': {
            'Sequence': 1,
            'Timestamp': 1
        }
    }

    dh.reset_subscription_offset(project_name, topic_name, sub_id, offsets1)
    dh.reset_subscription_offset(project_name, topic_name, sub_id, offsets2)
    dh.reset_subscription_offset(project_name, topic_name, sub_id, offsets3)
    dh.reset_subscription_offset(project_name, topic_name, sub_id, offsets4)


参数offsets是一个dict，其中的key是shard_id，value可以是OffsetBase对象以及其子类对象，也可以是一个dict。
OffsetBase是OffsetWithVersion的父类，只包含sequence, timestamp

详细定义：
:ref:`offset`