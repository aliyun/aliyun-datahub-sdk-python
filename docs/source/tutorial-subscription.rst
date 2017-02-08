.. _tutorial-subscription:

********************
subscription操作
********************

订阅服务提供了服务端保存用户消费点位的功能，只需要通过简单配置和处理，就可以实现高可用的点位存储服务。

创建subscription
---------------------

* create_subscription能够创建一个新的订阅

.. code-block:: python

    create_result = dh.create_subscription(project_name, topic_name, 'comment')

create_subscription返回的结果是CreateSubscriptionResult对象，其中包含sub_id成员，即创建的订阅id

详细定义：
:ref:`Results`

删除subscription
-------------------

* delete_subscription接口删除一个订阅

.. code-block:: python

    dh.delete_subscription(project_name, topic_name, sub_id)

传入需要删除的sub_id来删除指定的订阅

查询subscription
--------------------

* get_subscription接口能够查询subscription的详细信息

.. code-block:: python

    subscription_result = dh.get_subscription(project_name, topic_name, create_result.sub_id)

get_subscription返回的是GetSubscriptionResult对象，其中包含成员comment, create_time, is_owner, last_modify_time, state, sub_id, topic_name, type。
其中state是SubscriptionState枚举类的对象，分为ACTIVE和INACTIVE。

详细定义：
:ref:`subscription`, :ref:`Results`

更新subscription
--------------------

* update_subscription接口能够更新subscription

.. code-block:: python

    dh.update_subscription(project_name, topic_name, sub_id, new_comment)

update_subscription更新对应sub_id的subscription的comment

更新subscription状态
------------------------

* update_subscription_state接口更新subscription的状态

.. code-block:: python

    dh.update_subscription_state(project_name, topic_name, sub_id, state)

update_subscription_state更新对应sub_id的subscription状态，state是SubscriptionState枚举类的对象，分为ACTIVE和INACTIVE。

列出subscription
-------------------

* list_subscription接口列出topic下的所有subscription

.. code-block:: python

    subscriptions_result = dh.list_subscription(project_name, topic_name, query_key, page_index, page_size)

传入query_key作为搜索条件，可以传空字符串，通过page_index和page_size获取指定范围的subscription信息，如page_index=1, page_size=10，获取1-10个subscription；
page_index=2, page_size=5则获取6-10的subscription。
list_subscription返回的是ListSubscriptionResult对象，其中包含total_count和subscriptions两个成员。
total_count是topic下总共包含的subscription数量，subscriptions是Subscription对象的list。
Subscription对象包含成员comment, create_time, is_owner, last_modify_time, state, sub_id, topic_name, type。
其中state是SubscriptionState枚举类的对象，分为ACTIVE和INACTIVE。

详细定义：
:ref:`subscription`, :ref:`Results`