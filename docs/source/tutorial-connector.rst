.. _tutorial-connector:

*************
connector操作
*************

DataHub Connector是把DataHub服务中的流式数据同步到其他云产品中的功能，目前支持将Topic中的数据实时/准实时同步到MaxCompute(ODPS)中。用户只需要向DataHub中写入一次数据，并在DataHub服务中配置好同步功能，便可以在其他云产品中使用这份数据。

创建connector
----------------

* create_connector接口能够创建新的connector

给指定的topic创建指定类型的connector，由(project_name, topic_name, connector_type）确定唯一的connector，其中connector_type需要传入ConnectorType枚举类型的对象。
column_fields对象是包含str的list，内容是topic中的field_name。

.. code-block:: python

    from collections import OrderedDict
    from datahub.models import OdpsConnectorConfig, ConnectorType, PartitionMode

    column_fields = ['f1', 'f2', 'f3']
    partition_config = OrderedDict([
        ("ds", "%Y%m%d"),
        ("hh", "%H"),
        ("mm", "%M")
    ])

    connector_config = OdpsConnectorConfig(
        project_name, table_name, odps_endpoint,
        tunnel_endpoint, connector_access_id, connector_access_key,
        partition_mode, time_range, partition_config)
    dh.create_connector(project_name, topic_name, ConnectorType.SINK_ODPS,
                        column_fields, connector_config)

创建odps connector，connector_config是OdpsConnectorConfig类型的对象，需要指定connector_project_name, connector_table_name, odps_endpoint, tunnel_endpoint, connector_access_id, connector_access_key, partition_mode, time_range, partition_config。
partition_mode 是PartitionMode枚举类，包括SYSTEM_TIME，EVENT_TIME，USER_DEFINE三种，partition_config是一个OrderedDict，其中item的顺序要与table中保持一致

.. code-block:: python

    from datahub.models import DatabaseConnectorConfig

    column_fields = ['f1', 'f2', 'f3']
    connector_config = DatabaseConnectorConfig(host, port, database, user, password, table, max_commit_size, ignore)
    dh.create_connector(project_name, topic_name, ConnectorType.SINK_ADS, column_fields, connector_config)

创建ads connector，connector_config是DatabaseConnectorConfig类型的对象。其中max_commit_size是batchCommit大小，单位KB，ignore为bool类型，表示是否选择IgnoreInto模式。
ReplaceInto与IgnoreInto： ReplaceInto模式下，会使用replace into语句将数据插入，反之，IgnoreInto会使用insert方式插入数据库（replace into将根据主键覆盖记录，ignore into将忽略冲突进行写入）

.. code-block:: python

    from datahub.models import EsConnectorConfig

    column_fields = ['f1', 'f2', 'f3']
    connector_config = EsConnectorConfig(index, es_endpoint, es_user, es_password,
                                         es_id_fields, es_type_fields, max_commit_size, proxy_mode)
    dh.create_connector(project_name, topic_name, ConnectorType.SINK_ES, column_fields, connector_config)

创建es connector，connector_config是EsConnectorConfig类型的对象。其中proxy_mode表示是否使用代理模式，若未true将不会扫描es所有node，直接通过代理写入，vpc es必须使用该选项

更多connector相关详细定义：
:ref:`connector`

更新connector
-----------------

* update_connector接口更新指定的connector配置

.. code-block:: python

    dh.update_connector(project_name, topic_name, connector_type, connector_config)

通过指定(project_name, topic_name, connector_type）三个参数确定唯一的connector,connector_config是ConnectorConfig对象。

.. code-block:: python

    # 直接构造新的connector_config
    new_odps_project_name = "1"
    new_system_time_table_name = "2"
    new_odps_endpoint = "3"
    new_tunnel_endpoint = "4"
    new_odps_access_id = "5"
    new_odps_access_key = "6"

    new_partition_config = OrderedDict([("pt", "%Y%m%d"), ("ct", "%H%M")])
    new_connector_config = OdpsConnectorConfig(new_odps_project_name, new_system_time_table_name, new_odps_endpoint,
                                               new_tunnel_endpoint, new_odps_access_id, new_odps_access_key,
                                               PartitionMode.USER_DEFINE, 30, new_partition_config)

    dh.update_connector(cproject_name, topic_name, ConnectorType.SINK_ODPS, new_connector_config)

    #获取原本的connector_config进行部分修改
    new_connector_config = dh.get_connector(connector_test_project_name, system_time_topic_name,
                                                ConnectorType.SINK_ODPS).config

    new_connector_config.project_name = "1"
    new_connector_config.table_name = "2"
    new_connector_config.odps_endpoint = "3"
    new_connector_config.tunnel_endpoint = "4"
    new_connector_config.access_id = "5"
    new_connector_config.access_key = "6"

    dh.update_connector(project_name, topic_name, ConnectorType.SINK_ODPS, new_connector_config)

删除connector
-----------------

* delete_connector接口删除指定的connector

.. code-block:: python

    dh.delete_connector(project_name, topic_name, connector_type)

通过指定(project_name, topic_name, connector_type）三个参数删除对应的connector。

列出connector
-----------------

* list_connector接口能够列出指定topic下的connector名称

.. code-block:: python

    connectors_result = dh.list_connector(project_name, topic_name)
    connector_names = connectors_result.connector_names

list_connector返回的结果是ListConnectorResult对象，包含connector_names成员，是connector名称的list。

查询connector
---------------

* get_connector接口能够查询指定的connector信息

.. code-block:: python

    connector_result = dh.get_connector(project_name, topic_name, connector_type)

get_connector返回的结果是GetConnectorResult对象，成员包含column_fields, type, state, creator, owner, config, shard_contexts。
其中type是ConnectorType枚举类型的对象，state是ConnectorState枚举类型的对象，config是OdpsConnectorConfig对象，shard_contexts是ShardContext对象的list。
ShardContext对象包含shard_id, start_sequence, end_sequence, current_sequence四种shard信息。

详细定义：
:ref:`Shard`

查询connector shard状态
-------------------------

* get_connector_shard_status接口查询connector中指定shard的状态

.. code-block:: python

    status_result = dh.get_connector_shard_status(project_name, topic_name, connector_type, shard_id)

get_connector_shard_status返回的结果是GetDataShardStatusResult对象，其中包含成员start_sequence, end_sequence, current_sequence, last_error_message, state, update_time, record_time, discard_count。

详细定义：
:ref:`Results`

重启connector shard
-----------------------

* reload_connector接口能够重启connector中指定的shard

.. code-block:: python

    dh.reload_connector(project_name, topic_name, connector_type, shard_id)
    dh.reload_connector(project_name, topic_name, connector_type)

指定shard_id，可以重启对应的shard，不指定shard_id重启connector下全部shard

添加新field
---------------

* append_connector_field接口可以给connector添加新的field，但仍需是odps表中存在对应的列。

.. code-block:: python

    dh.append_connector_field(project_name, topic_name, connector_type, field_name)

更新connector状态
--------------------

* update_connector_state接口可以更改指定connector状态

.. code-block:: python

    dh.update_connector_state(project_name, topic_name, connector_type, state)

传入的state是ConnectorState枚举类的对象，分为CONNECTOR_CREATED，CONNECTOR_RUNNING和CONNECTOR_PAUSED，只有将状态置为CONNECTOR_PAUSED才能够更新connector shard状态。

详细定义：
:ref:`Connector`

查询connector完成时间
-------------------------

.. code-block:: python

    result = dh.get_connector_done_time(project_name, topic_name, connector_type)
    print(result.done_time)

get_connector_done_time返回的结果是GetConnectorDoneTimeResult对象，包含成员done_time表示完成时间。