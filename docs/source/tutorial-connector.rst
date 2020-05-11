.. _tutorial-connector:

*************
connector操作
*************

DataHub Connector是把DataHub服务中的流式数据同步到其他云产品中的功能，目前支持将Topic中的数据实时/准实时同步到MaxCompute(ODPS)中。用户只需要向DataHub中写入一次数据，并在DataHub服务中配置好同步功能，便可以在其他云产品中使用这份数据。

创建connector
----------------

* create_connector接口能够创建新的connector

给指定的topic创建指定类型的connector，由(project_name, topic_name, connector_id）确定唯一的connector，其中connector_id是创建connector时返回的结果，也可以通过list_connector来获取。
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

    start_time = int(time.time() * 1000)
    create_result = dh.create_connector(project_name, topic_name, ConnectorType.SINK_ODPS,
                                        column_fields, connector_config, start_time)
    print(create_result.connector_id)

创建odps connector，connector_config是OdpsConnectorConfig类型的对象，需要指定connector_project_name, connector_table_name, odps_endpoint, tunnel_endpoint, connector_access_id, connector_access_key, partition_mode, time_range, partition_config。
partition_mode 是PartitionMode枚举类，包括SYSTEM_TIME，EVENT_TIME，USER_DEFINE三种，partition_config是一个OrderedDict，其中item的顺序要与table中保持一致。start_time可以指定开始同步的时间，可以省略，默认从datahub中最早的数据开始同步。
2.14版本之前创建不返回结果，从2.14版本的服务开始，返回的结果中包含connectorId，用于标志topic下唯一的connector。

更多connector相关详细定义：
:ref:`connector`

.. code-block:: python

    from datahub.models import DatabaseConnectorConfig

    column_fields = ['f1', 'f2', 'f3']
    connector_config = DatabaseConnectorConfig(host, port, database, user, password, table, ignore)
    create_result = dh.create_connector(project_name, topic_name, ConnectorType.SINK_ADS, column_fields, connector_config)
    print(create_result.connector_id)

创建ads connector，connector_config是DatabaseConnectorConfig类型的对象。其中ignore为bool类型，表示是否选择IgnoreInto模式，其他均为str类型的db信息。
ReplaceInto与IgnoreInto： ReplaceInto模式下，会使用replace into语句将数据插入，反之，IgnoreInto会使用insert方式插入数据库（replace into将根据主键覆盖记录，ignore into将忽略冲突进行写入）。
2.14版本之前创建不返回结果，从2.14版本的服务开始，返回的结果中包含connectorId，用于标志topic下唯一的connector

更多connector相关详细定义：
:ref:`connector`

.. code-block:: python

    from datahub.models import EsConnectorConfig

    column_fields = ['f1', 'f2', 'f3']
    connector_config = EsConnectorConfig(index, es_endpoint, es_user, es_password,
                                         es_id_fields, es_type_fields, proxy_mode)
    create_result = dh.create_connector(project_name, topic_name, ConnectorType.SINK_ES, column_fields, connector_config)
    print(create_result.connector_id)

创建es connector，connector_config是EsConnectorConfig类型的对象。其中proxy_mode表示是否使用代理模式，若为true将不会扫描es所有node，直接通过代理写入，vpc es必须使用该选项。
2.14版本之前创建不返回结果，从2.14版本的服务开始，返回的结果中包含connectorId，用于标志topic下唯一的connector。

更多connector相关详细定义：
:ref:`connector`

.. code-block:: python

    from datahub.models import FcConnectorConfig

    column_fields = ['f1', 'f2', 'f3']
    connector_config = FcConnectorConfig(fc_endpoint, fc_service, fc_function, auth_mode, fc_access_id, fc_access_key)
    create_result = dh.create_connector(project_name, topic_name, ConnectorType.SINK_FC, column_fields, connector_config)
    print(create_result.connector_id)

创建fc connector，connector_config是FcConnectorConfig类型的对象。其中 fc_endpoint,fc_service,fc_function 是 function compute 服务的信息， auth_mode 鉴权模式是 AuthMode枚举类型，AK模式需要填写ak信息，STS模式可以不填写。
2.14版本之前创建不返回结果，从2.14版本的服务开始，返回的结果中包含connectorId，用于标志topic下唯一的connector。

更多connector相关详细定义：
:ref:`connector`

.. code-block:: python

    from datahub.models import DatabaseConnectorConfig

    column_fields = ['f1', 'f2', 'f3']
    connector_config = DatabaseConnectorConfig(host, port, database, user, password, table, ignore)
    create_result = dh.create_connector(project_name, topic_name, ConnectorType.SINK_MYSQL, column_fields, connector_config)
    print(create_result.connector_id)

创建 mysql connector，connector_config 是DatabaseConnectorConfig类型的对象。其中ignore为bool类型，表示是否选择IgnoreInto模式，其他均为str类型的db信息。
ReplaceInto与IgnoreInto： ReplaceInto模式下，会使用replace into语句将数据插入，反之，IgnoreInto会使用insert方式插入数据库（replace into将根据主键覆盖记录，ignore into将忽略冲突进行写入）。
2.14版本之前创建不返回结果，从2.14版本的服务开始，返回的结果中包含connectorId，用于标志topic下唯一的connector。

更多connector相关详细定义：
:ref:`connector`

.. code-block:: python

    from datahub.models import OssConnectorConfig

    column_fields = ['f1', 'f2', 'f3']
    connector_config = OssConnectorConfig(oss_endpoint, oss_bucket, prefix, time_format, time_range,
                                          auth_mode, oss_access_id, oss_access_key)
    create_result = dh.create_connector(project_name, topic_name, ConnectorType.SINK_OSS, column_fields, connector_config)
    print(create_result.connector_id)

创建oss connector，connector_config是OssConnectorConfig类型的对象。其中 oss_endpoint 是OSS服务的endpoint, oss_bucket 是OSS服务的Buckect,prefix 是OSS服务的目录前缀，
time_format指定时间格式，可使用'%Y%m%d%H%M'，time_range是oss分区存储的时间范围，单位是分钟， auth_mode 鉴权模式是 AuthMode枚举类型，AK模式需要填写ak信息，STS模式可以不填写。
2.14版本之前创建不返回结果，从2.14版本的服务开始，返回的结果中包含connectorId，用于标志topic下唯一的connector。

更多connector相关详细定义：
:ref:`connector`

.. code-block:: python

    from datahub.models import OtsConnectorConfig

    column_fields = ['f1', 'f2', 'f3']
    write_mode = WriteMode.PUT
    connector_config = OtsConnectorConfig(ots_endpoint, ots_instance, ots_table,
                                          auth_mode, ots_access_id, ots_access_key, write_mode)
    create_result = dh.create_connector(project_name, topic_name, ConnectorType.SINK_OTS, column_fields, connector_config)
    print(create_result.connector_id)

创建ots connector，connector_config是OtsConnectorConfig类型的对象。其中 ots_endpoint 是OTS服务的endpoint, ots_instance 是OTS服务的实例, ots_table 是OTS表名，write_mode是WriteMode枚举类型，默认是PUT模式， auth_mode 鉴权模式是 AuthMode枚举类型，AK模式需要填写ak信息，STS模式可以不填写。
2.14版本之前创建不返回结果，从2.14版本的服务开始，返回的结果中包含connectorId，用于标志topic下唯一的connector。

更多connector相关详细定义：
:ref:`connector`

.. code-block:: python

    from datahub.models import DataHubConnectorConfig

    column_fields = ['f1', 'f2', 'f3']
    connector_config = DataHubConnectorConfig(datahub_endpoint, datahub_project_name, datahub_topic_name,
                                              auth_mode, datahub_access_id, datahub_access_key)
    create_result = dh.create_connector(project_name, topic_name, ConnectorType.SINK_DATAHUB, column_fields, connector_config)
    print(create_result.connector_id)

创建datahub connector，connector_config是DataHubConnectorConfig类型的对象。其中 datahub_endpoint 是DataHub服务的endpoint, datahub_project_name 是DataHub服务的项目名, datahub_topic_name 是项目下的主题名 datahub_topic_name， auth_mode 鉴权模式是 AuthMode枚举类型，AK模式需要填写ak信息，STS模式可以不填写。
2.14版本之前创建不返回结果，从2.14版本的服务开始，返回的结果中包含connectorId，用于标志topic下唯一的connector。

更多connector相关详细定义：
:ref:`connector`


列出connector
-----------------

* list_connector接口能够列出指定topic下的connector名称

.. code-block:: python

    connectors_result = dh.list_connector(project_name, topic_name)
    connector_ids = connectors_result.connector_ids
    print(connector_ids)

list_connector返回的结果是ListConnectorResult对象，包含connector_ids成员，是connectorId的list。


更新connector
-----------------

* update_connector接口更新指定的connector配置

.. code-block:: python

    dh.update_connector(project_name, topic_name, connector_type, connector_id)

通过指定(project_name, topic_name, connector_id）确定唯一的connector，其中connector_id是创建connector时返回的结果，也可以通过list_connector来获取。connector_config是ConnectorConfig对象。

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

    dh.update_connector(cproject_name, topic_name, connector_id, new_connector_config)

    #获取原本的connector_config进行部分修改
    new_connector_config = dh.get_connector(connector_test_project_name, system_time_topic_name, connector_id).config

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

    dh.delete_connector(project_name, topic_name, connector_id)

通过指定(project_name, topic_name, connector_id）三个参数删除对应的connector，其中connector_id是创建connector时返回的结果，也可以通过list_connector来获取。

查询connector
---------------

* get_connector接口能够查询指定的connector信息

.. code-block:: python

    connector_result = dh.get_connector(project_name, topic_name, connector_id)
    print(connector_result)

通过指定(project_name, topic_name, connector_id）三个参数删除对应的connector，其中connector_id是创建connector时返回的结果，也可以通过list_connector来获取。
get_connector返回的结果是GetConnectorResult对象，成员包含connector_id, column_fields, type, state, creator, owner, config。
其中type是ConnectorType枚举类型的对象，state是ConnectorState枚举类型的对象，config是具体connector类型对应的config对象。

详细定义：
:ref:`Results`, :ref:`connector`

查询connector shard状态
-------------------------

* get_connector_shard_status接口查询connector中指定shard的状态

.. code-block:: python

    status_result = dh.get_connector_shard_status(project_name, topic_name, connector_id, shard_id)

通过指定(project_name, topic_name, connector_id）三个参数删除对应的connector，其中connector_id是创建connector时返回的结果，也可以通过list_connector来获取。
参数中的shard_id 不指定的情况下，表示获取所有shard的status信息。
get_connector_shard_status返回的结果是GetDataShardStatusResult对象，其中包含成员 shard_status_infos 是一个dict，key是shard_id, value 是 ShardStatusEntry类型的对象。

详细定义：
:ref:`Results`, :ref:`connector`

重启connector shard
-----------------------

* reload_connector接口能够重启connector中指定的shard

.. code-block:: python

    dh.reload_connector(project_name, topic_name, connector_id, shard_id)
    dh.reload_connector(project_name, topic_name, connector_id)

通过指定(project_name, topic_name, connector_id）三个参数删除对应的connector，其中connector_id是创建connector时返回的结果，也可以通过list_connector来获取。
指定shard_id，可以重启对应的shard，不指定shard_id重启connector下全部shard

添加新field
---------------

* append_connector_field接口可以给connector添加新的field，但仍需是odps表中存在对应的列。

.. code-block:: python

    dh.append_connector_field(project_name, topic_name, connector_id, field_name)

通过指定(project_name, topic_name, connector_id）三个参数删除对应的connector，其中connector_id是创建connector时返回的结果，也可以通过list_connector来获取。
field_name需要在topic的schema中存在。

更新connector状态
--------------------

* update_connector_state接口可以更改指定connector状态

.. code-block:: python

    dh.update_connector_state(project_name, topic_name, connector_id, state)

通过指定(project_name, topic_name, connector_id）三个参数删除对应的connector，其中connector_id是创建connector时返回的结果，也可以通过list_connector来获取。
传入的state是ConnectorState枚举类的对象，分为CONNECTOR_RUNNING和CONNECTOR_STOPPED，只有将状态置为CONNECTOR_STOPPED才能够更新connector shard点位。

详细定义：
:ref:`Connector`


更新connector点位
--------------------

* update_connector_offset接口可以更改指定connector点位

.. code-block:: python

    connector_offset = ConnectorOffset(100, 1582801630000)
    dh.update_connector_state(project_name, topic_name, connector_id, shard_id, connector_offset)

通过指定(project_name, topic_name, connector_id）三个参数删除对应的connector，其中connector_id是创建connector时返回的结果，也可以通过list_connector来获取。
传入的connector_offset是ConnectorOffset类的对象，成员有 sequence 和 timestamp（单位毫秒）。shard_id 传 '' 表示所有shard都指定到同一个点位

详细定义：
:ref:`Connector`


查询connector完成时间
-------------------------

.. code-block:: python

    result = dh.get_connector_done_time(project_name, topic_name, connector_id)
    print(result.done_time)

通过指定(project_name, topic_name, connector_id）三个参数删除对应的connector，其中connector_id是创建connector时返回的结果，也可以通过list_connector来获取。
get_connector_done_time返回的结果是GetConnectorDoneTimeResult对象，包含成员done_time表示完成时间，time_zone表示时区信息，time_window表示时间窗口大小，单位是秒。