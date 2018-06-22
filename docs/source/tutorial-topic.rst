.. _tutorial-topic:

*************
topic操作
*************

Topic是 DataHub 订阅和发布的最小单位，用户可以用Topic来表示一类或者一种流数据。目前支持Tuple与Blob两种类型。Tuple类型的Topic支持类似于数据库的记录的数据，每条记录包含多个列。Blob类型的Topic仅支持写入一块二进制数据。

Tuple Topic
===========

Tuple类型Topic写入的数据是有格式的，需要指定Record Schema，目前支持以下几种数据类型:

+-----------+------------------------------------+---------------------------------------------------+
|  类型     |                含义                |           值域                                    |
+===========+====================================+===================================================+
| Bigint    |           8字节有符号整型。        |     -9223372036854775807 ~ 9223372036854775807    |
+-----------+------------------------------------+---------------------------------------------------+
| String    |      字符串，只支持UTF-8编码。     |             单个String列最长允许1MB。             |
+-----------+------------------------------------+---------------------------------------------------+
| Boolean   |             布尔类型               |            True/False或true/false或0/1            |
+-----------+------------------------------------+---------------------------------------------------+
| Double    |         8字节双精度浮点数          |           -1.0 * 10^308 ~ 1.0 * 10^308            |
+-----------+------------------------------------+---------------------------------------------------+
| TimeStamp |            时间戳类型              |             表示到微秒的时间戳类型                |
+-----------+------------------------------------+---------------------------------------------------+

创建示例
--------

.. code-block:: python

    project_name = 'topic_test_project'
    topic_name = 'tuple_topic_test_topic'
    shard_count = 3
    life_cycle = 7

    record_schema = RecordSchema.from_lists(
        ['bigint_field',   'string_field',   'double_field',   'bool_field',      'time_field'       ],
        [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP]
    )
    
    try:
        dh.create_tuple_topic(project_name, topic_name, shard_count, life_cycle, record_schema, 'comment')
        print("create topic success!")
        print("=======================================\n\n")
    except InvalidParameterException as e:
        print(e)
        print("=======================================\n\n")
    except ResourceExistException as e:
        print("topic already exist!")
        print("=======================================\n\n")
    except Exception as e:
        print(traceback.format_exc())
        sys.exit(-1)

新增field
-------------

.. code-block:: python

    dh.append_field(project_name, topic_name, field_name, field_type)

新增field必须是allow_null为True的，给出field_name和field_type作为参数即可，field_type为FieldType枚举类型。

Blob Topic
==========

Blob类型Topic支持写入一块二进制数据作为一个Record，数据将会以BASE64编码传输。

创建示例
--------

.. code-block:: python

    project_name = 'topic_test_project'
    topic_name = 'blob_topic_test_topic'
    shard_count = 3
    life_cycle = 7

    
    try:
        dh.create_blob_topic(project_name, topic_name, shard_count, life_cycle, 'comment')
        print("create topic success!")
        print("=======================================\n\n")
    except InvalidParameterException as e:
        print(e)
        print("=======================================\n\n")
    except ResourceExistException as e:
        print("topic already exist!")
        print("=======================================\n\n")
    except Exception as e:
        print(traceback.format_exc())
        sys.exit(-1)

