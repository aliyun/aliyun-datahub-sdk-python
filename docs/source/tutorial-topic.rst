.. _topic:

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

    topic = Topic(name=topic_name)
    topic.project_name = project_name
    topic.shard_count = 3
    topic.life_cycle = 7
    topic.record_type = RecordType.TUPLE
    topic.record_schema = RecordSchema.from_lists(['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'], [Fie
    ldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])
    
    try:
        dh.create_topic(topic)
        print "create topic success!"
        print "=======================================\n\n"
    except ObjectAlreadyExistException, e:
        print "topic already exist!"
        print "=======================================\n\n"
    except Exception, e:
        print traceback.format_exc()
        sys.exit(-1)

Blob Topic
==========

Blob类型Topic支持写入一块二进制数据作为一个Record，数据将会以BASE64编码传输。

创建示例
--------

.. code-block:: python

    topic = Topic(name=topic_name)
    topic.project_name = project_name
    topic.shard_count = 3
    topic.life_cycle = 7
    topic.record_type = RecordType.BLOB
    
    try:
        dh.create_topic(topic)
        print "create topic success!"
        print "=======================================\n\n"
    except ObjectAlreadyExistException, e:
        print "topic already exist!"
        print "=======================================\n\n"
    except Exception, e:
        print traceback.format_exc()
        sys.exit(-1)

