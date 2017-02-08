.. _tutorial-schema:

*************
schema类型
*************

schema是用来标明数据存储的名称和对应类型的，在创建tuple topic 和 读写 record 的时候用到。因为网络传输中，数据都是以字符串的形式发送，需要schema来转换成对应的类型。

获取schema
===========

* 对于已创建的topic，可以使用get_topic接口来获取schema信息

.. code-block:: python

    topic_result = dh.get_topic(project_name, topic_name)
    record_schema = topic_result.record_schema


详细定义：
:ref:`schema`

定义schema
===========

要创建新的tuple topic,需要自己定义schema，schema可以通过以下方式进行初始化

详细定义：
:ref:`schema`

* 通过lists定义schema

.. code-block:: python

    from datahub.models import RecordSchema, FieldType, Field

    record_schema1 = RecordSchema.from_lists(
        ['bigint_field'  , 'string_field'  , 'double_field'  , 'bool_field'     , 'event_time1'      ],
        [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP]
    )

    record_schema2 = RecordSchema.from_lists(
        ['bigint_field'  , 'string_field'  , 'double_field'  , 'bool_field'     , 'event_time1'      ],
        [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP],
        [True            , False           , True            , False            , True               ]
    )

必须的参数为2个list，第一个list是对应field的名称，第二个list是对应field的类型，第三个list可选，True为对应feild允许为None， False为对应field不能为None，不传第三个list则默认所有field都为True，即可以为None

* 通过json字符串定义schema

.. code-block:: python

    record_schema_1 = RecordSchema.from_json_str(json_str)

json字符串的格式如下：

"{\"fields\":[{\"type\":\"BIGINT\",\"name\":\"a\"},{\"type\":\"STRING\",\"name\":\"b\"}]}"

* 逐个对schema进行set

.. code-block:: python

    record_schema = RecordSchema()
    record_schema.add_field(Field('bigint_field', FieldType.BIGINT))
    record_schema.add_field(Field('string_field', FieldType.STRING), False)
    record_schema.add_field(Field('double_field', FieldType.DOUBLE))
    record_schema.add_field(Field('bool_field', FieldType.BOOLEAN))
    record_schema.add_field(Field('event_time1', FieldType.TIMESTAMP))

参数为Field对象，Field构造函数第一个参数是field的名称，第二个是field的类型，第三个参数可选，True表示field的值允许为None， False表示field的值不能为None，True，即可以为None