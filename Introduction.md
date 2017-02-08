# Datahub Python SDK入门手册 #

 * [1. 前言](#1)
 * [2. 安装](#2)
    * [2.1 快速安装](#2.1)
    * [2.2 源码安装](#2.2)
    * [2.3 安装验证](#2.3)
 * [3. 基本概念](#3)
 * [4. 准备工作](#4)
 * [5. Topic操作](#5)
    * [5.1 Tuple Topic](#5.1)
    * [5.2 Blob Topic](#5.2)
 * [6. 数据发布/订阅](#6)
    * [6.1 获取Shard列表](#6.1)
    * [6.1 发布数据](#6.2)
    * [6.3 获取Cursor](#6.3)
    * [6.4 订阅数据](#6.4)
 * [7. 结尾](#7)

<h1><span id="1">前言</span></h1>

DataHub是 MaxCompute 提供的流式数据处理(Streaming Data)服务，它提供流式数据的发布 (Publish)和订阅 (Subscribe)的功能，让您可以轻松构建基于流式数据的分析和应用。DataHub 可以对各种移动设备，应用软件，网站服务，传感器等产生的大量流式数据进行持续不断的采集，存储和处理。用户可以编写应用程序或者使用流计算引擎来处理写入到 DataHub 的流式数据比如实时web访问日志、应用日志、各种事件等，并产出各种实时的数据处理结果比如实时图表、报警信息、实时统计等。用户编写Datahub应用程序最简单直接的方式就是基于Datahub SDK进行，目前Datahub官方提供的SDK包括C++ SDK和Java SDK，随着越来越多的Pythoner使用Datahub，Python版本Datahub SDK需求量也日益上升，这里就告诉各位Pythoner们一个好消息，Datahub官方Python SDK Beta正式Release（[Github地址](https://github.com/aliyun/aliyun-datahub-sdk-python)），使用非常简单，这里做个入门介绍，大家如有任何疑问随时在Github上提问留言。

<h1><span id="2">安装</span></h1>

<h2><span id="2.1">快速安装</span></h2>

```shell
$ sudo pip install pydatahub
```

<h2><span id="2.2">源码安装</span></h2>

```shell
$ git clone https://github.com/aliyun/aliyun-datahub-sdk-python.git
$ cd aliyun-datahub-sdk-python
$ sudo python setup.py install
```
<h2><span id="2.3">安装验证</span></h2>

```shell
$ python -c "from datahub import DataHub"
```
如果上述命令执行成功，恭喜你安装Datahub Python版本SDK成功！

<h1><span id="3">基本概念</span></h1>

详见: https://help.aliyun.com/document_detail/47440.html?spm=5176.product27797.3.2.VGxgya

<h1><span id="4">准备工作</span></h1>

* 访问DataHub服务需要使用阿里云认证账号，需要提供阿里云accessId及accessKey。 同时需要提供访问的服务地址。
* 创建Project
  * 登陆[Datahub WebConsole](https://datahub.console.aliyun.com/datahub)页面，创建Project
* 初始化Datahub

```python
import sys
import traceback

from datahub import DataHub
from datahub.utils import Configer
from datahub.models import Topic, RecordType, FieldType, RecordSchema, BlobRecord, TupleRecord, CursorType
from datahub.errors import DatahubException, ObjectAlreadyExistException

access_id = ***your access id***
access_key = ***your access key***
endpoint = ***your datahub server endpoint***
dh = DataHub(access_id, access_key, endpoint)
```

<h1><span id="5">Topic操作</span></h1>

<h2><span id="5.1">Tuple Topic</span></h2>

* Tuple类型Topic写入的数据是有格式的，需要指定Record Schema，目前支持以下几种数据类型:

| 类型      | 含义  | 值域          |
|:---------:|:----------:|:--------------------:|
| Bigint    |  8字节有符号整型。请不要使用整型的最小值 (-9223372036854775808)，这是系统保留值。 | -9223372036854775807 ~ 9223372036854775807 |
| String    |  字符串，只支持UTF-8编码。 | 单个String列最长允许1MB。 |
| Boolean   |  布尔型。 | 可以表示为True/False，true/false, 0/1 |
| Double    |  8字节双精度浮点数。     | -1.0 10308 ~ 1.0 10308 |
| TimeStamp |  时间戳类型     | 表示到微秒的时间戳类型 |

* 创建示例

```python
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
```

<h2><span id="5.2">Blob Topic</span></h2>

* Blob类型Topic支持写入一块二进制数据作为一个Record，数据将会以BASE64编码传输。

```python
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
```

<h1><span id="6">数据发布/订阅</span></h1>

<h2><span id="6.1">获取Shard列表</span></h2>

* list_shards接口获取topic下的所有shard

```python
shards = dh.list_shards(project_name, topic_name)
```
返回结果是一个List对象，每个元素是一个shard，可以获取shard_id，state状态，begin_hash_key，end_hash_key等信息

<h2><span id="6.2">发布数据</span></h2>

* put_records接口向一个topic发布数据

```python
failed_indexs = dh.put_records(project_name, topic_name, records)
```
其中传入参数records是一个List对象，每个元素为一个record，但是必须为相同类型的record，即Tuple类型或者Blob类型，返回结果为写入失败记录的数组下标

* 写入Tuple类型Record示例

```python
try:
    # block等待所有shard状态ready
    dh.wait_shards_ready(project_name, topic_name)
    print "shards all ready!!!"
    print "=======================================\n\n"

    topic = dh.get_topic(topic_name, project_name)
    print "get topic suc! topic=%s" % str(topic)
    if topic.record_type != RecordType.TUPLE:
        print "topic type illegal!"
        sys.exit(-1)
    print "=======================================\n\n"

    shards = dh.list_shards(project_name, topic_name)
    for shard in shards:
        print shard
    print "=======================================\n\n"

    records = []

    record0 = TupleRecord(schema=topic.record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
    record0.shard_id = shards[0].shard_id
    record0.put_attribute('AK', '47')
    records.append(record0)

    record1 = TupleRecord(schema=topic.record_schema)
    record1['bigint_field'] = 2
    record1['string_field'] = 'yc2'
    record1['double_field'] = 10.02
    record1['bool_field'] = False
    record1['time_field'] = 1455869335000011
    record1.shard_id = shards[1].shard_id
    records.append(record1)

    record2 = TupleRecord(schema=topic.record_schema)
    record2['bigint_field'] = 3
    record2['string_field'] = 'yc3'
    record2['double_field'] = 10.03
    record2['bool_field'] = False
    record2['time_field'] = 1455869335000013
    record2.shard_id = shards[2].shard_id
    records.append(record2)

    failed_indexs = dh.put_records(project_name, topic_name, records)
    print "put tuple %d records, failed list: %s" %(len(records), failed_indexs)
    # failed_indexs如果非空最好对failed record再进行重试
    print "=======================================\n\n"
except DatahubException, e:
    print traceback.format_exc()
    sys.exit(-1)
else:
    sys.exit(-1)
```

<h2><span id="6.3">获取cursor</span></h2>

* 获取Cursor，可以通过三种方式获取：OLDEST, LATEST, SYSTEM_TIME

  * OLDEST: 表示获取的cursor指向当前有效数据中时间最久远的record

  * LATEST: 表示获取的cursor指向当前最新的record

  * SYSTEM_TIME: 表示获取的cursor指向该时间之后接收到的第一条record

```python
cursor = dh.get_cursor(project_name, topic_name, CursorType.OLDEST, shard_id)
```
通过get_cursor接口获取用于读取指定位置之后数据的cursor

<h2><span id="6.4">订阅数据</span></h2>

* 从指定shard读取数据，需要指定从哪个Cursor开始读，并指定读取的上限数据条数，如果从Cursor到shard结尾少于Limit条数的数据，则返回实际的条数的数据。 

```python
dh.get_records(topic, shard_id, cursor, 10)
```

* 消费Tuple类型Record示例

```python
try:
    # block等待所有shard状态ready
    dh.wait_shards_ready(project_name, topic_name)
    print "shards all ready!!!"
    print "=======================================\n\n"

    topic = dh.get_topic(topic_name, project_name)
    print "get topic suc! topic=%s" % str(topic)
    if topic.record_type != RecordType.TUPLE:
        print "topic type illegal!"
        sys.exit(-1)
    print "=======================================\n\n"

    cursor = dh.get_cursor(project_name, topic_name, CursorType.OLDEST, '0')
    while True:
        (record_list, record_num, next_cursor) = dh.get_records(topic, '0', cursor, 10)
        for record in record_list:
            print record
        if 0 == record_num:
            time.sleep(1)
        cursor = next_cursor

except DatahubException, e:
    print traceback.format_exc()
    sys.exit(-1)
else:
    sys.exit(-1)
```

<h1><span id="7">结尾</span></h1>

* 以上所有代码示例都可以从源码仓库的datahub/tests目录下找到，其中

  * [tuple](https://github.com/aliyun/aliyun-datahub-sdk-python/tree/master/datahub/tests/tuple): 提供了Tuple类型Record的[发布](https://github.com/aliyun/aliyun-datahub-sdk-python/tree/master/datahub/tests/tuple/tuple_topic_pub.py)和[订阅](https://github.com/aliyun/aliyun-datahub-sdk-python/tree/master/datahub/tests/tuple/tuple_topic_sub.py)示例

  * [blob](https://github.com/aliyun/aliyun-datahub-sdk-python/tree/master/datahub/tests/blob): 提供了Blob类型Record的[发布](https://github.com/aliyun/aliyun-datahub-sdk-python/tree/master/datahub/tests/blob/blob_topic_pub.py)和[订阅](https://github.com/aliyun/aliyun-datahub-sdk-python/tree/master/datahub/tests/blob/blob_topic_sub.py)示例
