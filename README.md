# Datahub Python SDK

[![PyPI version](https://img.shields.io/pypi/v/pydatahub.svg?style=flat-square)](https://pypi.python.org/pypi/pydatahub) [![Docs](https://img.shields.io/badge/docs-latest-brightgreen.svg?style=flat-square)](http://pydatahub.alibaba.net/pydatahub-docs/) [![License](https://img.shields.io/pypi/l/pydatahub.svg?style=flat-square)](https://github.com/aliyun/aliyun-odps-python-sdk/blob/master/License) ![Implementation](https://img.shields.io/pypi/implementation/pydatahub.svg?style=flat-square)

<div align="center">
  <img src="http://pydatahub.readthedocs.io/zh_CN/latest/_static/PyDatahub.png"><br><br>
</div>
-----------------

Elegent way to access ODPS API. [Documentation](http://pydatahub.readthedocs.io/zh_CN/latest/)

## Installation

The quick way:

```shell
$ sudo pip install git+https://github.com/aliyun/aliyun-datahub-sdk-python.git
```

The dependencies will be installed automatically.

Or from source code:

```shell
$ virtualenv pydatahub_env
$ source pydatahub_env/bin/activate
$ git clone <git clone URL> pydatahub
$ cd pydatahub
$ sudo python setup.py install
```

## Dependencies

 * Python (>=2.6), including Python 3+, pypy, Python 2.7 recommended
 * setuptools (>=3.0)
 * requests (>=2.4.0)

## Run Tests

- fill datahub/tests/datahub.ini with your configuration
- run shell

```
$ cd datahub/tests
$ python blob_topic_test.py # blob类型topic测试demo程序
$ python tuple_topic_test.py # tuple类型topic测试demo程序
```

## Usage

```python
>>> from datahub import DataHub
>>> from datahub.models import Project
>>> dh = DataHub('**your-access-id**', '**your-secret-access-key**', endpoint='**your-end-point**')
>>>
>>> # create project
>>>
>>> project = Project(name='pydatahub_test', comment='pydatahubtest')
>>> dh.create_project(project)
>>> proj = dh.get_project('pydatahub_test')
>>> print proj
{"comment": "pydatahub test", "create_time": 1482917967, "name": "pydatahub_test", "last_modify_time": 1482917967}
>>>
>>> # create topic
>>>
>>> from datahub.models import Topic, RecordType, RecordSchema, FieldType
>>> topic = Topic(name='topic_test_blob')
>>> topic.project_name = 'pydatahub_test'
>>> topic.shard_count = 3
>>> topic.life_cycle = 7
>>> topic.record_type = RecordType.TUPLE
>>> topic.record_schema = RecordSchema.from_lists(['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'], [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])
>>> dh.create_topic(topic)
>>>
>>> # get topic
>>>
>>> topic = dh.get_topic('topic_test', 'pydatahub_test')
>>> print topic.record_schema
RecordSchema {
  bigint_field            bigint
  string_field            string
  double_field            double
  bool_field              boolean
  time_field              timestamp
}
>>> 
>>> # list shard
>>>
>>> shards = dh.list_shards('pydatahub_test','topic_test')
>>> shards
{"Shards": [{"ShardId": "0", "State": "ACTIVE", "BeginHashKey": "00000000000000000000000000000000", "LeftShardId": "4294967295", "ParentShardIds": [], "ClosedTime": 0, "EndHashKey": "55555555555555555555555555555555", "RightShardId": "1"}, {"ShardId": "2", "State": "ACTIVE", "BeginHashKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "LeftShardId": "1", "ParentShardIds": [], "ClosedTime": 0, "EndHashKey": "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", "RightShardId": "4294967295"}, {"ShardId": "1", "State": "ACTIVE", "BeginHashKey": "55555555555555555555555555555555", "LeftShardId": "0", "ParentShardIds": [], "ClosedTime": 0, "EndHashKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "RightShardId": "2"}]}
>>>
>>> # put records
>>> 
>>> from datahub.models import TupleRecord
>>> records = []
>>> topic = dh.get_topic('topic_test', 'pydatahub_test')
>>> record0 = TupleRecord(schema=topic.record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
>>> record0.shard_id = '0'
>>> record0.put_attribute('AK', '47')
>>> records.append(record0)
>>> failed_indexs = dh.put_records('pydatahub_test', 'topic_test', records)
>>> print failed_indexs
[]
>>>
>>> # get cursor
>>>
>>> from datahub.models import CursorType
>>> cursor = dh.get_cursor('pydatahub_test', 'topic_test', CursorType.OLDEST, '0')
>>> print cursor
20000000000000000000000000140000
>>>
>>> # get records
>>>
>>> (record_list, record_num, next_cursor) = dh.get_records(topic, '0', cursor, 10)
>>> print record_num
1
>>> print record_list[0]
{"ShardId": "0", "Attributes": {"AK": "47"}, "HashKey": "", "PartitionKey": "", "Data": ["1", "yc1", "10.01", "true", "1455869335000000"]}
>>> print record_list[0].get_attribute('AK')
47
```

## API Docs

Datahub Python SDK的API Doc使用Sphinx工具生成，所以第一步需要安装Sphinx

```shell
$ sudo pip install -U Sphinx
```

然后执行如下命令：

```shell
$ cd docs
$ make html
```

## Contributing

For a development install, clone the repository and then install from source:

```
git clone https://github.com/aliyun/aliyun-datahub-sdk-python.git
```

## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)
