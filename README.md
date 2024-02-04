# Datahub Python SDK

[![PyPI version](https://img.shields.io/pypi/v/pydatahub.svg?style=flat-square)](https://pypi.python.org/pypi/pydatahub) [![Docs](https://img.shields.io/badge/docs-latest-brightgreen.svg?style=flat-square)](http://pydatahub.readthedocs.io/zh_CN/latest/) [![License](https://img.shields.io/pypi/l/pydatahub.svg?style=flat-square)](https://github.com/aliyun/aliyun-datahub-sdk-python/blob/master/LICENSE) ![Implementation](https://img.shields.io/pypi/implementation/pydatahub.svg?style=flat-square)

<div align="center">
  <img src="http://pydatahub.readthedocs.io/zh_CN/latest/_static/PyDatahub.png"><br><br>
</div>
-----------------

Elegant way to access Datahub Python SDK API. [Documentation](https://aliyun-datahub-sdk-python.readthedocs.io/en/latest/)

## Installation

The quick way:

```shell
$ sudo pip install pydatahub
```

The dependencies will be installed automatically.

Or from source code:

```shell
$ virtualenv pydatahub_env
$ source pydatahub_env/bin/activate
$ git clone <git clone URL> pydatahub
$ cd pydatahub
$ python setup.py install
```

If python-dev was not installed, error message like 'Python.h: No such file or directory' will be printed. [See this](https://stackoverflow.com/questions/21530577/fatal-error-python-h-no-such-file-or-directory)

If install in windows, error message like 'Microsoft Visual C++ XX.0 is required', download and install dependency [here](https://wiki.python.org/moin/WindowsCompilers)

If network is not available, requirements are in dependency folder:

```shell
$ cd dependency
$ pip install -r first.txt
$ pip install -r second.txt
```

## Python Version

Tested on Python 2.7, 3.3, 3.4, 3.5, 3.6 and pypy, Python 3.6 recommended


## Dependencies

 * setuptools (>=39.2.0)
 * requests (>=2.4.0)
 * simplejson (>=3.3.0)
 * six (>=1.1.0)
 * enum34 (>=1.1.5 for python_version < '3.4')
 * crcmod (>=1.7)
 * lz4 (>=2.0.0)
 * cprotobuf (>=0.1.9)
 * funcsigs (>=1.0.2)
 * atomic>=0.7.0
 * rwlock>=0.0.6
 * urllib3>=1.26.10

## Run Tests

- install tox:

```shell
$ pip install -U tox
```

- fill datahub/tests/datahub.ini with your configuration
- run shell

```
$ tox
```

## Usage

```python
from datahub import DataHub
dh = DataHub('**your-access-id**', '**your-secret-access-key**', endpoint='**your-end-point**')

# with security token
# dh = DataHub('**your-access-id**', '**your-secret-access-key**', endpoint='**your-end-point**', security_token='**your-security-token**')

# ============================= create project =============================

project_name = 'my_project_name'
comment = 'my project'
dh.create_project(project_name, comment)

# ============================= get project =============================

project_result = dh.get_project('pydatahub_test')
print(project_result)

# ============================= create tuple topic =============================

from datahub.models import RecordSchema, FieldType
topic_name='tuple_topic_test'
shard_count = 3
life_cycle = 7
comment = 'tuple topic'
record_schema = RecordSchema.from_lists(['bigint_field', 'string_field', 'double_field', 'bool_field', 'time_field'],
                                        [FieldType.BIGINT, FieldType.STRING, FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.TIMESTAMP])
dh.create_tuple_topic(project_name, topic_name, shard_count, life_cycle, record_schema, comment)

# ============================= create blob topic =============================

topic_name='blob_topic_test'
shard_count = 3
life_cycle = 7
comment = 'blob topic'
dh.create_tuple_topic(project_name, topic_name, shard_count, life_cycle, comment)

# ============================= get topic =============================

topic_result = dh.get_topic(project_name, topic_name)
print(topic_result)
print(topic_result.record_schema)

# ============================= list shard =============================

shards_result = dh.list_shard(project_name, topic_name)
print(shards_result)

# ============================= put tuple records =============================

from datahub.models import TupleRecord

# put records by shard is recommended
records0 = []
record0 = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
record0.put_attribute('AK', '47')
records0.append(record0)
put_result = dh.put_records_by_shard('pydatahub_test', 'tuple_topic_test', "0", records0)

# records0 = []
# record0 = TupleRecord(schema=record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
# record0.shard_id = '0'
# record0.put_attribute('AK', '47')
# records0.append(record0)
# put_result = dh.put_records('pydatahub_test', 'tuple_topic_test', records0)
print(put_result)

# ============================= put blob records =============================

from datahub.models import BlobRecord

# put records by shard is recommended
data = None
with open(os.path.join(root_path, 'tests/resources/datahub.png'), 'rb') as f:
    data = f.read()
records1 = []
record1 = BlobRecord(blob_data=data)
record1.put_attribute('a', 'b')
records1.append(record1)
put_result = dh.put_records_by_shard('pydatahub_test', 'blob_topic_test', "0" records1)

# records1 = []
# record1 = BlobRecord(blob_data=data)
# record1.shard_id = '0'
# record1.put_attribute('a', 'b')
# records1.append(record1)
# put_result = dh.put_records('pydatahub_test', 'blob_topic_test', records1)

print(put_result)

# ============================= get cursor =============================

from datahub.models import CursorType
cursor_result = dh.get_cursor(project_name, topic_name, '0', CursorType.OLDEST)
print(cursor_result)

# ============================= get blob records =============================

limit = 10
blob_cursor_result = dh.get_cursor(project_name, topic_name, '0', CursorType.OLDEST)
get_result = dh.get_blob_records(project_name, topic_name, '0', blob_cursor_result.cursor, limit)
print(get_result)
print(get_result.records)
print(get_result.records[0])

# ============================= get tuple records =============================

limit = 10
tuple_cursor_result = dh.get_cursor(project_name, topic_name, '0', CursorType.OLDEST)
get_result = dh.get_tuple_records(project_name, topic_name, '0', record_schema, tuple_cursor_result.cursor, limit)
print(get_result)
print(get_result.records)
print(get_result.records[0].values)
```

## Examples

see more examples in [examples](https://github.com/aliyun/aliyun-datahub-sdk-python/tree/master/examples)

## Release

Update [changelog](https://github.com/aliyun/aliyun-datahub-sdk-python/tree/master/changelog.rst), then use [bumpversion](https://github.com/peritus/bumpversion) to update version:

1. bugfix: `bumpversion patch`
2. small feature: `bumpversion minor`
3. breaking change: `bumpversion major`

## Contributing

For a development install, clone the repository and then install from source:

```
git clone https://github.com/aliyun/aliyun-datahub-sdk-python.git
```

## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)
