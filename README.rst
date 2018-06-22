Datahub Python SDK
==================

|PyPI version| |Docs| |License| |Implementation|

Elegent way to access Datahub Python SDK API.
`Documentation <http://pydatahub.readthedocs.io/zh_CN/latest/>`__

Installation
------------

The quick way:

.. code:: shell

    $ sudo pip install pydatahub

The dependencies will be installed automatically.

If network is not available, requirements are in dependency folder:

.. code:: shell

    $ cd dependency
    $ pip install -r dependency.txt


Or from source code:

.. code:: shell

    $ virtualenv pydatahub_env
    $ source pydatahub_env/bin/activate
    $ git clone <git clone URL> pydatahub
    $ cd pydatahub
    $ sudo python setup.py install

Python Version
-------------------

Tested on Python 2.7, 3.3, 3.4, 3.5, 3.6 and pypy, Python 3.6 recommended

Dependencies
---------------

-  setuptools (>=3.0)
-  requests (>=2.4.0)
-  simplejson(>=3.3.0)
-  six(>=1.1.0)
-  enum34(>=1.1.5 for python_version < '3.4')

Run Tests
---------

-  install tox:

.. code:: shell

    $ pip install -U tox

-  fill datahub/tests/datahub.ini with your configuration
-  run shell

.. code:: shell

    $ tox

Usage
-----

.. code:: python

    from datahub import DataHub
    dh = DataHub('**your-access-id**', '**your-secret-access-key**', endpoint='**your-end-point**')

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

    shards_result = dh.list_shards(project_name, topic_name)
    print(shards_result)

    # ============================= put tuple records =============================

    from datahub.models import TupleRecord
    records0 = []
    record0 = TupleRecord(schema=topic.record_schema, values=[1, 'yc1', 10.01, True, 1455869335000000])
    record0.shard_id = '0'
    record0.put_attribute('AK', '47')
    records0.append(record0)
    put_result = dh.put_records('pydatahub_test', 'tuple_topic_test', records0)
    print(put_result)

    # ============================= put tuple records =============================

    from datahub.models import BlobRecord
    data = None
    with open(os.path.join(root_path, 'tests/resources/datahub.png'), 'rb') as f:
        data = f.read()
    records1 = []
    record1 = BlobRecord(blob_data=data)
    record1.shard_id = '0'
    record1.put_attribute('a', 'b')
    records1.append(record1)
    put_result = dh.put_records('pydatahub_test', 'blob_topic_test', records1)
    print(put_result)

    # ============================= get cursor =============================

    from datahub.models import CursorType
    cursor_result = dh.get_cursor(project_name, topic_name, '0', CursorType.OLDEST)
    print(cursor_result)

    # ============================= get blob records =============================

    get_result = dh.get_blob_records(project_name, topic_name, '0', cursor, 10)
    print(get_result)
    print(get_result.records)
    print(get_result.records[0])

    # ============================= get tuple records =============================

    get_result = dh.get_tuple_records(project_name, topic_name, '0', record_schema, cursor, 10)
    print(get_result)
    print(get_result.records)
    print(get_result.records[0].values)

Examples
-----------

see more examples in `examples <https://github.com/aliyun/aliyun-datahub-sdk-python/tree/master/examples>`__

Release
--------

Update `changelog <https://github.com/aliyun/aliyun-datahub-sdk-python/tree/master/changelog.rst>`__, then use `bumpversion <https://github.com/peritus/bumpversion>`__ to update version:

1. bugfix: ``bumpversion patch``
2. small feature：``bumpversion minor``
3. breaking change：``bumpversion major``

Contributing
------------

For a development install, clone the repository and then install from
source:

::

    git clone https://github.com/aliyun/aliyun-datahub-sdk-python.git

License
-------

Licensed under the `Apache License
2.0 <https://www.apache.org/licenses/LICENSE-2.0.html>`__

.. |PyPI version| image:: https://img.shields.io/pypi/v/pydatahub.svg?style=flat-square
   :target: https://pypi.python.org/pypi/pydatahub
.. |Docs| image:: https://img.shields.io/badge/docs-latest-brightgreen.svg?style=flat-square
   :target: http://pydatahub.readthedocs.io/zh_CN/latest/
.. |License| image:: https://img.shields.io/pypi/l/pydatahub.svg?style=flat-square
   :target: https://github.com/aliyun/aliyun-datahub-sdk-python/blob/master/LICENSE
.. |Implementation| image:: https://img.shields.io/pypi/implementation/pydatahub.svg?style=flat-square
