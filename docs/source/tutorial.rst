.. _tutorial:

*************
快速上手
*************


Datahub相关的基本概念
=====================

详情参见 `地址 <https://help.aliyun.com/document_detail/47440.html?spm=5176.doc47440.6.579.ScvA5s>`_ 。

准备工作
========

* 访问DataHub服务需要使用阿里云认证账号，需要提供阿里云accessId及accessKey。 同时需要提供可访问的DataHub服务地址。
* 登陆 `Datahub WebConsole页面 <https://datahub.console.aliyun.com/datahub>`_ ，创建Project 

初始化DataHub对象
=================

Datahub Python SDK提供的所有API接口均由 ``datahub.DataHub`` 类实现，所以第一步就是初始化一个DataHub对象。

.. code-block:: python

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


OK，我们针对常用接口分别给出上手示例：

.. toctree::
    :maxdepth: 1

    tutorial-topic
    tutorial-record

