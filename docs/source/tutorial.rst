.. _tutorial:

*************
快速上手
*************


Datahub相关的基本概念
=====================

详情参见 `DataHub基本概念 <https://help.aliyun.com/document_detail/47440.html?spm=5176.doc47440.6.579.ScvA5s>`_ 。

准备工作
========

* 访问DataHub服务需要使用阿里云认证账号，需要提供阿里云accessId及accessKey。 同时需要提供可访问的DataHub服务地址。
* 登陆 `Datahub WebConsole页面 <https://datahub.console.aliyun.com/datahub>`_ ，创建Project 

初始化DataHub对象
=================

Datahub Python SDK提供的所有API接口均由 ``datahub.DataHub`` 类实现，所以第一步就是初始化一个DataHub对象。
可选项：支持protobuf传输，主要在put/get record时，使用protobuf协议。Datahub版本未支持protobuf时需要手动指定enable_pb为False

.. code-block:: python

    from datahub import DataHub

    access_id = ***your access id***
    access_key = ***your access key***
    endpoint = ***your datahub server endpoint***
    dh = DataHub(access_id, access_key, endpoint) # default mode: not support protobuf for datahub <= 2.11
    dh = DataHub(access_id, access_key, endpoint, enable_pb=True) # support protobuf when put/get record, for datahub > 2.11
    dh = DataHub(access_id, access_key, endpoint, compress_format=CompressFormat.LZ4) # use lz4 compression when put/get record

更多详细定义：
:ref:`datahub_client`

接口示例
=================
针对常用接口分别给出以下示例：

.. toctree::
    :maxdepth: 1

    tutorial-project
    tutorial-topic
    tutorial-schema
    tutorial-record
    tutorial-shard
    tutorial-meter
    tutorial-connector
    tutorial-subscription
    tutorial-offset

