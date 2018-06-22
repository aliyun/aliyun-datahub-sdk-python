.. _install:

**************
安装指南
**************


基础环境准备
============

安装pip，可以参考 `地址 <https://pip.pypa.io/en/stable/installing/>`_ 。

安装PyDatahub
=============

快速安装
--------

.. code-block:: sh

    $ sudo pip install pydatahub

**注:** python3使用pip3,这里PyDatahub的相关依赖包如果没有安装的话会自动安装。

源码安装
--------

.. code-block:: sh

    $ git clone https://github.com/aliyun/aliyun-datahub-sdk-python.git
    $ cd aliyun-datahub-sdk-python
    $ sudo python setup.py install

**注:** python3使用pip3。

安装验证
========

.. code-block:: sh

    python -c "from datahub import DataHub"

如果上述命令执行成功，恭喜你安装Datahub Python版本SDK成功！
