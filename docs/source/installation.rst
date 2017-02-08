.. _install:

**************
安装指南
**************


基础环境准备
============

1. 安装pip，可以参考 `地址 <https://pip.pypa.io/en/stable/installing/>`_ 。

2. 安装setuptools

.. code-block:: sh

    $ sudo pip install -U setuptools>=3.0

3. 安装依赖包（可选）

.. code-block:: sh

    $ sudo pip install -U requests>=2.4.0
    $ sudo pip install -U simplejson>=3.3.0


安装PyDatahub
=============

快速安装
--------

.. code-block:: sh

    $ sudo pip install pydatahub

**注意** 这里PyDatahub的相关依赖包如果没有安装的话会自动安装。

源码安装
--------

.. code-block:: sh

    $ git clone https://github.com/aliyun/aliyun-datahub-sdk-python.git
    $ cd aliyun-datahub-sdk-python
    $ sudo python setup.py install

安装验证
========

.. code-block:: sh

    python -c "from datahub import DataHub"

如果上述命令执行成功，恭喜你安装Datahub Python版本SDK成功！
