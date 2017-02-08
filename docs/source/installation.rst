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

    $ pip install pydatahub

**注:** 这里PyDatahub的相关依赖包如果没有安装的话会自动安装。

源码安装
--------

.. code-block:: sh

    $ git clone https://github.com/aliyun/aliyun-datahub-sdk-python.git
    $ cd aliyun-datahub-sdk-python
    $ python setup.py install

**注:** 没有网络的情况下可以通过如下方式安装依赖：

.. code-block:: sh

    $ cd dependency
    $ pip install -r first.txt
    $ pip install -r second.txt

安装验证
========

.. code-block:: sh

    python -c "from datahub import DataHub"

如果上述命令执行成功，恭喜你安装Datahub Python版本SDK成功！

常见问题
==========

如果安装过程中出现错误信息'Python.h: No such file or directory'，常用的操作系统安装方式如下:

.. code-block:: sh

    $ sudo apt-get install python-dev   # for python2.x installs
    $ sudo apt-get install python3-dev  # for python3.x installs

    $ sudo yum install python-devel   # for python2.x installs
    $ sudo yum install python34-devel   # for python3.4 installs

如果使用windows操作系统，根据提示信息可到 `此处 <https://wiki.python.org/moin/WindowsCompilers>`__ 下载对应版本的 Visual C++ SDK

