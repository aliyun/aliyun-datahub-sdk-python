.. _tutorial-project:

*************
project操作
*************

项目（Project）是DataHub数据的基本组织单元,下面包含多个Topic。值得注意的是，DataHub的项目空间与MaxCompute的项目空间是相互独立的。用户在MaxCompute中创建的项目不能复用于DataHub，需要独立创建。

创建Project
-----------

* create_project接口创建新的Project

.. code-block:: python

    dh.create_project(project_name, comment)

创建Project需要提供Project的名字和描述，Project的名字长度限制为[3,32]，必须以英文字母开头，仅允许英文字母、数字及“_”，大小写不敏感。

删除Project
-----------

* delete_project接口删除Project

.. code-block:: python

    dh.delete_project(project_name)

要删除Project，必须保证Project内没有Topic。

列出Project
-----------

* list_project接口能够获取datahub服务下的所有Project的名字

.. code-block:: python

    projects_result = dh.list_project()

list_project返回的结果是ListProjectResult对象，其中包含成员project_names，是一个包含Project名字的list。

查询Project
-----------

* get_project接口获取一个Project的详细信息

.. code-block:: python

    project_result = dh.get_project(project_name)

get_project返回的结果是GetProjectResult对象，其中包含project_name, comment, create_time, last_modify_time这四个成员。
