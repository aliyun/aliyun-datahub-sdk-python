#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import sys

import setuptools

repo_root = os.path.dirname(os.path.abspath(__file__))

try:
    execfile
except NameError:
    def execfile(fname, globs, locs=None):
        locs = locs or globs
        exec(compile(open(fname).read(), fname, "exec"), globs, locs)

version = sys.version_info
PY2 = version[0] == 2
PY3 = version[0] == 3

if PY2 and version[:2] < (2, 7):
    raise Exception('Datahub Python SDK supports Python 2.7+ (including Python 3.3+).')

version_ns = {}
execfile(os.path.join(repo_root, 'datahub', 'version.py'), version_ns)

requirements = []
with open('requirements.txt') as f:
    requirements.extend(f.read().splitlines())


long_description = None
if os.path.exists('README.rst'):
    with open('README.rst') as f:
        long_description = f.read()

setuptools.setup(
    name='pydatahub',
    version=version_ns['__version__'],
    keywords='pydatahub, python, aliyun, datahub, sdk',
    description='Datahub Python SDK',
    long_description=long_description,
    author='panjinxing.pjx',
    author_email='panjinxing.pjx@alibaba-inc.com',
    url='https://github.com/aliyun/aliyun-datahub-sdk-python',
    packages=setuptools.find_packages(exclude=('tests', 'examples')),
    install_requires=requirements,
    license='Apache License 2.0'
)
