#!/usr/bin/env python
# -*- coding: utf-8 -*-
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


import time
import threading


class Timer:

    def __init__(self, timeout):
        self._timeout = 0
        self._start_time = 0
        self._deadline_time = 0
        self._condition = threading.Condition()
        self.reset(timeout)

    def reset(self, timeout=None):
        if timeout:
            self._timeout = timeout
        self._start_time = Timer.get_curr_time()
        self._deadline_time = self._start_time + self._timeout

    def reset_deadline(self):
        self._deadline_time = 0

    def is_expired(self, curr_time=None):
        if curr_time is None:
            return Timer.get_curr_time() > self._deadline_time
        return curr_time > self._deadline_time

    def elapse(self):
        return Timer.get_curr_time() - self._start_time

    def wait_expire(self, diff=None):
        if diff is None:
            diff = self._deadline_time - Timer.get_curr_time()
        else:
            diff = min(diff, self._deadline_time - Timer.get_curr_time())
        if diff > 0:
            with self._condition:
                self._condition.wait(diff)

    def notify_all(self):
        with self._condition:
            self._condition.notify_all()

    def notify_one(self):
        with self._condition:
            self._condition.notify()

    @property
    def timeout(self):
        return self._timeout

    @property
    def start_time(self):
        return self._start_time

    @property
    def deadline_time(self):
        return self._deadline_time

    @staticmethod
    def get_curr_time():
        return time.time()
