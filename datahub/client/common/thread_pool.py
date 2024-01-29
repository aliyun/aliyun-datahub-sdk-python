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


import sys
import queue
import threading
from concurrent.futures import Future


class TaskItem:

    def __init__(self, future, func, args, kwargs):
        self._future = future
        self._func = func
        self._args = args
        self._kwargs = kwargs

    def run(self):
        if not self._future.set_running_or_notify_cancel():
            return

        try:
            self._future.set_result(self._func(*self._args, **self._kwargs))
        except BaseException as e:
            self._future.set_exception(e)

    @property
    def future(self):
        return self._future

    @future.setter
    def future(self, value):
        self._future = value


class ThreadPool:

    __slots__ = '_name', '_shut_down', '_shut_down_lock', \
                '_queue_limit', '_queue', '_workers_limit', '_workers'

    def __init__(self, queue_limit=0, workers_limit=8, name="ThreadPool"):
        self._name = name
        self._shut_down = False
        self._shut_down_lock = threading.Lock()

        self._queue_limit = sys.maxsize if queue_limit <= 0 else queue_limit
        self._queue = queue.Queue(self._queue_limit)

        self._workers_limit = workers_limit
        self._workers = [
            threading.Thread(target=self.__worker_run, name="thread_{}".format(_))
            for _ in range(self._workers_limit)
        ]
        list(map(lambda th: th.start(), self._workers))

    def shutdown(self, wait_done=True, cancel_futures=False):
        with self._shut_down_lock:
            if self._shut_down:
                return

            self._shut_down = True

            if cancel_futures:
                while True:
                    try:
                        task = self._queue.get_nowait()
                    except queue.Empty:
                        break

                    if task is not None:
                        task.future.cancel()

            if wait_done:
                list(map(lambda th: th.join(), self._workers))
            self._workers.clear()

    def submit(self, func, *args, **kwargs):
        return self.__submit_task(func, True, *args, **kwargs)

    def submit_nowait(self, func, *args, **kwargs):
        return self.__submit_task(func, False, *args, **kwargs)

    def __submit_task(self, func, block, *args, **kwargs):
        with self._shut_down_lock:
            if self._shut_down:
                return False

            future = Future()
            task = TaskItem(future, func, args, kwargs)
            try:
                self._queue.put(task, block=block)
            except queue.Full:
                return False
            return future

    def __worker_run(self):
        while True:
            try:
                task = self._queue.get(block=True, timeout=1)
            except queue.Empty:
                if self._shut_down:
                    break
                continue
            if task is not None:
                task.run()


class HashThreadPool:

    __slots__ = '_name', '_shut_down', '_shut_down_lock', \
                '_queue_limit', '_workers_limit', '_workers'

    def __init__(self, queue_limit=0, workers_limit=8, name="HashThreadPool"):
        self._name = name
        self._shut_down = False
        self._shut_down_lock = threading.Lock()

        self._queue_limit = sys.maxsize if queue_limit <= workers_limit else queue_limit
        self._workers_limit = workers_limit
        self._workers = [
            ThreadPool(max(self._queue_limit // self._workers_limit, 1), 1, "ThreadPool_{}".format(_))
            for _ in range(self._workers_limit)
        ]

    def shutdown(self, wait_done=True, cancel_futures=False):
        with self._shut_down_lock:
            if self._shut_down:
                return

            self._shut_down = True
            for worker in self._workers:
                worker.shutdown(wait_done, cancel_futures)
            self._workers.clear()

    def submit(self, key, func, *args, **kwargs):
        with self._shut_down_lock:
            if self._shut_down:
                return False

            index = key % self._workers_limit
            worker = self._workers[index]
            return worker.submit(func, *args, **kwargs)

    def submit_nowait(self, key, func, *args, **kwargs):
        with self._shut_down_lock:
            if self._shut_down:
                return False

            index = key % self._workers_limit
            worker = self._workers[index]
            return worker.submit_nowait(func, *args, **kwargs)


def do_something(num):
    print("id = ", num)


if __name__ == "__main__":
    tp = ThreadPool()
    for id in range(100):
        tp.submit(do_something, id)
        # time.sleep(1)
    tp.shutdown()

    print('++++++++++++++++')

    htp = HashThreadPool()
    for id in range(100):
        htp.submit(id, do_something, id)
        # time.sleep(1)
    htp.shutdown()
