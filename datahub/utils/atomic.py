"""Pure Python AtomicLong, replaces the 'atomic' C extension."""

import threading


class AtomicLong(object):

    def __init__(self, value=0):
        self._lock = threading.Lock()
        self._value = int(value)

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, new_value):
        with self._lock:
            self._value = int(new_value)

    def get_and_set(self, new_value):
        with self._lock:
            old = self._value
            self._value = int(new_value)
            return old

    def compare_and_set(self, expect, update):
        with self._lock:
            if self._value == expect:
                self._value = int(update)
                return True
            return False

    def increment_and_get(self):
        with self._lock:
            self._value += 1
            return self._value

    def get_and_increment(self):
        with self._lock:
            old = self._value
            self._value += 1
            return old

    def decrement_and_get(self):
        with self._lock:
            self._value -= 1
            return self._value

    def get_and_decrement(self):
        with self._lock:
            old = self._value
            self._value -= 1
            return old

    def add_and_get(self, delta):
        with self._lock:
            self._value += int(delta)
            return self._value

    def get_and_add(self, delta):
        with self._lock:
            old = self._value
            self._value += int(delta)
            return old

    def __repr__(self):
        return 'AtomicLong({})'.format(self._value)
