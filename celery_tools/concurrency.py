# -*- coding: utf-8 -*-
"""
Concurrency utils.
"""
from django.core.cache import cache


class CacheLock(object):
    """
    A Lock implementation.

    A lock manages an internal value that is 0 or nonexistent when lock is free and 1 when is closed. Can be locked
    calling acquire() and freed calling release().
    """
    def __init__(self, cache_key: str, timeout: int=None):
        """
        Create a Lock using Django cache as backend.

        :param cache_key: Key that will be used in cache to store the lock.
        :param timeout: Time to expire.
        """
        self._cache_key = cache_key
        self._timeout = timeout

    def acquire(self):
        """
        Acquire the lock, blocking follow calls.

        :return: True if lock have been acquired, otherwise False.
        """
        if not self.locked():
            cache.add(self._cache_key, 1, self._timeout)
            result = True
        else:
            result = False
        return result

    def release(self):
        cache.delete(self._cache_key)

    def locked(self):
        return cache.get(self._cache_key, 0) == 1

    def __del__(self):
        return self.release()


class CacheSemaphore(object):
    """
    A Semaphore implementation.

    A semaphore manages an internal counter which is decremented by each acquire() call and incremented by each
    release() call. The counter can never go below zero; when acquire() finds that it is zero, it raise ValueError.
    """
    def __init__(self, cache_key: str, value: int=1):
        """
        Create a semaphore with Django cache as backend.

        :param cache_key: Key that will be used in cache to store the semaphore.
        :param value: Initial value.
        """
        self._cache_key = cache_key

        # Add cache key with given value if key isn't present
        if not cache.get(self._cache_key, False):
            cache.set(self._cache_key, value, None)

    def acquire(self, value: int=1) -> bool:
        """
        Decrement semaphore value. If semaphore is locked or value parameter is greater than current semaphore value
        then raise ValueError indicating that cannot be acquired.

        :param value: Number of values to acquire.
        :return: True if semaphore acquired.
        """
        current_value = self.value()
        if current_value < 1 or current_value < value:
            raise ValueError('Semaphore cannot be acquired')

        cache.decr(self._cache_key, value)
        return True

    def acquire_all(self):
        """
        Decrement all semaphore values.

        :return: Current semaphore value.
        """
        value = self.value()
        self.acquire(value)
        return value

    def value(self) -> int:
        """
        Retrieve the semaphore current value.

        :return: Current value.
        """
        return cache.get(self._cache_key, 0)

    def locked(self) -> bool:
        """
        Check if semaphore is locked.

        :return: True if semaphore is locked, otherwise False.
        """
        return self.value() < 1

    def release(self, value: int=1):
        """
        Increment semaphore value.

        :param value:  Number of values to release.
        """
        cache.incr(self._cache_key, value)

    def delete(self):
        """
        Delete semaphore.
        """
        cache.delete(self._cache_key)

    def __del__(self):
        self.delete()


class CacheBoundedSemaphore(CacheSemaphore):
    """
    A bounded semaphore implementation. Inherit from CacheSemaphore.

    This cannot have more slots than a max value.
    """
    def __init__(self, cache_key: str, value: int=1, max_value: int=None):
        """
        Create a bounded semaphore with Django cache as backend.

        :param cache_key: Key that will be used in cache to store the semaphore.
        :param value: Initial value.
        :param max_value: Max value.
        """
        super(CacheBoundedSemaphore, self).__init__(cache_key, value)
        if max_value is None:
            max_value = value

        if value > max_value:
            raise ValueError("Initial value cannot be greater than max value")

        self._max_value = max_value

    def full(self) -> bool:
        """
        Check if semaphore is full.

        :return: True if semaphore is full, otherwise False.
        """
        return cache.get(self._cache_key, self._max_value) >= self._max_value

    def release(self, value: int=1):
        """
        Increment semaphore value. If values to be released is greater than current slots (max - current values) then
        all slots will be released.

        :param value:  Number of values to release.
        """
        current_value = self.value()
        if value > (self._max_value - current_value):
            value = self._max_value - current_value

        cache.incr(self._cache_key, value)

    def release_all(self):
        """
        Increment semaphore value to max.
        """
        self.release(self._max_value - self.value())

    def __del__(self):
        super(CacheBoundedSemaphore, self).__del__()
