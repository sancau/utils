"""
Logic to use server side caching.
Supports multiple independent queries from different processes / threads.

if data in cache -> return data
if no data in cache:
    if it is being fetched by some process -> wait for result to return it
    else:
        fetch data, save it in cache and return it
"""

import functools
from time import sleep

from django.core.cache import cache


WAIT_FOR_POLLING_DELAY = 0.25  # sec
DEFAULT_CALL_TIMEOUT = 20  # sec


def wait_for_data(key, call_timeout):
    time_passed = 0
    while not cache.get(key).get('resolved'):
        sleep(WAIT_FOR_POLLING_DELAY)
        time_passed += WAIT_FOR_POLLING_DELAY
        if time_passed > call_timeout:
            cache.delete(key)  # kind of circuit breaker
            raise TimeoutError(
                f'Timeout reached while waiting for data. Key: {key}',
            )
    return cache.get(key)['data']


def cacheable(key_prefix=None, call_timeout=DEFAULT_CALL_TIMEOUT, ttl=None):
    """Stores function output in Django cache using given prefix and TTL.

    Supports multithreading: different threads/processes will not concurrently call the function.
    The function will be called only once. Other threads/processes will be waiting for the data.
    """
    if key_prefix is None:
        key_prefix = ''

    def deco(function):
        @functools.wraps(function)
        def inner(*args, **kwargs):
            cache_key = f'{key_prefix}__{function.__name__}'
            cache_record = cache.get(cache_key)
            if cache_record:
                if cache_record['resolved']:
                    return cache_record['data']
                else:
                    return wait_for_data(cache_key, call_timeout)

            # try to start fetching if fetch not in progress or wait for data
            should_wait = not cache.add(
                cache_key,
                {'resolved': False, 'data': None},
            )
            if should_wait:
                return wait_for_data(cache_key, call_timeout)

            data = function(*args, **kwargs)

            kwargs = {}
            if ttl is not None:
                kwargs['timeout'] = ttl  # cache time to live value (expiration timeout)

            cache.set(
                cache_key,
                {'resolved': True, 'data': data},
                **kwargs,
            )

            return data
        return inner
    return deco
