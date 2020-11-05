"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""

from mockredis import MockRedis
from redis.exceptions import ConnectionError, TimeoutError


class PanoptesMockRedis(MockRedis):
    def __init__(self, bad_connection=False, timeout=False, **kwargs):
        """
        Establish a connection.

        Args:
            self: (todo): write your description
            bad_connection: (str): write your description
            timeout: (int): write your description
        """
        if bad_connection:
            raise ConnectionError
        super(PanoptesMockRedis, self).__init__(**kwargs)
        self.connection_pool = u'mockredis connection pool'
        self.timeout = timeout

    def get(self, key):
        """
        Get a value from the cache.

        Args:
            self: (todo): write your description
            key: (todo): write your description
        """
        if self.timeout:
            raise TimeoutError
        else:
            return super(PanoptesMockRedis, self).get(key)

    def set(self, key, value, ex=None, px=None, nx=False, xx=False):
        """
        The memcached value.

        Args:
            self: (todo): write your description
            key: (str): write your description
            value: (todo): write your description
            ex: (dict): write your description
            px: (dict): write your description
            nx: (dict): write your description
            xx: (dict): write your description
        """
        if self.timeout:
            raise TimeoutError
        else:
            return super(PanoptesMockRedis, self).set(key, value, ex=ex, px=px, nx=nx, xx=xx)

    def getset(self, key, value, expire=604800):
        """
        Returns the value of the key.

        Args:
            self: (todo): write your description
            key: (str): write your description
            value: (todo): write your description
            expire: (str): write your description
        """
        return super(PanoptesMockRedis, self).getset(key, value)
