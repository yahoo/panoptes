from mockredis import MockRedis
from redis.exceptions import ConnectionError, TimeoutError


class PanoptesMockRedis(MockRedis):
    def __init__(self, bad_connection=False, timeout=False, **kwargs):
        if bad_connection:
            raise ConnectionError
        super(PanoptesMockRedis, self).__init__(**kwargs)
        self.connection_pool = 'mockredis connection pool'
        self.timeout = timeout

    def get(self, key):
        if self.timeout:
            raise TimeoutError
        else:
            return super(PanoptesMockRedis, self).get(key)

    def set(self, key, value, ex=None, px=None, nx=False, xx=False):
        if self.timeout:
            raise TimeoutError
        else:
            return super(PanoptesMockRedis, self).set(key, value, ex=ex, px=px, nx=nx, xx=xx)
