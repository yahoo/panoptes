"""
This module implements an abstract Key/Value store based around Redis
"""
import mmh3

from six import string_types

from .. import const


class PanoptesKeyValueStoreException(BaseException):
    pass


class PanoptesKeyValueStoreValidators(object):
    @classmethod
    def valid_kv_store_class(cls, kv_store_class):
        """
        valid_kv_store_class(cls, kv_store_class)

        Checks if the passed class is a subclass of PanoptesKeyValueStore

        Args:
            kv_store_class (class): The class to check

        Returns:
            bool: True if the class is not null and is an subclass of PanoptesPluginInfo
        """
        return kv_store_class and issubclass(kv_store_class, PanoptesKeyValueStore)


class PanoptesKeyValueStore(object):
    redis_group = const.DEFAULT_REDIS_GROUP_NAME
    """
    Interface to the Key/Value store provided by the Panoptes framework

    Args:
        panoptes_context (PanoptesContext): The PanoptesContext to use. The Redis client associated with the context \
        would be used to create the key/value store
        namespace (str): The namespace associated with the key/value store. Namespaces are the mechanism to partition \
        the underlying key/value store
    """

    def __init__(self, panoptes_context, namespace):
        self._namespace = namespace
        self._panoptes_context = panoptes_context
        self._no_of_shards = self._panoptes_context.get_redis_shard_count(self.redis_group)

    def _normalized_key(self, key):
        return const.KV_NAMESPACE_DELIMITER.join([self.namespace, key])

    def _get_redis_shard(self, key):
        """

        Args:
            key (str): The key to hash on to calculate the shard to get data from/to

        Returns:
            redis.StrictRedis: The Redis Connection
        """
        shard_no = mmh3.hash(key, signed=False) % self._no_of_shards
        return self._panoptes_context.get_redis_connection(group=self.redis_group, shard=shard_no)

    @property
    def namespace(self):
        """
        Returns the namespace property of the PanoptesKeyValueStore instance
        """
        return self._namespace

    def get(self, key):
        """
        Get the value associated with the key from the key/value store

        Args:
            key (str): The key whose value should be returned

        Returns:
            str: The value associated with the key. None if the key is not found in the key/value store. Passes \
            through exceptions in case of failure
        """
        assert key and isinstance(key, string_types), 'key must be a non-empty str or unicode'

        return self._get_redis_shard(key).get(self._normalized_key(key))

    def set(self, key, value, expire=604800):
        """
        Set the value associated with the key in the key/value store

        This does an 'upsert' - inserts the key/value if it does not exist and updates the value if the key exists

        Args:
            key (str): The key whose value should be set
            value (str): The value to set
            expire (int): A positive integer that, if sets, would expire the key in the number of seconds specified

        Returns:
            None: Nothing. Passes through exceptions in case of failure

        """
        assert key and isinstance(key, string_types), 'key must be a non-empty str or unicode'
        assert value and isinstance(value, string_types), 'value must be a non-empty str or unicode'
        assert expire is None or (isinstance(expire, int) and expire > 0), \
            'expire must be an integer greater than zero'

        return self._get_redis_shard(key).set(self._normalized_key(key), value, ex=expire)

    def getset(self, key, value, expire=604800):
        """
        Set the value associated with the key in the key/value store and returns the previously set value

        This does an 'upsert' - inserts the key/value if it does not exist and updates the value if the key exists

        Args:
            key (str): The key whose value should be set
            value (str): The value to set
            expire (int): A positive integer that, if sets, would expire the key in the number of seconds specified

        Returns:
            None: Nothing. Passes through exceptions in case of failure

        """
        assert key and isinstance(key, string_types), 'key must be a non-empty str or unicode'
        assert value and isinstance(value, string_types), 'value must be a non-empty str or unicode'
        assert expire is None or (isinstance(expire, int) and expire > 0), \
            'expire must be an integer greater than zero'

        value = self._get_redis_shard(key).getset(self._normalized_key(key), value)
        self._get_redis_shard(key).expire(self._normalized_key(key), expire)
        return value

    def ttl(self, key):
        """
        Return the Time to Live (TTL) in seconds, of the given key.

        Args:
            key (str): The key whose ttl should be obtained

        Returns:
            ttl (int): TTL of the key, in seconds
        """
        assert key and isinstance(key, string_types), 'key must be a non-empty str or unicode'

        return self._get_redis_shard(key).ttl(self._normalized_key(key))

    def find_keys(self, pattern=None):
        """
        Find keys in the key/value store matching the supplied pattern.

        Args:
            pattern (str): The pattern we should match in finding keys

        Returns:
            List<str>: List of keys matching the supplied pattern.
        """
        keys = list()

        # We iterate through all shards to get all keys
        # TODO: Dedup keys
        for shard in range(0, self._no_of_shards):
            redis_shard_connection = self._panoptes_context.get_redis_connection(self.redis_group, shard)
            keys.extend([_.replace(self.namespace + const.KV_NAMESPACE_DELIMITER, '')
                         for _ in redis_shard_connection.scan_iter(match=self._normalized_key(pattern),
                                                                   count=const.KV_STORE_SCAN_ITER_COUNT)])

        return keys

    def delete(self, key):
        assert key and isinstance(key, string_types), 'key must be a non-empty str or unicode'

        return self._get_redis_shard(key).delete(self._normalized_key(key))

    def set_members(self, set_name):
        """
        Get the members associated with the set from the key/value store

        Args:
            set_name (str): The set whose members should be returned

        Returns:
            list: The members associated with the sey. None if the set is not found in the key/value store. Passes \
            through exceptions in case of failure
        """
        assert set_name and isinstance(set_name, string_types), 'set_name must be a non-empty str or unicode'

        return self._get_redis_shard(set_name).smembers(set_name)

    def set_add(self, set_name, member):
        """
        Add a member to the set associated in the the key/value store

        This does an 'upsert' - inserts the set (and member) if it does not exist and updates the member if the set \
        exists

        Args:
            set_name (str): The key whose value should be set
            member (str): The value of the member to add to the set

        Returns:
            None: Nothing. Passes through exceptions in case of failure

        """
        assert set_name and isinstance(set_name, string_types), 'set_name must be a non-empty str or unicode'
        assert member and isinstance(member, string_types), 'member must be a non-empty str or unicode'

        return self._get_redis_shard(set_name).sadd(self._normalized_key(set_name), member)
