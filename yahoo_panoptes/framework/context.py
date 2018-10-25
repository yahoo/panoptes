"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

The Panoptes Context is one of the most important abstractions throughout the system. It is a thread-safe interface to
configuration and utilities throughout the system.

A Panoptes Context holds the system wide configuration, a logger and a Redis connection pool

In addition, a Context can optionally hold the following: multiple Key/Value stores, a Message Producer (with it's
underlying Kafka Client) and a ZooKeeper client

The Context object, once created, would be passed between multiple objects and methods within a process
"""
import os
import inspect
import logging
import re
from logging import StreamHandler, Formatter

import kazoo.client
import kazoo.client
import redis
from kafka import KafkaClient
from kafka.common import ConnectionError
from kazoo.exceptions import LockTimeout

from . import const
from .validators import PanoptesValidators
from .configuration_manager import PanoptesConfig
from .exceptions import PanoptesBaseException
from .utilities.helpers import get_calling_module_name
from .utilities.key_value_store import PanoptesKeyValueStore
from .utilities.message_queue import PanoptesMessageQueueProducer


class PanoptesContextError(PanoptesBaseException):
    """
    A class that encapsulates all context creation errors
    """
    pass


class PanoptesContextValidators(object):
    @classmethod
    def valid_panoptes_context(cls, panoptes_context):
        """
        valid_panoptes_context(cls, panoptes_context)

        Checks if the passed object is an instance of PanoptesContext

        Args:
            panoptes_context (PanoptesContext): The object to check

        Returns:
            bool: True if the object is not null and is an instance of PanoptesContext
        """
        return panoptes_context and isinstance(panoptes_context, PanoptesContext)


class PanoptesContext(object):
    """
    A thread-safe object that parses system wide config and sets up clients to various stores like Redis, Zookeeper and
    Kafka.

    A PanoptesContext is an essential object for all Panoptes subsystems. Creating a context does the following:

        * Parses and loads the system wide Panoptes configuration file
        * Creates a Python logger hierarchy based on the logging configuration file name provided in the system wide \
        configuration
        * A Redis Connection Pool (created through a call to the PanoptesConfiguraton class)
        * (Optional) Creates one or more Key/Value stores
        * (Optional) Creates a message producer client
        * (Optional) Creates a ZooKeeper client

    Args:
        config_file (str): Absolute path to the system wide configuration file
        key_value_store_class_list (list): A list of the Key/Value classes. PanoptesContext will create KV stores
        based on each class. Default is an empty list
        create_message_producer (bool): Whether a message producer client should be created. Default is False
        async_message_producer (bool): Whether the message producer client should be asynchronous. Default is False. \
        This parameter only matters if the create_message_producer is set to True
        create_zookeeper_client (bool): Whether a ZooKeeper client should be created


    Notes:
        * If a ZooKeeper client is created, three additional threads are created by by the Kazoo library
    """
    __rootLogger = None

    def __init__(self, config_file=None, key_value_store_class_list=None,
                 create_message_producer=False, async_message_producer=False, create_zookeeper_client=False):
        assert config_file is None or PanoptesValidators.valid_nonempty_string(config_file), \
            'config_file must be a non-empty string'
        assert key_value_store_class_list is None or isinstance(key_value_store_class_list,
                                                                list), 'key_value_store_class_list must be a list'

        self.__redis_connections = dict()
        self.__kv_stores = dict()
        self.__message_producer = None

        """
        Setup a default root logger so that in case configuration parsing or logger hierarchy creation fails, we have
        a place to send the error messages for failures
        """
        if not self.__class__.__rootLogger:
            try:
                self.__class__.__rootLogger = logging.getLogger(const.DEFAULT_ROOT_LOGGER_NAME)
                self.__class__.__rootLogger.setLevel(logging.INFO)
                handler = StreamHandler()
                handler.setFormatter(Formatter(fmt=const.DEFAULT_LOG_FORMAT))
                self.__class__.__rootLogger.addHandler(handler)
            except Exception as e:
                raise PanoptesContextError('Could not create root logger: %s' % str(e))

        if not config_file:
            if const.CONFIG_FILE_ENVIRONMENT_VARIABLE in os.environ:
                config_file = os.environ[const.CONFIG_FILE_ENVIRONMENT_VARIABLE]
            else:
                config_file = const.DEFAULT_CONFIG_FILE_PATH

        try:
            self.__logger = self.__class__.__rootLogger
            self.__config = self._get_panoptes_config(config_file)
            self.__logger = self._get_panoptes_logger()
        except Exception as e:
            raise PanoptesContextError('Could not create PanoptesContext: %s' % str(e))

        self.__redis_pool = self.get_redis_connection(const.DEFAULT_REDIS_GROUP_NAME)

        """
        Instantiate the KeyValueStore classes provide in the list (if any) and store the reference to the objects
        created in an object dictionary called __kv_stores
        """
        if key_value_store_class_list is not None:
            for key_value_store_class in key_value_store_class_list:
                if not inspect.isclass(key_value_store_class):
                    raise PanoptesContextError('Current item in key_value_store_class_list is not a class')
                if not issubclass(key_value_store_class, PanoptesKeyValueStore):
                    raise PanoptesContextError(key_value_store_class.__name__ +
                                               " in key_value_store_class_list does not subclass PanoptesKeyValueStore")

            for key_value_store_class in key_value_store_class_list:
                self.__kv_stores[key_value_store_class.__name__] = self._get_kv_store(key_value_store_class)

        if create_message_producer:
            self._kafka_client = self._get_kafka_client()

        if create_message_producer:
            self.__message_producer = self._get_message_producer(async_message_producer)

        if create_zookeeper_client:
            self.__zookeeper_client = self._get_zookeeper_client()

    def __repr__(self):
        kv_repr = 'KV Stores: [' + ','.join([str(Obj) for Obj in self.kv_stores]) + ']'
        config_repr = 'Config: ' + repr(self.config_object)
        redis_pool_repr = 'Redis pool set: ' + str(hasattr(self, '__redis_pool'))
        message_producer_repr = 'Message producer set: ' + str(hasattr(self, '__message_producer'))
        kafka_client_repr = 'Kafka client set: ' + str(hasattr(self, '_kafka_client'))
        zk_client_repr = 'Zookeeper client set: ' + str(hasattr(self, '__zookeeper_client'))
        return '[PanoptesContext: %s, %s, %s, %s, %s, %s]' \
               % (kv_repr, config_repr, redis_pool_repr, message_producer_repr, kafka_client_repr, zk_client_repr)

    def __del__(self):
        """
        Attempt to do resource clean-up when the reference count for PanoptesContext goes to zero, namely:
            * Delete any KV stores. Then delete __kv_stores.
            * Disconnect the redis pool.
            * Stop the message producer if one was requested/created.
            * Flush and close the kafka client.
            * Stop and close the zookeeper client if one was requested/created.
        """
        try:
            del self.__kv_stores
        except AttributeError as e:
            self.logger.error('__kv_stores attribute no longer exists: %s' % str(e))
        except Exception as e:
            self.logger.error('Attempt to delete _kv_stores failed: %s' % str(e))

        if hasattr(self, '_' + self.__class__.__name__ + '__message_producer'):
            try:
                self.__message_producer.stop()
            except Exception as e:
                self.logger.error('Attempt to stop message producer failed: %s' % str(e))

        if hasattr(self, '_kafka_client'):
            try:
                self._kafka_client.close()
            except Exception as e:
                self.logger.error('Attempt to close the Kafka client failed: %s' % str(e))

        if hasattr(self, '_' + self.__class__.__name__ + '__zookeeper_client'):
            try:
                self.__zookeeper_client.stop()
                self.__zookeeper_client.close()
            except Exception as e:
                self.logger.error('Attempt to stop and close the zookeeper client failed: %s' % str(e))

    def _get_panoptes_config(self, config_file):
        """
        Returns the system wide configuration to be used with the context

        Args:
            config_file (str): The path and name of the configuration file to parse

        Returns:
            PanoptesConfig: The Panoptes Config object that holds the system wide configuration

        Raises:
            PanoptesContextError: This exception is raised if any errors happen in reading or parsing the configuration
            file
        """
        self.__logger.info('Attempting to get Panoptes Configuration')
        try:
            panoptes_config = PanoptesConfig(self.__class__.__rootLogger, config_file)
        except Exception as e:
            raise PanoptesContextError('Could not get Panoptes Configuration object: %s' % str(e))

        self.__logger.info('Got Panoptes Configuration: %s' % panoptes_config)
        return panoptes_config

    def _get_panoptes_logger(self):
        """
        Returns the logger to be used by the context

        The method attempts to guess the name of the calling module based on introspection of the stack

        Returns:
            logger(logger): A Python logger subsystem logger

        Raises:
            PanoptesContextError: This exception is raised is any errors happen trying to instantiate the logger
        """
        self.__logger.info('Attempting to get logger')
        try:
            module = get_calling_module_name()
            logger = self.__rootLogger.getChild(module)
            self.__logger.info('Got logger for module %s' % module)
            return logger
        except Exception as e:
            raise PanoptesContextError('Could not get logger: %s' % str(e))

    def _get_redis_connection(self, group, shard):
        """
        Create and return a Redis Connection for the given group

        Returns:
            redis.StrictRedis: The Redis Connection

        Raises:
            Exception: Passes through any exceptions that happen in trying to get the connection pool
        """
        redis_group = self.__config.redis_urls_by_group[group][shard]

        self.__logger.info('Attempting to connect to Redis for group "{}", shard "{}", url "{}"'.format(group, shard,
                                                                                                        redis_group))

        redis_pool = redis.BlockingConnectionPool(host=redis_group.host,
                                                  port=redis_group.port,
                                                  db=redis_group.db,
                                                  password=redis_group.password)

        redis_connection = redis.StrictRedis(connection_pool=redis_pool)

        self.__logger.info('Successfully connected to Redis for group "{}", shard "{}", url "{}"'.format(group, shard,
                                                                                                         redis_group))

        return redis_connection

    def _get_kv_store(self, cls):
        """
        Create and return a Key/Value store

        Args:
            cls (class): The class of the Panoptes Key/Value store to create

        Returns:
            PanoptesKeyValueStore: The Key/Value store object created

        Raises:
            PanoptesContextError: Passes through any exceptions that happen in trying to create the Key/Value store
        """
        self.__logger.info('Attempting to connect to KV Store "{}"'.format(cls.__name__))
        try:

            key_value_store = cls(self)
        except Exception as e:
            raise PanoptesContextError('Could not connect to KV store "{}": {}'.format(cls.__name__, repr(e)))
        self.__logger.info('Connected to KV Store "{}": {}'.format(cls.__name__, key_value_store))
        return key_value_store

    def _get_kafka_client(self):
        """
        Create and return a Kafka Client

        Returns:
            KafkaClient: The created Kafka client

        Raises:
            PanoptesContextError: Passes through any exceptions that happen in trying to create the Kafka client
        """
        # The logic of the weird check that follows is this: KafkaClient initialization can fail if there is a problem
        # connecting with even one broker. What we want to do is: succeed if the client was able to connect to even one
        # broker. So, we catch the exception and pass it through - and then check the number of brokers connected to the
        # client in the next statement (if not kafka_client.brokers) and fail if the client is not connected to any
        # broker
        self.__logger.info('Attempting to connect Kafka')
        config = self.__config
        kafka_client = None
        try:
            kafka_client = KafkaClient(config.kafka_brokers)
        except ConnectionError:
            pass

        if not kafka_client.brokers:
            raise PanoptesContextError('Could not connect to any Kafka broker from this list: %s'
                                       % config.kafka_brokers)
        self.__logger.info('Successfully connected to Kafka brokers: %s' % kafka_client.brokers)

        return kafka_client

    def _get_message_producer(self, async):
        """
        Creates and returns a Message Producer

        Args:
            async (bool): Whether the created message producer should be asynchronous or not

        Returns:
            PanoptesMessageQueueProducer: The created message producer

        Raises:
            PanoptesContextError: asses through any exceptions that happen in trying to create the message producer
        """
        self.__logger.info('Attempting to connect to message bus')
        try:
            message_producer = PanoptesMessageQueueProducer(self, async)
        except Exception as e:
            raise PanoptesContextError('Could not connect to message bus: %s' % str(e))
        self.__logger.info('Connected to message bus: %s' % message_producer)
        return message_producer

    def _get_zookeeper_client(self):
        """
        Create and return a ZooKeeper client

        Returns:
            KazooClient: The created ZooKeeper client

        Raises:
            PanoptesContextError: Passes through any exceptions that happen in trying to create the ZooKeeper client
        """
        config = self.__config
        if not config.zookeeper_servers:
            raise PanoptesContextError('No Zookeeper servers configured')

        self.__logger.info('Attempting to connect to Zookeeper with servers: %s' % ",".join(config.zookeeper_servers))
        try:
            zk = kazoo.client.KazooClient(hosts=",".join(config.zookeeper_servers))
            zk.start()
        except Exception as e:
            raise PanoptesContextError('Could not connect to Zookeeper: %s' % str(e))
        self.__logger.info('Successfully connected to Zookeeper: %s' % zk)

        return zk

    def get_kv_store(self, key_value_store_class_name):
        """
        Get the Key Value Store object associated with the provided KeyValueStore class

        Args:
            key_value_store_class_name (class): The class (not just the classname string) for which the object is \
            desired

        Returns:
            KeyValueStore: An object which can be used to set/get values from the associated Key/Value store. Raises \
            PanoptesContextError if an object of the specified class does not exist
        """
        try:
            return self.__kv_stores[key_value_store_class_name.__name__]
        except KeyError:
            raise PanoptesContextError(
                'No Key Value Store based on class %s' % key_value_store_class_name)

    def get_redis_shard_count(self, group, fallback_to_default=True):
        try:
            return len(self.__config.redis_urls_by_group[group])
        except:
            if (group != const.DEFAULT_REDIS_GROUP_NAME) and fallback_to_default:
                return len(self.__config.redis_urls_by_group[const.DEFAULT_REDIS_GROUP_NAME])
            else:
                raise

    def get_redis_connection(self, group, shard=0, fallback_to_default=True):
        """
        Returns a Redis connection for the given group and shard

        Args:
            group (str): The name of the group for which to return the Redis connection
            shard (int): The number of the shard for which to return the Redis connection
            fallback_to_default (bool): If we can't find a connection for given group, whether to fallback to the \
            'default` group name

        Returns:
            redis.StrictRedis: The Redis connection
        """
        def _inner_get_redis_connection():
            try:
                connection = self._get_redis_connection(group, shard)
                self.__redis_connections[group][shard] = connection
            except:
                if (group != const.DEFAULT_REDIS_GROUP_NAME) and fallback_to_default:
                    self.__redis_connections[group][shard] = self.redis_pool
                else:
                    raise

        if group not in self.__redis_connections:
            self.__redis_connections[group] = dict()
            _inner_get_redis_connection()
        elif shard not in self.__redis_connections[group]:
            _inner_get_redis_connection()

        return self.__redis_connections[group][shard]

    def get_lock(self, path, timeout, retries=1, identifier=None, listener=None):
        """
        A wrapper around the kazoo library lock

        Args:
            path (str): A '/' separated path for the lock
            timeout (int): in seconds. Must be a positive integer
            retries (int): how many times to try before giving up. Zero implies try forever
            identifier (str): Name to use for this lock contender. This can be useful for querying \
            to see who the current lock contenders are
            listener (callable): The callable to use to handle Zookeeper state changes

        Returns:
            kazoo.recipe.lock.Lock: lock
        """
        assert isinstance(path, str) and re.search("^/\S+", path), 'path must be a non-empty string that begins with /'
        assert isinstance(timeout, int) and (timeout > 0), 'timeout must be a positive integer'
        assert isinstance(retries, int) and (retries > -1), 'retries must be a non-negative integer'
        assert identifier and isinstance(identifier, str), 'identifier must be a non-empty string'
        assert (not listener) or callable(listener), 'listener must be a callable'

        logger = self.logger
        calling_module = get_calling_module_name(2)
        logger.info("Creating lock for module: " + calling_module + " with lock parameters: path=" + path +
                    ",timeout=" + str(timeout) + ",retries=" + str(retries) + ",identifier=" + identifier)
        try:
            lock = self.zookeeper_client.Lock(path, identifier)
        except Exception as e:
            logger.error('Failed to create lock object: %s' % str(e))
            return None

        if retries == 0:
            while True:
                logger.info('Trying to acquire lock with client id "%s" under path %s. Other contenders: %s. '
                            % (identifier, path, lock.contenders()))
                try:
                    lock.acquire(timeout=timeout)
                except LockTimeout:
                    logger.info('Timed out after %d seconds trying to acquire lock.  Retrying.' % timeout)
                except Exception as e:
                    logger.info('Error in acquiring lock: %s.  Retrying.' % str(e))
                if lock.is_acquired:
                    break
        else:
            tries = 0
            while tries < retries:
                logger.info('Trying to acquire lock with client id "%s" under path %s. Other contenders: %s. ' %
                            (identifier, path, lock.contenders()))
                try:
                    lock.acquire(timeout=timeout)
                except LockTimeout:
                    logger.info('Timed out after %d seconds trying to acquire lock. Retrying %d more times' %
                                (timeout, retries - tries - 1))
                except Exception as e:
                    logger.info('Error in acquiring lock: %s.  Retrying %d more times' % (str(e), (retries - tries)))
                if lock.is_acquired:
                    break
                tries += 1
            if not lock.is_acquired:
                logger.warn('Unable to acquire lock after %d tries' % tries)

        if lock.is_acquired:
            logger.info(
                'Lock acquired. Other contenders: %s' % lock.contenders())

            if listener:
                self.zookeeper_client.add_listener(listener)

            return lock

    @property
    def config_object(self):
        """
        The PanoptesConfig object created by the context

        Returns:
            PanoptesConfig

        """
        return self.__config

    @property
    def config_dict(self):
        """
        A **copy** of the system wide configuration

        Returns:
            ConfigObj

        """
        return self.__config.get_config()

    @property
    def logger(self):
        """
        A module-aware logger which will try and guess the right name for the calling module

        Returns:
            logging.logger

        """
        return self.__logger

    @property
    def message_producer(self):
        """
        The message producer object which can be used to send messages

        Returns:
            PanoptesMessageQueueProducer

        """
        return self.__message_producer

    @property
    def redis_pool(self):
        """
        A Redis Connection Pool

        Returns:
            RedisConnectionPool

        """
        return self.__redis_pool

    @property
    def zookeeper_client(self):
        """
        A Kazoo ZooKeeper client

        Returns:
            KazooClient

        """
        return self.__zookeeper_client

    @property
    def kafka_client(self):
        """
        A Kafka client

        Returns:
            KafkaClient

        """
        return self._kafka_client

    @property
    def kv_stores(self):
        """
        Dictionary of KV stores

        Returns:
            A dictionary of KV store name/KV store class

        """
        return self.__kv_stores
