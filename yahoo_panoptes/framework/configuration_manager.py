"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module defines classes to help parse and validate the system wide configuration file
"""
import copy
import logging
import os
import sys
import traceback
from logging.config import fileConfig

from configobj import ConfigObj, ConfigObjError, flatten_errors
from validate import Validator

from . import const
from .exceptions import PanoptesBaseException
from .utilities.ratelimitingfilter import RateLimitingFilter
from .validators import PanoptesValidators

# Constants
_CONFIG_SPEC_FILE = os.path.dirname(os.path.realpath(__file__)) + '/panoptes_configspec.ini'


class PanoptesConfigurationError(PanoptesBaseException):
    """
    The exception class for Panoptes system wide configuration errors
    """
    pass


class PanoptesRedisConnectionConfiguration(object):
    def __init__(self, group, namespace, shard, host, port, db, password):
        self._group = group
        self._namespace = namespace
        self._shard = shard
        self._host = host
        self._port = port
        self._db = db
        self._password = password

        self._url = 'redis://'

        if password:
            self._url += ':' + password + '@'

        self._url += host + ':' + str(port) + '/' + str(db)

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def db(self):
        return self._db

    @property
    def password(self):
        return self._password

    @property
    def url(self):
        return self._url

    def __repr__(self):
        if not self._password:
            return self.url
        else:
            return 'redis://:**@' + self.url.rsplit('@', 1)[1]


class PanoptesConfig(object):
    """
    This class parses and validates the system wide configuration file and sets up the logging subsystem

    Args:
        logger(yahoo_panoptes.framework.context.PanoptesContext.logger): The logger to use
        conf_file(str): The path and name of the configuration file to parse
    """
    def __init__(self, logger, conf_file=None):
        assert PanoptesValidators.valid_readable_file(conf_file), 'conf_file must be a readable file'
        self._logger = logger

        logger.info('Using configuration file: ' + conf_file)

        try:
            config = ConfigObj(conf_file, configspec=_CONFIG_SPEC_FILE, interpolation='template', file_error=True)
        except (ConfigObjError, IOError):
            raise

        validator = Validator()
        result = config.validate(validator, preserve_errors=True)

        if result is not True:
            errors = ''
            for (section_list, key, error) in flatten_errors(config, result):
                errors += 'The "' + key + '" key in section "' + ','.join(section_list) + '" failed validation\n'
            raise SyntaxError('Error parsing the configuration file: %s' % errors)

        self._setup_logging(config)

        self._get_sites(config)
        logger.info('Got list of sites: %s' % self._sites)

        self._get_redis_urls(config)
        logger.info('Got Redis URLs "%s"' % self.redis_urls)
        logger.info('Got Redis URLs by group "%s"' % self.redis_urls_by_group)
        logger.info('Got Redis URLs by namespace "%s"' % self.redis_urls_by_namespace)

        self._get_zookeeper_servers(config)
        logger.info('Got list of ZooKeeper servers: %s' % self._zookeeper_servers)

        self._get_kafka_brokers(config)
        logger.info('Got list of Kafka brokers: %s' % self._kafka_brokers)

        self._get_snmp_defaults(config)
        logger.info('Got SNMP defaults: %s' % self._snmp_defaults)

        for plugin_type in const.PLUGIN_TYPES:
            plugins_path = config[plugin_type]['plugins_path']

            if not PanoptesValidators.valid_readable_path(plugins_path):
                raise PanoptesConfigurationError(
                    "Specified plugin path is not readable: %s" % plugins_path
                )

            logger.info(plugin_type + ' plugins path: ' + plugins_path)

        self._config = config

    def _setup_logging(self, config):
        log_config_file = config['log']['config_file']
        self._logger.info('Logging configuration file: ' + log_config_file)

        try:
            logging.config.fileConfig(log_config_file)
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, limit=2, file=sys.stderr)
            raise PanoptesConfigurationError(
                    'Could not instantiate logger with logging configuration provided in file "%s": (%s) %s' % (
                        log_config_file, exc_type, exc_value))

        # Create a filter to rate limit logs so that a misconfiguration or failure does not make the disk I/O go
        # beserk or fill up the disk space. We do this in code instead if configuration for two reasons:
        # - It enforces a filter on every handler, so no chance of messing them up in configuration
        # - We use fileConfig (nof dictConfig) to setup our logging and fileConfig does not support filter configuration
        throttle = RateLimitingFilter(rate=config['log']['rate'], per=config['log']['per'],
                                      burst=config['log']['burst'])

        # Apply the filter to all handlers. Note that this would be a shared filter across ALL logs generated by this
        # process and thus the rate/burst should be set appropriately high
        for handler in logging._handlerList:
            # _handlerList is a list of weakrefs, so the object returned has to be dereferenced
            handler().addFilter(throttle)

    def _get_redis_urls(self, config):
        """
        This method constructs and stores a Redis URL (of the format "redis://host:port/db")

        Args:
            config (ConfigObj): The ConfigObj that holds the configuration

        Returns:
            None
        """
        redis_urls = list()
        redis_urls_by_group = dict()
        redis_urls_by_namespace = dict()

        for group_name in config['redis']:
            redis_urls_by_group[group_name] = list()
            group = config['redis'][group_name]

            namespace = group['namespace']
            if namespace in redis_urls_by_namespace:
                raise ValueError('Invalid Redis configuration: namespace "{}" is configured multiple times'.
                                 format(namespace))

            redis_urls_by_namespace[namespace] = list()
            for shard_name in group['shards']:
                shard = group['shards'][shard_name]
                url = PanoptesRedisConnectionConfiguration(group=group_name, namespace=namespace, shard=shard_name,
                                                           host=shard['host'], port=shard['port'],
                                                           db=shard['db'], password=shard['password'])

                redis_urls.append(url)
                redis_urls_by_group[group_name].append(url)
                redis_urls_by_namespace[namespace].append(url)

        if const.DEFAULT_REDIS_GROUP_NAME not in redis_urls_by_group:
            raise ValueError('Invalid Redis configuration: no "{}" group found. Configuration has the following '
                             'groups: {}'.format(const.DEFAULT_REDIS_GROUP_NAME, redis_urls_by_group.keys()))
        self._redis_urls = redis_urls
        self._redis_urls_by_group = redis_urls_by_group
        self._redis_urls_by_namespace = redis_urls_by_namespace

    def _get_zookeeper_servers(self, config):
        """
        This method parses and stores the ZooKeeper servers to be used by the system

        Args:
            config (ConfigObj): The ConfigObj that holds the configuration

        Returns:
            None
        """
        zookeeper_servers = set()
        for zookeeper_server_id in config['zookeeper']['servers']:
            zookeeper_server = config['zookeeper']['servers'][zookeeper_server_id]['host'] + ':' + \
                               str(config['zookeeper']['servers'][zookeeper_server_id]['port'])
            zookeeper_servers.add(zookeeper_server)

        self._zookeeper_servers = zookeeper_servers

    def _get_kafka_brokers(self, config):
        """
        This method parses and stores the Kafka brokers to be used by the system

        Args:
            config (ConfigObj): The ConfigObj that holds the configuration

        Returns:
            None
        """
        kafka_brokers = set()
        for kafka_broker_id in config['kafka']['brokers']:
            kafka_broker = config['kafka']['brokers'][kafka_broker_id]['host'] + ':' + \
                           str(config['kafka']['brokers'][kafka_broker_id]['port'])

            kafka_brokers.add(kafka_broker)

        self._kafka_brokers = kafka_brokers

    def _get_sites(self, config):
        """
        This method parses and stores the sites to be used by the system

        Args:
            config (ConfigObj): The set that holds the sites.

        Returns:
                None
        """
        sites = set()
        for site in config['main']['sites']:
            sites.add(site)

        self._sites = sites

    def _get_snmp_defaults(self, config):
        """
        This method parses and stores the SNMP defaults to be used by the system

        Args:
            config (ConfigObj): The set that holds the SNMP defaults.

        Returns:
                None
        """
        self._snmp_defaults = config['snmp'].copy()

    def get_config(self):
        """
        This method returns a *copy* of the configuration

        Returns:
            ConfigObj: A copy of the configuration
        """
        return copy.deepcopy(self._config)

    @property
    def redis_urls(self):
        """
        The Redis URLs to be used by the system

        Returns:
            list: The Redis URLs to be used by the system
        """
        return self._redis_urls

    @property
    def redis_urls_by_group(self):
        return self._redis_urls_by_group

    @property
    def redis_urls_by_namespace(self):
        return self._redis_urls_by_namespace

    @property
    def zookeeper_servers(self):
        """
        The set of ZooKeeper servers to be used by the system

        Returns:
            set: The set of ZooKeeper servers to be used by the system
        """
        return self._zookeeper_servers

    @property
    def kafka_brokers(self):
        """
        The set of Kafka Brokers to be used by the system

        Returns:
             set: The set of Kafka Brokers to be used by the system
        """
        return self._kafka_brokers

    @property
    def sites(self):
        """
        The set of sites to be used by the system

        Returns:
            set: The set of sites to be used by the system.

        """
        return self._sites

    @property
    def snmp_defaults(self):
        """
        The SNMP defaults to be used by the system

        Returns:
            dict: The SNMP defaults be used by the system.

        """
        return self._snmp_defaults

    def __repr__(self):
        config = self.get_config()

        # Mask redis passwords
        for group_name in config['redis']:
            group = config['redis'][group_name]
            for shard_name in group['shards']:
                shard = group['shards'][shard_name]
                if 'password' in shard:
                    if shard['password'] != '':
                        shard['password'] = '**'

        # Mask community string
        if 'community' in config['snmp']:
            config['snmp']['community'] = '**'

        return repr(config)
