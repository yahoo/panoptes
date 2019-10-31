"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module defines classes to help parse and validate the system wide configuration file
"""
from builtins import str
from builtins import object
import collections
import copy
import logging
import os
import re
import sys
import traceback
from logging.config import fileConfig

from configobj import ConfigObj, ConfigObjError, flatten_errors
from validate import Validator

from yahoo_panoptes.framework import const
from yahoo_panoptes.framework.exceptions import PanoptesBaseException
from ratelimitingfilter import RateLimitingFilter
from yahoo_panoptes.framework.validators import PanoptesValidators

# Constants
_CONFIG_SPEC_FILE = os.path.dirname(os.path.realpath(__file__)) + u'/panoptes_configspec.ini'


class PanoptesConfigurationError(PanoptesBaseException):
    """
    The exception class for Panoptes system wide configuration errors
    """
    pass


class PanoptesRedisConnectionConfiguration(object):
    """
    This class encapsulates a Redis connection
    """
    def __init__(self, host, port, db, password):
        """
        Args:
            host (str): The hostname for the Redis connection
            port (int): The port for the Redis connection
            db (int): The database number for the Redis connection
            password (str): The password for the Redis connection
        """
        self._host = host
        self._port = port
        self._db = db
        self._password = password

        self._url = u'redis://'

        if password:
            self._url += u':' + password + u'@'

        self._url += host + u':' + str(port) + u'/' + str(db)

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
            return u'redis://:**@' + self.url.rsplit(u'@', 1)[1]


Sentinel = collections.namedtuple(u'Sentinel', [u'host', u'port', u'password'])


class PanoptesRedisSentinelConnectionConfiguration(object):
    """
    This class encapsulates a Redis Sentinel connection
    """
    def __init__(self, sentinels, master_name, db, master_password=None):
        """
        Args:
            sentinels: The list of Redis Sentinels expressed as comma separated list of
                sentinel://<:password@>host:<port>
            master_name: The name of master to use while querying the Redis Sentinels
            master_password: The password to use while connecting to the master, if set
        """
        self._sentinels = list()
        self._master_name = master_name
        self._db = db
        self._master_password = master_password

        for sentinel in sentinels:
            url = re.match(r'sentinel://(?P<password>:.*@)?(?P<host>.*?)(?P<port>:\d+)', sentinel)

            if not url:
                raise ValueError(u'Sentinel host not in expected format: sentinel://<:password@>host:<port>')

            port = int(re.sub(r'^:', '', url.group(u'port')))
            password = re.sub(r'^:(.*?)@$', r'\1', url.group(u'password'))

            self._sentinels.append(
                Sentinel(
                    host=url.group(u'host'),
                    port=port,
                    password=password
                )
            )

    @property
    def sentinels(self):
        return self._sentinels

    @property
    def master_name(self):
        return self._master_name

    @property
    def db(self):
        return self._db

    @property
    def master_password(self):
        return self._master_password

    def __repr__(self):
        """
        Returns:
            str: The list of Redis Sentinels returned as a comma separated list, with the passwords (if present)
                obfuscated
        """
        return u','.join(
            [u'sentinel://{}{}{}'.format(
                u':**@' if sentinel.password else u'',
                sentinel.host, u':' + str(sentinel.port)
            ) for sentinel in self._sentinels]
        )


class PanoptesConfig(object):
    """
    This class parses and validates the system wide configuration file and sets up the logging subsystem

    Args:
        logger(yahoo_panoptes.framework.context.PanoptesContext.logger): The logger to use
        conf_file(str): The path and name of the configuration file to parse
    """
    def __init__(self, logger, conf_file=None):
        assert PanoptesValidators.valid_readable_file(conf_file), u'conf_file must be a readable file'
        self._logger = logger

        logger.info(u'Using configuration file: ' + conf_file)

        try:
            config = ConfigObj(conf_file, configspec=_CONFIG_SPEC_FILE, interpolation=u'template', file_error=True)
        except (ConfigObjError, IOError):
            raise

        validator = Validator()
        result = config.validate(validator, preserve_errors=True)

        if result is not True:
            errors = u''
            for (section_list, key, error) in flatten_errors(config, result):
                if key is None:
                    errors += u'Section(s) ' + u','.join(section_list) + u' are missing\n'
                else:
                    errors += u'The "' + key + u'" key in section "' + u','\
                        .join(section_list) + u'" failed validation\n'
            raise SyntaxError(u'Error parsing the configuration file: %s' % errors)

        kafka_config = config[u'kafka']

        if kafka_config[u'publish_to_site_topic'] is False and \
                kafka_config[u'publish_to_global_topic'] is False:
            raise PanoptesConfigurationError(
                u'Panoptes metrics will not be published to the message queue. Set atleast one of '
                u'`publish_to_site_topic` or `publish_to_global_topic` (or both) to true in the '
                u'side wide configuration file'
            )

        # If the settings aren't set to publish panoptes metrics to both site and global topics at the same time
        #  Panoptes needs to check the consumers are consuming from the correct topic
        if not (kafka_config[u'publish_to_site_topic'] and kafka_config[u'consume_from_site_topic']):
            if ((kafka_config[u'publish_to_site_topic'] and not kafka_config[u'consume_from_site_topic']) or
                    (kafka_config[u'publish_to_global_topic'] and kafka_config[u'consume_from_site_topic'])):
                raise PanoptesConfigurationError(u'Panoptes metrics will not be consumed. The consumer is set to '
                                                 u'consume from the incorrect topic. Change either `publish_to_'
                                                 u'site_topic` or `publish_to_global_topic` in the site wide '
                                                 u'configuration file')

        self._setup_logging(config)

        self._get_sites(config)
        logger.info(u'Got list of sites: %s' % self._sites)

        self._get_redis_urls(config)
        logger.info(u'Got Redis URLs "%s"' % self.redis_urls)
        logger.info(u'Got Redis URLs by group "%s"' % self.redis_urls_by_group)
        logger.info(u'Got Redis URLs by namespace "%s"' % self.redis_urls_by_namespace)

        self._get_zookeeper_servers(config)
        logger.info(u'Got list of ZooKeeper servers: %s' % self._zookeeper_servers)

        self._get_kafka_brokers(config)
        logger.info(u'Got list of Kafka brokers: %s' % self._kafka_brokers)

        self._get_snmp_defaults(config)
        logger.info(u'Got SNMP defaults: %s' % self._snmp_defaults)

        self._get_x509_defaults(config)
        logger.info(u'Got x509 defaults: %s' % self._x509_defaults)

        for plugin_type in const.PLUGIN_TYPES:
            if config[plugin_type] is None:
                raise Exception(u'No configuration section for %s plugins' % plugin_type)

            plugins_paths = config[plugin_type][u'plugins_paths']

            for plugins_path in plugins_paths:
                if not os.path.isdir(plugins_path):
                    raise Exception(u'%s plugins path "%s" does not exist or is not accessible' % (plugin_type,
                                                                                                   plugins_path))

                if not os.access(plugins_path, os.R_OK):
                    raise Exception(u'%s plugins path "%s" is not accessible' % (plugin_type, plugins_path))

            logger.info(plugin_type + u' plugins paths: ' + str(plugins_paths))

        self._config = config

    def _setup_logging(self, config):
        log_config_file = config[u'log'][u'config_file']
        self._logger.info(u'Logging configuration file: ' + log_config_file)

        try:
            logging.config.fileConfig(log_config_file)
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, limit=2, file=sys.stderr)
            raise PanoptesConfigurationError(
                    u'Could not instantiate logger with logging configuration provided in file "%s": (%s) %s' % (
                        log_config_file, exc_type, exc_value))

        # Create a filter to rate limit logs so that a misconfiguration or failure does not make the disk I/O go
        # beserk or fill up the disk space. We do this in code instead if configuration for two reasons:
        # - It enforces a filter on every handler, so no chance of messing them up in configuration
        # - We use fileConfig (nof dictConfig) to setup our logging and fileConfig does not support filter configuration
        throttle = RateLimitingFilter(rate=config[u'log'][u'rate'], per=config[u'log'][u'per'],
                                      burst=config[u'log'][u'burst'])

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

        for group_name in config[u'redis']:
            redis_urls_by_group[group_name] = list()
            group = config[u'redis'][group_name]

            namespace = group[u'namespace']
            if namespace in redis_urls_by_namespace:
                raise ValueError(u'Invalid Redis configuration: namespace "{}" is configured multiple times'.
                                 format(namespace))

            redis_urls_by_namespace[namespace] = list()
            for shard_name in group[u'shards']:
                shard = group[u'shards'][shard_name]
                if u'host' in shard and shard[u'host'] is not None:
                    if u'sentinels' in shard and shard[u'sentinels'] is not None:
                        raise ValueError(
                            u'Invalid Redis configuration: '
                            u'shard "{}" in group "{}" has both "host" and "sentinels" configured'.format(shard_name,
                                                                                                          group_name)
                        )
                    else:
                        connection = PanoptesRedisConnectionConfiguration(
                            host=shard[u'host'],
                            port=shard[u'port'],
                            db=shard[u'db'],
                            password=shard[u'password']
                        )
                elif u'sentinels' in shard and shard[u'sentinels'] is not None:
                    try:
                        connection = PanoptesRedisSentinelConnectionConfiguration(
                            sentinels=shard[u'sentinels'],
                            master_name=shard[u'master_name'],
                            db=shard[u'db'],
                            master_password=shard.get(u'password', None)
                        )
                    except ValueError:
                        raise ValueError(
                            u'Invalid Redis configuration: '
                            u'shard "{}" in group "{}" has invalid sentinel configuration'.format(shard_name,
                                                                                                  group_name)
                        )
                else:
                    raise ValueError(
                        u'Invalid Redis configuration: '
                        u'shard "{}" in group "{}" has neither "host" or "sentinels" configured'.format(shard_name,
                                                                                                        group_name)
                    )

                redis_urls.append(connection)
                redis_urls_by_group[group_name].append(connection)
                redis_urls_by_namespace[namespace].append(connection)

        if const.DEFAULT_REDIS_GROUP_NAME not in redis_urls_by_group:
            raise ValueError(u'Invalid Redis configuration: no "{}" group found. Configuration has the following '
                             u'groups: {}'.format(const.DEFAULT_REDIS_GROUP_NAME, list(redis_urls_by_group.keys())))
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
        for zookeeper_server_id in config[u'zookeeper'][u'servers']:
            zookeeper_server = config[u'zookeeper'][u'servers'][zookeeper_server_id][u'host'] + u':' + \
                               str(config[u'zookeeper'][u'servers'][zookeeper_server_id][u'port'])
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
        for kafka_broker_id in config[u'kafka'][u'brokers']:
            kafka_broker = config[u'kafka'][u'brokers'][kafka_broker_id][u'host'] + u':' + \
                           str(config[u'kafka'][u'brokers'][kafka_broker_id][u'port'])

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
        for site in config[u'main'][u'sites']:
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
        self._snmp_defaults = config[u'snmp'].copy()

    def _get_x509_defaults(self, config):
        """
        This method parses and stores the x509 defaults to be used by the system

        Args:
            config (ConfigObj): The set that holds the x509 defaults.

        Returns:
                None
        """
        self._x509_defaults = config[u'x509'].copy()

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

    @property
    def x509_defaults(self):
        """
        The x509 defaults set up in panoptes_configspec.ini

        Returns:
             dict: The X509 defaults to be used by the system.
        """
        return self._x509_defaults

    def __repr__(self):
        config = self.get_config()

        if config is None:
            return

        # Mask redis passwords
        for group_name in config[u'redis']:
            group = config[u'redis'][group_name]
            for shard_name in group[u'shards']:
                shard = group[u'shards'][shard_name]
                if u'password' in shard:
                    if shard[u'password'] != u'':
                        shard[u'password'] = u'**'

        # Mask community string
        if u'community' in config[u'snmp']:
            config[u'snmp'][u'community'] = u'**'

        return repr(config)
