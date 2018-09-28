"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module defines classes related to all Panoptes Plugins
"""
import abc
import hashlib
import json
import os
import time
from collections import defaultdict

import six
from cached_property import threaded_cached_property
from yapsy.IPlugin import IPlugin
from yapsy.PluginInfo import PluginInfo

from .. import const
from ..exceptions import PanoptesBaseException
from ..validators import PanoptesValidators
from ..utilities.helpers import get_client_id, get_module_mtime, normalize_plugin_name
from ..utilities.lock import PanoptesLock
from .context import PanoptesPluginContext


class PanoptesPluginConfigurationError(PanoptesBaseException):
    """
    The exception class for Panoptes Plugin configuration errors
    """
    pass


class PanoptesPluginRuntimeError(PanoptesBaseException):
    """
    The exception class for Panoptes Plugin runtime errors
    """
    pass


class PanoptesPluginInfoValidators(object):
    @classmethod
    def valid_plugin_info_class(cls, plugin_info_class):
        """
        valid_plugin_info_class(cls, plugin_info_class)

        Checks if the passed class is a subclass of PanoptesPluginInfo

        Args:
            plugin_info_class (cls): The class to check

        Returns:
            bool: True if the class is not null and is an subclass of PanoptesPluginInfo
        """
        return plugin_info_class and issubclass(plugin_info_class, PanoptesPluginInfo)


class PanoptesPluginInfo(PluginInfo):
    """
    This class represents all the metadata associated with a plugin such as:
        - Configuration read from the plugin definition file
        - The mtime of the plugin module and the configuration
        - The last executed time and age
        - The last results time and age
        - The Panoptes Context associated with the Plugin Agent that is executing this plugin
        - The Key/Value store associated that the plugin can use
        - Whether this plugin should be executed right now
        - The 'signature' of the plugin - which is a hash of it's config and data
        - The data to be passed to the plugin
        - A lock that the plugin can acquire

    Args:
        plugin_name(str): The name of the plugin
        plugin_path(str): The 'path' of the plugin - either the absolute path of the directory that contains the plugin
        module or the path of the module file without the '.py' extension
    """

    def __init__(self, plugin_name, plugin_path):
        super(PanoptesPluginInfo, self).__init__(plugin_name, plugin_path)
        self._normalized_name = normalize_plugin_name(plugin_name)
        self._panoptes_context = None
        self._kv_store_class = None
        self._last_executed = None
        self._last_executed_key = None
        self._last_results = None
        self._last_results_key = None
        self._config_filename = None
        self._data = None
        self._lock = None

    def __repr__(self):
        return 'PanoptesPluginInfo: ' \
               'Normalized name: {}, '\
               'Config file: {}, '\
               'Panoptes context: {}, '\
               'KV store class: {}, '\
               'Last executed timestamp: {}, '\
               'Last executed key: {}, '\
               'Last results timestamp: {}, '\
               'Last results key: {}, '\
               'Data: {}, '\
               'Lock: {}'.format(self.normalized_name,
                                 str(self.config_filename),
                                 repr(self.panoptes_context) if self._panoptes_context else 'None',
                                 str(self.kv_store_class.__name__),
                                 str(self.last_executed),
                                 str(self.last_executed_key),
                                 str(self.last_results),
                                 str(self.last_results_key),
                                 'Data object passed' if (self.data is not None) else 'None',
                                 'Lock is set' if (self.lock is not None) else 'None')

    def _get_key(self, suffix):
        """
        Returns the key that should be used to get the plugin's metadata from the Key/Value store

        The key returned contains the normalized name signature of the plugin (see below for their definition)

        Args:
            suffix(str): The suffix is the specific piece of metadata to lookup (e.g. 'last_results'

        Returns:
            str: The lookup key that should be used

        """
        return ('plugin_metadata:' +
                self._normalized_name + ':' +
                self.signature + ':' +
                suffix)

    @threaded_cached_property
    def normalized_name(self):
        """
        The normalized named of the plugin

        Normalized name represents a 'safe' name that can be used throughout the Panoptes system

        Returns:
            str: The normalized plugin name
        """
        return self._normalized_name

    @threaded_cached_property
    def normalized_category(self):
        """
        The normalized named of the plugin's category

        Normalized name represents a 'safe' name that can be used throughout the Panoptes system

        Returns:
            str: The normalized plugin name
        """
        return normalize_plugin_name(self.category)

    @property
    def moduleMtime(self):
        """
        The plugin module's mtime in Unix Epoch format

        Returns:
            int: The plugin module's mtime in Unix Epoch format

        Raises:
            PanoptesPluginConfigurationError: This exception is raised if the plugin module's mtime cannot be determined
        """
        try:
            return get_module_mtime(self.path)
        except Exception as e:
            raise PanoptesPluginConfigurationError(
                'Could not get mtime for module file/directory "%s" for plugin "%s": %s' % (
                    self.path, self.name, str(e)))

    @property
    def configMtime(self):
        """
        The plugin config's time in Unix Epoch format

        Returns:
            int: The plugin config's mtime in Unix Epoch format

        Raises:
            PanoptesPluginConfigurationError: This exception is raised if the plugin config's mtime cannot be determined
        """
        try:
            return int(os.path.getmtime(self._config_filename))
        except Exception as e:
            raise PanoptesPluginConfigurationError(
                'Could not get mtime for configuration file "%s" for plugin "%s": %s' % (
                    self._config_filename, self.name, str(e)))

    @property
    def execute_frequency(self):
        """
        The execution frequency of the plugin in seconds specified in the plugin's config file

        Returns zero in case the execution frequency is not specified or is not an integer

        Returns:
            int: The execution frequency of the plugin in seconds
        """
        try:
            return self.details.getint('main', 'execute_frequency')
        except:
            return 0

    @property
    def results_cache_age(self):
        """
        The results cache age of the plugin in seconds specified in the plugin's config file

        Returns zero in case the results cache agey is not specified or is not an integer

        Returns:
            int: The results cache ageof the plugin in seconds
        """
        try:
            return self.details.getint('main', 'results_cache_age')
        except:
            return 0

    @threaded_cached_property
    def config(self):
        """
        The configuration of the plugin

        This method folds the configparser which holds the plugin's parsed configuration file into a dictionary

        Returns:
            dict: The configuration of the plugin in sorted order.

        """
        config = defaultdict(dict)
        for section in self.details.sections():
            for option in self.details.options(section):
                config[section][option] = self.details.get(section, option)

        return config

    @property
    def panoptes_context(self):
        """
        The Panoptes Context associated with the Plugin Agent executing the plugin

        Returns:
            PanoptesContext: The Panoptes Context associated with the Plugin Agent executing the plugin

        Raises:
            PanoptesPluginConfigurationError: If no Panoptes Context is associated with the plugin
        """
        if not self._panoptes_context:
            raise PanoptesPluginConfigurationError('No Panoptes Context associated with plugin "%s"' % self.name)
        return self._panoptes_context

    @panoptes_context.setter
    def panoptes_context(self, panoptes_context):
        """
        Sets the Panoptes Context associated with this plugin instance

        Args:
            panoptes_context (PanoptesContext): The Panoptes Context for with the Plugin Agent executing the plugin

        Returns:
            None
        """
        self._panoptes_context = panoptes_context

    @property
    def kv_store_class(self):
        """
        The Key/Value store class that the plugin instance is using

        Returns:
            class: The Key/Value store class that the plugin instance is using

        """
        return self._kv_store_class

    @kv_store_class.setter
    def kv_store_class(self, kv_store_class):
        """
        Sets the Key/Value store class that the plugin instance should use

        Args:
            kv_store_class (PanoptesKeyValueStore): The Key/Value store class that the plugin instance should use

        Returns:
            None
        """
        self._kv_store_class = kv_store_class

    @property
    def metadata_kv_store(self):
        """
        The Key/Value store class that the plugin info object is using

        Returns:
            PanoptesKeyValueStore: The Key/Value store class that the plugin info object is using
        """
        return self._panoptes_context.get_kv_store(self.kv_store_class)

    @property
    def last_executed_key(self):
        """
        The key to lookup to get the last executed time for the plugin

        Returns:
            str: he key to lookup to get the last executed time for the plugin
        """
        return self._get_key('last_executed')

    @property
    def last_executed(self):
        """
        The last execution time of the plugin in Unix Epoch format

        Returns zero if the plugin has never been executed or there is an error in fetching the last execution time

        Returns:
            int: The last execution time of the plugin in Unix Epoch format
        """
        if self._last_executed is not None:
            return self._last_executed

        try:
            self._last_executed = int(self.metadata_kv_store.get(self.last_executed_key))
        except:
            self._last_executed = 0

        return self._last_executed

    @last_executed.setter
    def last_executed(self, timestamp):
        """
        Sets the last execution time of the plugin

        Args:
            timestamp (int): The last execution time of the plugin in Unix Epoch format

        Returns:
            None
        """
        try:
            self.metadata_kv_store.set(self.last_executed_key, str(timestamp),
                                       expire=const.PLUGIN_AGENT_PLUGIN_TIMESTAMPS_EXPIRE)
        except Exception as exp:
            self.panoptes_context.logger.error('Could not store value for last successful execution time for plugin '
                                               '"%s": %s' % (self.name, str(exp)))

    @property
    def last_executed_age(self):
        """
        The last execution age (in seconds) of the plugin

        Returns:
            int: The last execution age (in seconds) of the plugin
        """
        return int(time.time()) - self.last_executed

    @property
    def last_results_key(self):
        """
        The key to lookup to get the last results time for the plugin

        Returns:
            str: he key to lookup to get the last results time for the plugin
        """
        return self._get_key('last_results')

    @property
    def last_results(self):
        """
        The last results time of the plugin in Unix Epoch format

        Returns zero if the plugin has never been produced results or there is an error in fetching the last results
        time

        Returns:
            int: The last results time of the plugin in Unix Epoch format
        """
        if self._last_results is not None:
            return self._last_results

        try:
            self._last_results = int(self.metadata_kv_store.get(self.last_results_key))
        except:
            self._last_results = 0

        return self._last_results

    @last_results.setter
    def last_results(self, timestamp):
        """
        Sets the last results time of the plugin

        Args:
            timestamp (int): The last results time of the plugin in Unix Epoch format

        Returns:
            None
        """
        try:
            self.metadata_kv_store.set(self.last_results_key, str(timestamp),
                                       expire=const.PLUGIN_AGENT_PLUGIN_TIMESTAMPS_EXPIRE)
        except Exception as exp:
            self.panoptes_context.logger.error('Could not store value for last successful results time for plugin '
                                               '"%s": %s' % (self.name, str(exp)))

    @property
    def last_results_age(self):
        """
        The last results age (in seconds) of the plugin

        Returns:
            int: The last results age (in seconds) of the plugin
        """
        return int(time.time()) - self.last_results

    @property
    def execute_now(self):
        """
        Decides whether a plugin should be executed right now or not

        True if the plugin's last execution age is greater than the execution frequency OR it's module/config mtimes are
        greater than the last execution time

        The motivation is to ensure that a plugin is re-run as soon as possible after it's module or config have changed

        Similar check for plugin results

        Returns:
            bool: True if the plugin should be executed now - false otherwise
        """
        logger = self.panoptes_context.logger
        skew = self.panoptes_context.config_dict['main']['plugins_skew']

        if self.last_executed_age + skew < self.execute_frequency:
            if (self.last_executed > self.moduleMtime) and (
                        self.last_executed > self.configMtime):
                logger.info('Skipping execution of plugin "%s" since it was last executed at %s (UTC), which is less '
                            'then %s seconds ago while the plugin execution frequency is set to %s seconds'
                            % (self.name, self.last_executed, self.last_executed_age,
                               self.execute_frequency))
                return False

        if self.last_results_age + skew < self.results_cache_age:
            if (self.last_results > self.moduleMtime) and (
                        self.last_results > self.configMtime):
                logger.info('Skipping execution of plugin "%s" since it last produced a resource set at %s (UTC), '
                            'which is less then %s seconds ago while the results cache age is set to %s seconds'
                            % (self.name, self.last_results, self.last_results_age,
                               self.results_cache_age))
                return False

        return True

    @property
    def config_filename(self):
        """
        The name of configuration file associated with the plugin

        Returns:
            str: The name of configuration file associated with the plugin
        """
        return self._config_filename

    @config_filename.setter
    def config_filename(self, filename):
        """
        Sets name of configuration file associated with the plugin - note that setting this does not change/reload the
        configuration

        Args:
            filename (str): The name of configuration file associated with the plugin

        Returns:
            None
        """
        assert PanoptesValidators.valid_nonempty_string(filename), 'filename must be a non empty string'
        self._config_filename = filename

    @property
    def data(self):
        """
        The data associated with the plugin info object - this is passed to the plugin when it is being executed

        Returns:
            object: The data associated with the plugin info object
        """
        return self._data

    @data.setter
    def data(self, data):
        """
        Sets the data to be passed to the plugin when it is being executed

        Args:
            data (object): The data to be passed to the plugin when it is being executed

        Returns:
            None
        """
        assert PanoptesValidators.valid_hashable_object(data), 'plugin_data must be a valid hashable object'
        self._data = data

    @threaded_cached_property
    def signature(self):
        """
        The 'signature' of a plugin instance to uniquely identify it

        Returns:
            str: A hash of the plugin's config and data

        """
        return hashlib.md5(json.dumps(self.config, sort_keys=True) + str(hash(self.data))).hexdigest()

    @property
    def lock(self):
        """
        Attempts to get a lock for unique plugin instance

        Returns:
            KazooLock: The lock object

        Raises:
            Exception: Passes through any exceptions raised while trying to get a lock
        """
        if self._lock:
            return self._lock

        logger = self.panoptes_context.logger

        client_id = get_client_id(const.PLUGIN_CLIENT_ID_PREFIX)
        """
        We acquire a lock for a plugin under it's name and the hash of it's configuration and data. The motivation is
        that multiple instance of the plugins are allowed to execute in parallel - as long as they are using they acting
        on different resources or using different configurations
        """
        lock_path = '/'.join([const.PLUGIN_AGENT_LOCK_PATH,
                              self.normalized_category,
                              '/plugins/lock/',
                              self.normalized_name,
                              self.signature])

        logger.debug('Attempting to get lock for plugin "%s", with lock path "%s" and identifier "%s" in %d seconds' % (
            self.name, lock_path, client_id, const.PLUGIN_AGENT_LOCK_ACQUIRE_TIMEOUT))

        self._lock = PanoptesLock(context=self.panoptes_context,
                                  path=lock_path,
                                  timeout=const.PLUGIN_AGENT_LOCK_ACQUIRE_TIMEOUT,
                                  retries=1,
                                  identifier=client_id)

        return self._lock


class PanoptesBasePluginValidators(object):
    @classmethod
    def valid_plugin_class(cls, plugin_class):
        """
        valid_plugin_class(cls, plugin_class)

        Checks if the passed class is a subclass of PanoptesBasePlugin

        Args:
            plugin_class (cls): The class to check

        Returns:
            bool: True if the class is not null and is an subclass of PanoptesBasePlugin
        """
        return plugin_class and issubclass(plugin_class, PanoptesBasePlugin)


@six.add_metaclass(abc.ABCMeta)
class PanoptesBasePlugin(IPlugin):
    """
    The base class for all Panoptes plugins

    Every plugin in Panoptes needs to implement the 'run' method

    The Panoptes plugin system is based on `yapsy <http://yapsy.sourceforge.net/>`_

    """

    @abc.abstractmethod
    def run(self, context):
        """
        The entry point for every plugin in the Panoptes system

        Args:
            context (PanoptesPluginContext):  The context of the plugin contains the plugins configuration, the logger \
            it should use and a client for the system wide plugins KV store

        Returns:
            varies: The output of a plugin depends on the type of the plugin

        Raises:
            PanoptesPluginConfigurationError: If the plugin cannot proceed with the configuration provided to it
            through the context, it should raise a PanoptesPluginConfigurationError
        """
        pass
