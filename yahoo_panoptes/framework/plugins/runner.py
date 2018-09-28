"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements a 'runner' that cna take a given plugin name and type and execute it, validate and return the
results to a callback function

It also updates metadata like the plugin's last execution time and last results time
"""
import gc
import time
import weakref

from ..validators import PanoptesValidators
from ..context import PanoptesContext
from ..enrichment import PanoptesEnrichmentCacheError, PanoptesEnrichmentCache
from ..plugins.context import PanoptesPluginContext, PanoptesPluginWithEnrichmentContext
from ..plugins.manager import PanoptesPluginManager
from ..plugins.panoptes_base_plugin import PanoptesBasePluginValidators, PanoptesPluginInfoValidators
from ..utilities.key_value_store import PanoptesKeyValueStoreValidators


class PanoptesPluginRunner(object):
    """
    This class implements a 'runner' for a given plugin

    Args:
        plugin_name (str): The name of the plugin to run
        plugin_name (str): The type of the plugin
        plugin_name (PanoptesBasePlugin): The plugin class
        plugin_info_class (PanoptesPluginInfo): The plugin info class
        plugin_data (object): An optional arbitrary data object to be passed to the plugin
        panoptes_context (PanoptesContext): The Panoptes Context associated with the Plugin Agent executing the plugin
        plugin_agent_kv_store_class (class): The class of the Plugin Agent's KV store
        plugin_kv_store_class (class): The class of the Plugin's KV store
        plugin_kv_store_class (class): The class of the Plugin's secrets store
        plugin_logger_name (str): The name of the logger that the plugin should use
        plugin_result_class (class): The class of the results expected from the plugin
        results_callback (callable): The function that should called with the results generated from the plugin
    """

    def __init__(self, plugin_name, plugin_type, plugin_class, plugin_info_class, plugin_data, panoptes_context,
                 plugin_agent_kv_store_class, plugin_kv_store_class, plugin_secrets_store_class,
                 plugin_logger_name, plugin_result_class, results_callback):
        assert PanoptesValidators.valid_nonempty_string(plugin_name), 'plugin_name must be a non-empty string'
        assert PanoptesValidators.valid_nonempty_string(plugin_type), 'plugin_type must be a non-empty string'
        assert PanoptesBasePluginValidators.valid_plugin_class(
                plugin_class), 'plugin_class must be instance of PanoptesBasePlugin'
        assert PanoptesPluginInfoValidators.valid_plugin_info_class(
                plugin_info_class), 'plugin_info_class must be instance of PanoptesPluginInfo'
        assert PanoptesValidators.valid_hashable_object(plugin_data), 'plugin_data must be a valid hashable object'
        assert PanoptesKeyValueStoreValidators.valid_kv_store_class(
                plugin_kv_store_class), 'plugin_kv_store_class must be a subclass of PanoptesKeyValueStore'
        assert PanoptesKeyValueStoreValidators.valid_kv_store_class(
                plugin_secrets_store_class), 'plugin_secrets_store_class must be a subclass of PanoptesKeyValueStore'
        assert PanoptesValidators.valid_nonempty_string(plugin_logger_name), 'plugin_logger_name must be a non-empty '\
                                                                             'string'
        assert PanoptesValidators.valid_callback(results_callback), 'plugin_callback must be a callable'
        self._plugin_name = plugin_name
        self._plugin_type = plugin_type
        self._plugin_class = plugin_class
        self._plugin_info_class = plugin_info_class
        self._plugin_data = plugin_data
        self._panoptes_context = panoptes_context
        self._plugin_agent_kv_store_class = plugin_agent_kv_store_class
        self._plugin_secrets_store_class = plugin_secrets_store_class
        self._plugin_kv_store_class = plugin_kv_store_class
        self._plugin_logger_name = plugin_logger_name
        self._logger = self._panoptes_context.logger
        self._plugin_result_class = plugin_result_class
        self._results_callback = weakref.proxy(results_callback)

    def info(self, plugin, message):
        self._logger.info('[{}:{}] [{}] {}'.format(plugin.name, plugin.signature, str(plugin.data), message))

    def warn(self, plugin, message):
        self._logger.warn('[{}:{}] [{}] {}'.format(plugin.name, plugin.signature, str(plugin.data), message))

    def error(self, plugin, message, exception):
        self._logger.error(
                '[{}:{}] [{}] {}: {}'.format(plugin.name, plugin.signature, str(plugin.data), message, repr(exception)))

    def exception(self, plugin, message):
        self._logger.exception('[{}:{}] [{}] {}:'.format(plugin.name, plugin.signature, str(plugin.data), message))

    def _get_context(self, plugin):
        return PanoptesPluginContext(panoptes_context=self._panoptes_context,
                                     logger_name=self._plugin_logger_name,
                                     config=plugin.config,
                                     key_value_store=self._panoptes_context.get_kv_store(
                                         self._plugin_kv_store_class),
                                     secrets_store=self._panoptes_context.get_kv_store(
                                         self._plugin_secrets_store_class),
                                     data=self._plugin_data)

    def execute_plugin(self):
        """
        This method loads, executes and returns the results of the plugin

        The steps involved are:
          * Create a PanoptesPluginManager for the type of plugins we are interested in and then try and load the \
        named plugin
          * Check if the plugin should be executed right now based on the last execution and results time and the \
          configured execution and results caching time
          * setup a PluginContext and attempt to take a lock on the plugin name and it's signature \
        (which is hash of the plugin's config and data)
          * Try and run the plugin
          * If the plugin execution returns without errors, then update the last execution time
          * Pass the results obtained to the callback function
          * If the callback function succeeds, then update the last successful run time

        Returns:
            None
        """
        logger = self._logger
        config = self._panoptes_context.config_dict
        utc_epoch = int(time.time())

        logger.info('Attempting to execute plugin "%s"' % self._plugin_name)

        try:
            plugin_manager = PanoptesPluginManager(plugin_type=self._plugin_type,
                                                   plugin_class=self._plugin_class,
                                                   plugin_info_class=self._plugin_info_class,
                                                   plugin_data=self._plugin_data,
                                                   panoptes_context=self._panoptes_context,
                                                   kv_store_class=self._plugin_agent_kv_store_class)

            plugin = plugin_manager.getPluginByName(name=self._plugin_name, category=self._plugin_type)
        except Exception as e:
            logger.error('Error trying to load plugin "%s": %s' % (self._plugin_name, repr(e)))
            return

        if not plugin:
            logger.warn('No plugin named "%s" found in "%s"' % (
                self._plugin_name, config[self._plugin_type]['plugins_path']))
            return

        if not plugin.execute_now:
            return

        try:
            plugin_context = self._get_context(plugin)
        except:
            self.exception(plugin, 'Could not setup context for plugin')
            return

        self.info(plugin, 'Attempting to get lock for plugin "%s"' % self._plugin_name)

        try:
            lock = plugin.lock
        except:
            self.exception(plugin, 'Error in acquiring lock')
            return

        if lock is None:
            return

        self.info(plugin, 'Acquired lock')

        self.info(plugin,
                  'Going to run plugin "{}", version "{}", which last executed at {} (UTC) ({} seconds ago) and '
                  'last produced results at {} (UTC) ({} seconds ago), module mtime {} (UTC), config mtime {} ('
                  'UTC)'.format(
                          plugin.name, plugin.version, plugin.last_executed,
                          plugin.last_executed_age,
                          plugin.last_results,
                          plugin.last_results_age, plugin.moduleMtime, plugin.configMtime))

        results = None

        plugin_start_time = time.time()
        try:
            results = plugin.plugin_object.run(plugin_context)
        except:
            self.exception(plugin, 'Failed to execute plugin')
        finally:
            plugin_end_time = time.time()
            self.info(plugin, 'Ran in {:0.2f} seconds'.format(plugin_end_time - plugin_start_time))
            try:
                lock.release()
            except:
                self.exception(plugin, 'Failed to release lock for plugin')
            else:
                self.info(plugin, 'Released lock')

        if results is None:
            self.warn(plugin, 'Plugin did not return any results')
            return
        elif not isinstance(results, self._plugin_result_class):
            logger.warn(plugin, 'Plugin returned an unexpected result type: "{}"'.format(type(results).__name__))
            return

        plugin.last_executed = utc_epoch

        self.info(plugin, 'Plugin returned a result set with {} members'.format(len(results)))

        if len(results) > 0:
            # Non-empty result set - send the results to the callback function
            callback_start_time = time.time()
            try:
                self._results_callback(self._panoptes_context, results, plugin)
            except:
                self.exception(plugin, 'Results callback function failed')
                return
            finally:
                callback_end_time = time.time()
                self.info(plugin,
                          'Callback function ran in {:0.2f} seconds'.format(callback_end_time - callback_start_time))

            # If the callback was successful, then set the last results time
            # The logic behind this is: in case the callback fails, then the plugin should be re-executed again after
            # the plugin execute_frequency seconds - the execution should not be preempted by the results caching logic,
            # which depends on the last results time in the KV store

            plugin.last_results = utc_epoch

        gc_start_time = time.time()
        gc.collect()
        gc_end_time = time.time()

        logger.info(
            'GC took %.2f seconds. There are %d garbage objects.' % (gc_end_time - gc_start_time, len(gc.garbage)))


class PanoptesPluginWithEnrichmentRunner(PanoptesPluginRunner):
    def _get_context(self, plugin):

        self._enrichment = None

        if plugin.config.get('enrichment'):
            try:
                self._enrichment = PanoptesEnrichmentCache(self._panoptes_context, plugin.config, self._plugin_data)
            except Exception as e:
                raise PanoptesEnrichmentCacheError('Error while creating PanoptesEnrichmentResource object for plugin '
                                                   '{}: {}, skipping run'.format(plugin.name, repr(e)))

            if self._enrichment is None:
                raise PanoptesEnrichmentCacheError('No enrichments found for plugin {} (configured {}), '
                                                   'skipping run'.format(plugin.name, plugin.config.get('enrichment')))

        return PanoptesPluginWithEnrichmentContext(panoptes_context=self._panoptes_context,
                                                   logger_name=self._plugin_logger_name,
                                                   config=plugin.config,
                                                   key_value_store=self._panoptes_context.get_kv_store(
                                                       self._plugin_kv_store_class),
                                                   secrets_store=self._panoptes_context.get_kv_store(
                                                       self._plugin_secrets_store_class),
                                                   data=self._plugin_data,
                                                   enrichment=self._enrichment)
