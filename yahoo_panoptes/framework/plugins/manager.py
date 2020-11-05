"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements the Panoptes Plugin Manager - a class that can locate and load the plugins specified by plugin
type and plugin class
"""
import imp
import sys

from yapsy.PluginManager import PluginManager

from yahoo_panoptes.framework import const
from yahoo_panoptes.framework.plugins.panoptes_base_plugin import PanoptesPluginInfo


class PanoptesPluginManager(PluginManager):
    """
    This class implements a Plugin Manager that locates and loads all plugins of the given type

    This extends yapsy's PluginManager by filling in default values and setting PanoptesPluginInfo as the
    plugin_info_class. Also, it attaches the Panoptes Context, Key/Value Store class and any data to the
    PanoptesPluginInfo object so that these are available to the plugin when it is being executed

    Args:
        plugin_type(str): The 'type' of the plugin
        plugin_class(PanoptesBasePlugin): The class of the plugin - should be a subclass of PanoptesBasePlugin
        plugin_info_class(class): The plugin info class - defaults to PanoptesPluginInfo
        plugin_data(object): An optional, arbitrary data object to be passed to the plugin during execution
        panoptes_context(PanoptesContext): Th Panoptes Context of the Plugin Agent executing the plugin
        kv_store_class(class): The Key/Value Store class that the Plugin Info class should use
        secrets_store_class(class): The secrets store class that the Plugin Info class should use
    """

    def __init__(self, plugin_type=None, plugin_class=None, plugin_info_class=PanoptesPluginInfo, plugin_data=None,
                 panoptes_context=None, kv_store_class=None, secrets_store_class=None):
        """
        Initialize the plugin.

        Args:
            self: (todo): write your description
            plugin_type: (todo): write your description
            plugin_class: (todo): write your description
            plugin_info_class: (todo): write your description
            PanoptesPluginInfo: (todo): write your description
            plugin_data: (todo): write your description
            panoptes_context: (todo): write your description
            kv_store_class: (todo): write your description
            secrets_store_class: (str): write your description
        """
        self._plugin_type = plugin_type
        self._panoptes_context = panoptes_context
        self._kv_store_class = kv_store_class
        self._secrets_store_class = secrets_store_class
        self._data = plugin_data
        self._categories_filter = {plugin_type: plugin_class}
        self._directories_list = panoptes_context.config_dict[plugin_type][u'plugins_paths']
        self._panoptes_context.logger.debug(
            u'Starting Plugin Manager for "%s" plugins with the following configuration: %s, %s, %s' % (
                plugin_type, self._categories_filter, self._directories_list, const.PLUGIN_EXTENSION))
        super(PanoptesPluginManager, self).__init__(categories_filter=self._categories_filter,
                                                    directories_list=self._directories_list,
                                                    plugin_info_ext=const.PLUGIN_EXTENSION,
                                                    plugin_locator=None)
        self.setPluginInfoClass(plugin_info_class)
        self.collectPlugins()

    def collectPlugins(self):
        """
        This method locates and loads plugins

        Also, it attaches the Panoptes Context, Key/Value Store class and any data to the
        PanoptesPluginInfo object so that these are available to the plugin when it is being executed

        Returns:
            None
        """
        logger = self._panoptes_context.logger
        self.locatePlugins()

        # Set the name of the configuration file so that we can find the mtime for that file while deciding if the
        # the plugin should be executed
        for candidate_infofile, candidate_filepath, plugin_info in self._candidates:
            plugin_info.config_filename = candidate_infofile

        imp.acquire_lock()
        plugins = self.loadPlugins()
        imp.release_lock()
        logger.debug(u'Found %d plugins' % len(plugins))
        for plugin in plugins:
            logger.debug(
                u'Loaded plugin "%s", version "%s" of type "%s", category "%s"' % (
                    plugin.name, plugin.version, self._plugin_type, plugin.category))
            plugin.panoptes_context = self._panoptes_context
            plugin.kv_store_class = self._kv_store_class
            plugin.data = self._data

    def getPluginsOfCategory(self, category_name):
        """
        Plugin Managers compares the current list of plugins returned by Yapsy's PluginManager to the previous list
        and reschedules if the two don't match.  To guarantee that the list order is invariant in order to
        prevent unnecessary re-scheduling, we sort plugins returned by PanoptesPluginManager.getPluginsOfCategory
        by signature.

        Args:
            category_name (str): Plugin category

        Returns:
            list: Sorted list of plugins
        """
        plugins_unfiltered = super(PanoptesPluginManager, self).getPluginsOfCategory(category_name)
        return sorted(plugins_unfiltered, key=lambda obj: obj.signature)

    def unload_modules(self):
        """
        Unload all modules.

        Args:
            self: (todo): write your description
        """
        logger = self._panoptes_context.logger
        for plugin in self.getAllPlugins():
            plugin_module = plugin.plugin_object.__module__
            logger.debug(u'Deleting module: %s' % plugin_module)
            imp.acquire_lock()
            try:
                del sys.modules[plugin_module]
            except KeyError:
                logger.error('Error trying to delete module "%s" for plugin "%s" with config "%s"' % (
                    plugin_module, plugin.normalized_name, plugin.config))
            finally:
                imp.release_lock()
            del plugin.plugin_object
