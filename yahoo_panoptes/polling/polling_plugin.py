"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module declares an abstract base class for Panoptes Polling Plugins - and related helpers

A Polling Plugin's role is to return a collection of metrics that have been monitored by the system
"""
import abc

import six

from ..framework.exceptions import PanoptesBaseException
from ..framework.metrics import PanoptesMetricSet
from ..framework.plugins.context import PanoptesPluginContext
from ..framework.plugins.panoptes_base_plugin import PanoptesBasePlugin, PanoptesPluginInfo, \
    PanoptesPluginConfigurationError


class PanoptesPollingPluginError(PanoptesBaseException):
    """
    The class for Polling Plugin runtime errors
    """
    pass


class PanoptesPollingPluginConfigurationError(PanoptesPluginConfigurationError):
    """
    The class for Polling Plugin configuration errors
    """
    pass


class PanoptesPollingPluginInfo(PanoptesPluginInfo):
    """
    This class extends the PanoptesPluginInfo to contain the resource_filter associated with a Polling Plugin

    Args:
        plugin_name(str): The name of the plugin
        plugin_path(str): The 'path' of the plugin - either the absolute path of the directory that contains the plugin\
        module or the path of the module file without the '.py' extension
    """
    def __init__(self, plugin_name, plugin_path):
        super(PanoptesPollingPluginInfo, self).__init__(plugin_name, plugin_path)

    @property
    def resource_filter(self):
        """
        The Resource DSL query to be applied to resources passed to this plugin

        Returns:
            str: The Resource DSL filter specified in the plugin configuration file

        Raises:
            PanoptesPollingPluginConfigurationError: This error if raised if no Resource DSL filter is specified in the\
            plugin configuration file
        """
        try:
            return self.details.get('main', 'resource_filter')
        except Exception as e:
            raise PanoptesPollingPluginConfigurationError(
                'No resource filter specified or error in parsing resource filter for plugin %s: %s' % (
                    self.name, str(e)))


@six.add_metaclass(abc.ABCMeta)
class PanoptesPollingPlugin(PanoptesBasePlugin):
    """
    The base class for all Panoptes Polling plugins

    A Polling Plugin's role is to return a collection of metrics associated with a resource
    """

    @abc.abstractmethod
    def run(self, context):
        """
        The entry point for Polling plugins

        Args:
            context (PanoptesPluginContext): The context of the plugin contains the plugins configuration, the logger \
            it should use and a client for the system wide plugins KV store

        Returns:
            PanoptesMetricSet: A collection of metrics polled by the plugin

       Raises:
            PanoptesPluginConfigurationError: If the plugin cannot proceed with the configuration provided to it \
            through the context, it should raise a PanoptesPluginConfigurationError

        """
        pass
