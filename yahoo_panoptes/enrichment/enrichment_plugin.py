"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module declares an abstract base class for Panoptes Enrichment Plugins - and related helpers

A Enrichment Plugin's role is to return a collection of enrichment info that have been collected by the system
"""
import abc

import six

from ..framework.plugins.context import PanoptesPluginContext
from ..polling.polling_plugin import PanoptesPollingPluginError,\
    PanoptesPollingPluginConfigurationError, PanoptesPollingPlugin, PanoptesPollingPluginInfo


class PanoptesEnrichmentPluginError(PanoptesPollingPluginError):
    """
    The class for Enrichment Plugin runtime errors
    """
    pass


class PanoptesEnrichmentPluginConfigurationError(PanoptesPollingPluginConfigurationError):
    """
    The class for Enrichment Plugin configuration errors
    """
    pass


class PanoptesEnrichmentPluginInfo(PanoptesPollingPluginInfo):
    """
    This class extends the PanoptesPollingPluginInfo to contain the resource_filter associated with an Enrichment
    Plugin

    Args:
        plugin_name(str): The name of the plugin
        plugin_path(str): The 'path' of the plugin - either the absolute path of the directory that contains the plugin\
        module or the path of the module file without the '.py' extension
    """
    pass


@six.add_metaclass(abc.ABCMeta)
class PanoptesEnrichmentPlugin(PanoptesPollingPlugin):
    """
    The base class for all Panoptes Enrichment plugins

    A Enrichment Plugin's role is to return a collection of enrichment info associated with a resource
    """

    @abc.abstractmethod
    def run(self, context):
        """
        The entry point for Enrichment plugins

        Args:
            context (PanoptesPluginContext): The context of the plugin contains the plugins configuration, the logger \
            it should use and a client for the system wide plugins KV store

        Returns:
            object: Enrichment data PanoptesEnrichmentGroupSet/PanoptesEnrichmentMultiGroupSet
        collected by the plugin

       Raises:
            PanoptesPluginConfigurationError: If the plugin cannot proceed with the configuration provided to it \
            through the context, it should raise a PanoptesPluginConfigurationError

        """
        pass
