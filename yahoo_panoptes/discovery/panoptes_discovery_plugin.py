"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module declares an abstract base class for Panoptes Discovery Plugins - and related helpers

A Discovery Plugin's role is to return a collection of resources that should be monitored by the system
"""
import abc

import six

from ..framework.exceptions import PanoptesBaseException
from ..framework.plugins.context import PanoptesPluginContext
from ..framework.plugins.panoptes_base_plugin import PanoptesBasePlugin


class PanoptesDiscoveryPluginError(PanoptesBaseException):
    """
    The class for Discovery Plugin runtime errors
    """
    pass


@six.add_metaclass(abc.ABCMeta)
class PanoptesDiscoveryPlugin(PanoptesBasePlugin):
    """
    The base class for all Panoptes Discovery plugins

    A Discovery Plugin's role is to return a collection of resources that should be monitored by the system
    """

    @abc.abstractmethod
    def run(self, context):
        """
        The entry point for Discovery Manager plugins

        Args:
            context (PanoptesPluginContext): The context of the plugin contains the plugins configuration, the logger \
            it should use and a client for the system wide plugins KV store

        Returns:
            PanoptesResourceSet: A collection of resources discovered by the plugin

       Raises:
            PanoptesPluginConfigurationError: If the plugin cannot proceed with the configuration provided to it \
            through the context, it should raise a PanoptesPluginConfigurationError

        """
        pass
