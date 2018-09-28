"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements a context object that is passed to each plugin during execution

The context of plugin contains it's configuration, the logger it should use, the key/value store it can use and an
optional, arbitrary data object to be passed to the plugin
"""
from ..context import PanoptesContext, PanoptesContextValidators
from ..utilities.key_value_store import PanoptesKeyValueStore


class PanoptesPluginContext(object):
    """
    This class defines the context which is passed to each plugin instance when it is executed

    Args:
        panoptes_context(PanoptesContext): The Panoptes Context associated with the Plugin Agent that
        executed the plugin
        logger_name(str): The name of the logger to use
        config(dict): The plugin configuration parsed from the .panoptes-plugin info file associated with the plugin
        key_value_store(PanoptesKeyValueStore): The Key/Value store class to use
        secrets_store(PanoptesKeyValueStore): The secrets key/value store class to use
        data(object): An optional data object which would be passed through to the plugin being executed
    """

    def __init__(self, panoptes_context, logger_name, config, key_value_store, secrets_store, data=None):
        assert PanoptesContextValidators.valid_panoptes_context(
            panoptes_context), 'panoptes_context must be an instance of PanoptesContext'
        assert logger_name and isinstance(logger_name, str), 'logger_name must be a non-empty str'
        assert config is None or isinstance(config, dict), 'config must be a dict'
        assert isinstance(secrets_store,
                          PanoptesKeyValueStore), 'secrets_store must be an instance of PanoptesKeyValueStore'
        assert isinstance(key_value_store,
                          PanoptesKeyValueStore), 'key_value_store must be an instance of PanoptesKeyValueStore'
        self._panoptes_context = panoptes_context
        self._logger = panoptes_context.logger.getChild(logger_name)
        self._config = config
        self._kv_store = key_value_store
        self._secrets_store = secrets_store
        self._data = data
        self._sites = panoptes_context.config_object.sites
        self._snmp_defaults = panoptes_context.config_object.snmp_defaults

    @property
    def logger(self):
        """
        The logger being used by the plugin

        Returns:
            logging.logger: The logger being used by the plugin

        """
        return self._logger

    @property
    def config(self):
        """
        The config being used by the plugin

        Returns:
            dict: The config being used by the plugin
        """
        return self._config

    @property
    def kv(self):
        """
        The Key/Value store being used by the plugin

        Returns:
            PanoptesKeyValueStore: The Key/Value store being used by the plugin
        """
        return self._kv_store

    @property
    def secrets(self):
        """
        The secrets store being used by the plugin

        Returns:
            PanoptesKeyValueStore: The secrets store being used by the plugin
        """
        return self._secrets_store

    @property
    def data(self):
        """
        The data object passed to the plugin

        Returns:
            object:  The data object passed to the plugin
        """
        return self._data

    @property
    def sites(self):
        """
        The set of sites used by the plugin

        Returns:
            set(str):  Set of sites used by the plugin
        """
        return self._sites

    @property
    def snmp(self):
        """
        The SNMP configuration from the site configuration file

        Returns:
            dict: A dictionary containing SNMP configuration
        """
        return self._snmp_defaults


class PanoptesPluginWithEnrichmentContext(PanoptesPluginContext):
    def __init__(self, panoptes_context, logger_name, config, key_value_store,
                 secrets_store, data=None, enrichment=None):
        super(PanoptesPluginWithEnrichmentContext, self).__init__(panoptes_context,
                                                                  logger_name,
                                                                  config,
                                                                  key_value_store,
                                                                  secrets_store,
                                                                  data=data)
        self._enrichment = enrichment

    @property
    def enrichment(self):
        """
        The enrichment object passed to the plugin

        Returns:
            object:  The enrichment object passed to the plugin
        """
        return self._enrichment
