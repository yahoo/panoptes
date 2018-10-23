"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements a context object that is passed to each plugin during execution

The context of plugin contains it's configuration, the logger it should use, the key/value store it can use and an
optional, arbitrary data object to be passed to the plugin
"""
import os
import unittest

from mock import patch

from yahoo_panoptes.framework.plugins.context import PanoptesPluginContext, PanoptesPluginWithEnrichmentContext
from yahoo_panoptes.framework.resources import PanoptesContext

from .test_framework import PanoptesTestKeyValueStore, panoptes_mock_redis_strict_client

plugin_conf = {
    'Core': {
        'name': 'Test Plugin',
        'module': 'test_plugin'
    },
    'main': {
        'execute_frequency': '60',
        'resource_filter': 'resource_class = "network"'
    }
}


class TestEnrichment:
    pass


class TestPanoptesPluginContexts(unittest.TestCase):
    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = _get_test_conf_file()
        self.__panoptes_context = PanoptesContext(self.panoptes_test_conf_file)

    def test_panoptes_plugin_context(self):
        test_kv_store = PanoptesTestKeyValueStore(self.__panoptes_context)
        test_secrets_store = PanoptesTestKeyValueStore(self.__panoptes_context)
        panoptes_plugin_context = PanoptesPluginContext(self.__panoptes_context, "test logger", plugin_conf,
                                                        test_kv_store, test_secrets_store, {'test_key': 'test_value'})

        self.assertEqual(panoptes_plugin_context.logger, self.__panoptes_context.logger.getChild("test logger"))
        self.assertEqual(panoptes_plugin_context.config, plugin_conf)
        self.assertEqual(panoptes_plugin_context.kv, test_kv_store)
        self.assertEqual(panoptes_plugin_context.secrets, test_secrets_store)
        self.assertEqual(panoptes_plugin_context.data, {'test_key': 'test_value'})
        self.assertSetEqual(panoptes_plugin_context.sites, self.__panoptes_context.config_object.sites)
        self.assertDictEqual(panoptes_plugin_context.snmp, self.__panoptes_context.config_object.snmp_defaults)

    def test_panoptes_plugin_with_enrichment_context(self):
        test_kv_store = PanoptesTestKeyValueStore(self.__panoptes_context)
        test_secrets_store = PanoptesTestKeyValueStore(self.__panoptes_context)
        test_enrichment = TestEnrichment()
        panoptes_plugin_with_enrichment_context = PanoptesPluginWithEnrichmentContext(self.__panoptes_context,
                                                                                      "test logger", plugin_conf,
                                                                                      test_kv_store, test_secrets_store,
                                                                                      {'test_key': 'test_value'},
                                                                                      test_enrichment)
        self.assertEqual(panoptes_plugin_with_enrichment_context.enrichment, test_enrichment)


def _get_test_conf_file():
    my_dir = os.path.dirname(os.path.realpath(__file__))
    panoptes_test_conf_file = os.path.join(my_dir, 'config_files/test_panoptes_config.ini')

    return my_dir, panoptes_test_conf_file
