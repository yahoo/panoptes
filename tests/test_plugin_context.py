"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements a context object that is passed to each plugin during execution

The context of plugin contains it's configuration, the logger it should use, the key/value store it can use and an
optional, arbitrary data object to be passed to the plugin
"""
import unittest

from mock import patch

from yahoo_panoptes.framework.plugins.context import PanoptesPluginContext, PanoptesPluginWithEnrichmentContext
from yahoo_panoptes.framework.resources import PanoptesContext

from .test_framework import PanoptesTestKeyValueStore, panoptes_mock_redis_strict_client
from .helpers import get_test_conf_file

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
        self.my_dir, self.panoptes_test_conf_file = get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file)

    def test_panoptes_plugin_context(self):
        test_kv_store = PanoptesTestKeyValueStore(self._panoptes_context)
        test_secrets_store = PanoptesTestKeyValueStore(self._panoptes_context)
        panoptes_plugin_context = PanoptesPluginContext(self._panoptes_context, "test logger", plugin_conf,
                                                        test_kv_store, test_secrets_store, {'test_key': 'test_value'})

        self.assertEqual(panoptes_plugin_context.logger, self._panoptes_context.logger.getChild("test logger"))
        self.assertEqual(panoptes_plugin_context.config, plugin_conf)
        self.assertEqual(panoptes_plugin_context.kv, test_kv_store)
        self.assertEqual(panoptes_plugin_context.secrets, test_secrets_store)
        self.assertEqual(panoptes_plugin_context.data, {'test_key': 'test_value'})
        self.assertSetEqual(panoptes_plugin_context.sites, self._panoptes_context.config_object.sites)
        self.assertDictEqual(panoptes_plugin_context.snmp, self._panoptes_context.config_object.snmp_defaults)

    def test_panoptes_plugin_with_enrichment_context(self):
        test_kv_store = PanoptesTestKeyValueStore(self._panoptes_context)
        test_secrets_store = PanoptesTestKeyValueStore(self._panoptes_context)
        test_enrichment = TestEnrichment()
        panoptes_plugin_with_enrichment_context = PanoptesPluginWithEnrichmentContext(self._panoptes_context,
                                                                                      "test logger", plugin_conf,
                                                                                      test_kv_store, test_secrets_store,
                                                                                      {'test_key': 'test_value'},
                                                                                      test_enrichment)
        self.assertEqual(panoptes_plugin_with_enrichment_context.enrichment, test_enrichment)
