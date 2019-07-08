import unittest
import os
import json

from yahoo_panoptes.plugins.enrichment.generic.snmp.plugin_enrichment_generic_snmp import \
    PanoptesEnrichmentGenericSNMPPlugin
from tests.plugins.helpers import setup_module_default, tear_down_module_default, SNMPEnrichmentPluginTestFramework
from mock import *
from yahoo_panoptes.framework.utilities.helpers import ordered

pwd = os.path.dirname(os.path.abspath(__file__))
mock_time = Mock()
mock_time.return_value = 1512629517.03121


def setUpModule():
    return setup_module_default(plugin_pwd=pwd)


def tearDownModule():
    return tear_down_module_default()


class TestPluginEnrichmentGenericSNMP(SNMPEnrichmentPluginTestFramework, unittest.TestCase):
    """
    Test Juniper SRX Functional Metrics
    """
    path = pwd
    results_data_file = 'results.json'
    resource_backplane = 'backplane'
    plugin_conf = {'Core': {'name': 'Test Plugin', 'module': 'test_plugin'},
                   'main': {'execute_frequency': 60, 'enrichment_ttl': 300,
                            'resource_filter': 'resource_class = "network" AND resource_type = "juniper" AND '
                                               'resource_metadata.model LIKE "SRX%"'},
                   'snmp': {'timeout': 5, 'retries': 2},
                   'enrichment': {'preload': 'self:metrics'}
                   }
    plugin_class = PanoptesEnrichmentGenericSNMPPlugin

    @patch('time.time', mock_time)
    @patch('yahoo_panoptes.framework.resources.time', mock_time)
    def test_polling_plugin_results(self):
        """Test plugin result and validate results with input data/results.json"""
        plugin = self.plugin_class()
        result = plugin.run(self._plugin_context)
        self.assertEqual(ordered(json.loads(result.json())), ordered(self._expected_results))

    def test_enrichment_plugin_timeout(self):
        """Test plugin raises error during timeout"""
        pass

    def test_bad_config(self):
        """Test correct error thrown when configuration is malformed"""
        malformed_config = {'port': {},  # port must be positive int
                            'timeout': self._snmp_timeout,
                            'retries': self._snmp_retries,
                            'community_string_key': 'panoptes:secrets:snmp_community_string'}
        plugin = self.plugin_class()
        with self.assertRaises(Exception):
            plugin.run(malformed_config)
