import os
import unittest

from yahoo_panoptes.framework import enrichment
from yahoo_panoptes.framework.plugins import panoptes_base_plugin
from yahoo_panoptes.plugins.polling.generic.snmp import plugin_polling_generic_snmp
from tests.plugins import helpers

module_path = os.path.dirname(os.path.abspath(__file__))


def setUpModule():
    helpers.setup_module_default(module_path)


def tearDownModule():
    helpers.tear_down_module_default()


class TestPluginPollingCiscoIOS(helpers.SNMPPollingPluginTestFramework, unittest.TestCase):

    plugin_class = plugin_polling_generic_snmp.PluginPollingGenericSNMPMetrics
    path = os.path.dirname(os.path.abspath(__file__))

    snmp_community = "3560G-48TS-S"
    results_data_file = "3560G-48TS-S.results.json"
    enrichment_data_file = "3560G-48TS-S.enrichment_data"
    plugin_conf = {
        'Core': {
            'name': 'Test Plugin',
            'module': 'test_plugin'
        },
        'main': {
            'execute_frequency': '60',
            'enrichment_ttl': '300',
            'resource_filter': 'resource_class = "network"',
            'namespace': 'metrics',
            'enrichment_schema_version': '0.2',
            'polling_status_metric_name': 'polling_status'
        },
        'snmp': {
            'timeout': 10,
            'retries': 1,
            'non_repeaters': 0,
            'max_repetitions': 25,
        },
        'enrichment': {
            'preload': 'self:metrics'
        },
        'x509': {'x509_secured_requests': 0}
    }

    def test_inactive_port(self):
        pass


class TestPluginPollingCiscoIOSEnrichmentFromFile(helpers.SNMPPollingPluginTestFramework, unittest.TestCase):
    plugin_class = plugin_polling_generic_snmp.PluginPollingGenericSNMPMetrics
    path = os.path.dirname(os.path.abspath(__file__))
    snmp_community = "3560G-48TS-S"
    results_data_file = "3560G-48TS-S.results.json"

    plugin_conf = {
        'Core': {
            'name': 'Test Plugin',
            'module': 'test_plugin'
        },
        'main': {
            'execute_frequency': '60',
            'enrichment_ttl': '300',
            'resource_filter': 'resource_class = "network"',
            'namespace': 'metrics',
            'polling_status_metric_name': 'polling_status'
        },
        'snmp': {
            'timeout': 10,
            'retries': 1,
            'non_repeaters': 0,
            'max_repetitions': 25,
        },
        'enrichment': {
            'file': 'tests/plugins/polling/generic/snmp/cisco/ios/data/3560G-48TS-S.enrichment_data.json'
        },
        'x509': {'x509_secured_requests': 0}
    }

    def test_inactive_port(self):
        pass


class TestPluginPollingCiscoIOSEnrichmentFromFileBad(TestPluginPollingCiscoIOSEnrichmentFromFile, unittest.TestCase):
    plugin_conf = {
        'Core': {
            'name': 'Test Plugin',
            'module': 'test_plugin'
        },
        'main': {
            'execute_frequency': '60',
            'enrichment_ttl': '300',
            'resource_filter': 'resource_class = "network"',
            'namespace': 'metrics',
            'polling_status_metric_name': 'polling_status'
        },
        'snmp': {
            'timeout': 10,
            'retries': 1,
            'non_repeaters': 0,
            'max_repetitions': 25,
        },
        'enrichment': {
            'file': 'tests/plugins/polling/generic/snmp/cisco/ios/data/3560G-48TS-S.enrichment_bad.json'
        },
        'x509': {'x509_secured_requests': 0}
    }

    def test_basic_operations(self):
        with self.assertRaises(panoptes_base_plugin.PanoptesPluginConfigurationError):
            plugin = self.plugin_class()
            plugin.run(self._plugin_context)


class TestPluginPollingCiscoIOSEnrichmentFromFileMissing(TestPluginPollingCiscoIOSEnrichmentFromFile,
                                                         unittest.TestCase):
    plugin_conf = {
        'Core': {
            'name': 'Test Plugin',
            'module': 'test_plugin'
        },
        'main': {
            'execute_frequency': '60',
            'enrichment_ttl': '300',
            'resource_filter': 'resource_class = "network"',
            'namespace': 'metrics',
            'polling_status_metric_name': 'polling_status'
        },
        'snmp': {
            'timeout': 10,
            'retries': 1,
            'non_repeaters': 0,
            'max_repetitions': 25,
        },
        'enrichment': {
        },
        'x509': {'x509_secured_requests': 0}
    }

    def test_basic_operations(self):
        with self.assertRaises(plugin_polling_generic_snmp.PanoptesEnrichmentFileEmptyError):
            plugin = self.plugin_class()
            plugin.run(self._plugin_context)


class TestPluginPollingCiscoIOSEnrichmentFromFileBothPresent(TestPluginPollingCiscoIOSEnrichmentFromFile,
                                                             unittest.TestCase):
    plugin_conf = {
        'Core': {
            'name': 'Test Plugin',
            'module': 'test_plugin'
        },
        'main': {
            'execute_frequency': '60',
            'enrichment_ttl': '300',
            'resource_filter': 'resource_class = "network"',
            'namespace': 'metrics',
            'polling_status_metric_name': 'polling_status'
        },
        'snmp': {
            'timeout': 10,
            'retries': 1,
            'non_repeaters': 0,
            'max_repetitions': 25,
        },
        'enrichment': {
            'file': 'tests/plugins/polling/generic/snmp/cisco/ios/data/3560G-48TS-S.json',
            'preload': 'self:metrics'
        },
        'x509': {'x509_secured_requests': 0}
    }

    def test_basic_operations(self):
        with self.assertRaises(enrichment.PanoptesEnrichmentCacheError):
            plugin = self.plugin_class()
            plugin.run(self._plugin_context)

    def test_no_service_active(self):
        # Since the enrichment is defined in both the config and via Key-Value store a
        # PanoptesEnrichmentCacheError error will be thrown.
        pass


class TestPluginPollingCiscoIOS4900M(TestPluginPollingCiscoIOS):
    snmp_community = "4900M"
    results_data_file = "4900M.results.json"
    enrichment_data_file = "4900M.enrichment_data"
    plugin_class = plugin_polling_generic_snmp.PluginPollingGenericSNMPMetrics


class TestPluginPollingCiscoIOS6509E(TestPluginPollingCiscoIOS):
    snmp_community = "6509-E"
    results_data_file = "6509-E.results.json"
    enrichment_data_file = "6509-E.enrichment_data"
    plugin_class = plugin_polling_generic_snmp.PluginPollingGenericSNMPMetrics


class TestPluginPollingCiscoIOS4948E(TestPluginPollingCiscoIOS):
    snmp_community = "4948E"
    results_data_file = "4948E.results.json"
    enrichment_data_file = "4948E.enrichment_data"
    plugin_class = plugin_polling_generic_snmp.PluginPollingGenericSNMPMetrics


