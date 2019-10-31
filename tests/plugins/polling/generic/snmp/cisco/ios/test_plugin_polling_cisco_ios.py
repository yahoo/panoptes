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
        u'Core': {
            u'name': u'Test Plugin',
            u'module': u'test_plugin'
        },
        u'main': {
            u'execute_frequency': 60,
            u'enrichment_ttl': 300,
            u'resource_filter': u'resource_class = "network"',
            u'namespace': u'metrics',
            u'enrichment_schema_version': u'0.2',
            u'polling_status_metric_name': u'polling_status'
        },
        u'snmp': {
            u'timeout': 10,
            u'retries': 1,
            u'non_repeaters': 0,
            u'max_repetitions': 25,
        },
        u'enrichment': {
            u'preload': u'self:metrics'
        },
        u'x509': {u'x509_secured_requests': 0}
    }

    def test_inactive_port(self):
        pass


class TestPluginPollingCiscoIOSEnrichmentFromFile(helpers.SNMPPollingPluginTestFramework, unittest.TestCase):
    plugin_class = plugin_polling_generic_snmp.PluginPollingGenericSNMPMetrics
    path = os.path.dirname(os.path.abspath(__file__))
    snmp_community = "3560G-48TS-S"
    results_data_file = "3560G-48TS-S.results.json"

    plugin_conf = {
        u'Core': {
            u'name': u'Test Plugin',
            u'module': u'test_plugin'
        },
        u'main': {
            u'execute_frequency': 60,
            u'enrichment_ttl': 300,
            u'resource_filter': u'resource_class = "network"',
            u'namespace': u'metrics',
            u'polling_status_metric_name': u'polling_status'
        },
        u'snmp': {
            u'timeout': 10,
            u'retries': 1,
            u'non_repeaters': 0,
            u'max_repetitions': 25,
        },
        u'enrichment': {
            u'file': u'tests/plugins/polling/generic/snmp/cisco/ios/data/3560G-48TS-S.enrichment_data.json'
        },
        u'x509': {u'x509_secured_requests': 0}
    }

    def test_inactive_port(self):
        pass


class TestPluginPollingCiscoIOSEnrichmentFromFileBad(TestPluginPollingCiscoIOSEnrichmentFromFile, unittest.TestCase):
    plugin_conf = {
        u'Core': {
            u'name': u'Test Plugin',
            u'module': u'test_plugin'
        },
        u'main': {
            u'execute_frequency': 60,
            u'enrichment_ttl': 300,
            u'resource_filter': u'resource_class = "network"',
            u'namespace': u'metrics',
            u'polling_status_metric_name': u'polling_status'
        },
        u'snmp': {
            u'timeout': 10,
            u'retries': 1,
            u'non_repeaters': 0,
            u'max_repetitions': 25,
        },
        u'enrichment': {
            u'file': u'tests/plugins/polling/generic/snmp/cisco/ios/data/3560G-48TS-S.enrichment_bad.json'
        },
        u'x509': {u'x509_secured_requests': 0}
    }

    snmp_community = "4900M"
    results_data_file = "enrichment_failure.results.json"
    enrichment_data_file = "4900M.enrichment_data"


class TestPluginPollingCiscoIOSEnrichmentFromFileMissing(TestPluginPollingCiscoIOSEnrichmentFromFile,
                                                         unittest.TestCase):
    plugin_conf = {
        u'Core': {
            u'name': u'Test Plugin',
            u'module': u'test_plugin'
        },
        u'main': {
            u'execute_frequency': 60,
            u'enrichment_ttl': 300,
            u'resource_filter': u'resource_class = "network"',
            u'namespace': u'metrics',
            u'polling_status_metric_name': u'polling_status'
        },
        u'snmp': {
            u'timeout': 10,
            u'retries': 1,
            u'non_repeaters': 0,
            u'max_repetitions': 25,
        },
        u'enrichment': {
        },
        u'x509': {u'x509_secured_requests': 0}
    }

    snmp_community = "4900M"
    results_data_file = "internal_failure.results.json"
    enrichment_data_file = "4900M.enrichment_data"


class TestPluginPollingCiscoIOSEnrichmentFromFileBothPresent(TestPluginPollingCiscoIOSEnrichmentFromFile,
                                                             unittest.TestCase):
    plugin_conf = {
        u'Core': {
            u'name': u'Test Plugin',
            u'module': u'test_plugin'
        },
        u'main': {
            u'execute_frequency': 60,
            u'enrichment_ttl': 300,
            u'resource_filter': u'resource_class = "network"',
            u'namespace': u'metrics',
            u'polling_status_metric_name': u'polling_status'
        },
        u'snmp': {
            u'timeout': 10,
            u'retries': 1,
            u'non_repeaters': 0,
            u'max_repetitions': 25,
        },
        u'enrichment': {
            u'file': u'tests/plugins/polling/generic/snmp/cisco/ios/data/3560G-48TS-S.json',
            u'preload': u'self:metrics'
        },
        u'x509': {u'x509_secured_requests': 0}
    }

    snmp_community = "4900M"
    results_data_file = "internal_failure.results.json"
    enrichment_data_file = "4900M.enrichment_data"

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
