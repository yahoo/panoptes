import mock
import os
import unittest

from yahoo_panoptes.plugins.enrichment.generic.snmp.cisco.nxos import plugin_enrichment_cisco_nxos_device_metrics
from tests.plugins import helpers

pwd = os.path.dirname(os.path.abspath(__file__))
mock_time = mock.Mock()
mock_time.return_value = 1512629517.03121

mock_total_single_ports_oid = mock.Mock()
mock_total_single_ports_oid.return_value = None

mock_source_pools = mock.Mock()
mock_source_pools.return_value = None

mock_routing_engines = mock.Mock()
mock_routing_engines.return_value = None


def setUpModule():
    return helpers.setup_module_default(plugin_pwd=pwd)


def tearDownModule():
    return helpers.tear_down_module_default()


class TestPluginCiscoNXOSEnrichment(helpers.SNMPEnrichmentPluginTestFramework, unittest.TestCase):
    """
    Test Juniper SRX Functional Metrics.
    """
    path = pwd
    resource_id = '3048'
    resource_model = '3048'
    snmp_community = '3048'
    results_data_file = '3048.results.json'
    resource_backplane = 'backplane'
    plugin_conf = {u'Core': {u'name': u'Test Plugin', u'module': u'test_plugin'},
                   u'main': {u'execute_frequency': 60, u'enrichment_ttl': 300,
                             u'resource_filter': u'resource_class = "network" AND resource_type = "cisco" AND '
                                                 u'resource_metadata.os_name LIKE "CISCO NX-OS%"',
                             u'polling_frequency': 300,
                             u'n3k_models': [u"3048T", u"3064-X", u"3064"]},
                   u'snmp': {u'timeout': 5, u'retries': 2},
                   u'enrichment': {u'preload': u'self:metrics'},
                   u'x509': {u'x509_secured_requests': 0}
                   }
    plugin_class = plugin_enrichment_cisco_nxos_device_metrics.CiscoNXOSPluginEnrichmentMetrics
    use_enrichment = False

    def test_polling_plugin_timeout(self):
        """Test plugin raises error during timeout"""
        pass


class TestPluginCiscoNXOSEnrichmentN3KModels(TestPluginCiscoNXOSEnrichment, unittest.TestCase):
    """
    Test plugin's handling of N3K Model NXOS devices.
    """
    resource_id = 'n3k_3048T'
    snmp_community = 'n3k_3048T'
    results_data_file = 'n3k_3048T.results.json'
    resource_model = '3048T'
