import mock
import os
import unittest

from yahoo_panoptes.plugins.enrichment.generic.snmp.cisco.ios import plugin_enrichment_cisco_ios_device_metrics
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


class TestPluginCiscoIOSEnrichment(helpers.SNMPEnrichmentPluginTestFramework, unittest.TestCase):
    """
    Test Cisco Device Metrics.
    """
    path = pwd
    resource_id = '4948E'
    snmp_community = '4948E'
    results_data_file = '4948E.results.json'
    resource_backplane = 'backplane'
    resource_model = '4948E'
    plugin_conf = {u'Core': {u'name': u'Test Plugin', u'module': u'test_plugin'},
                   u'main': {u'execute_frequency': 60, u'enrichment_ttl': 300,
                             u'resource_filter': u'resource_class = "network" AND resource_type = "cisco" AND '
                                                 u'resource_metadata.os_name LIKE "CISCO IOS%"',
                             u'polling_frequency': 300},
                   u'snmp': {u'timeout': 5, u'retries': 2},
                   u'enrichment': {u'preload': u'self:metrics'},
                   u'x509': {u'x509_secured_requests': 0}
                   }
    plugin_class = plugin_enrichment_cisco_ios_device_metrics.CiscoIOSPluginEnrichmentMetrics
    use_enrichment = False

    def test_polling_plugin_timeout(self):
        """Test plugin raises error during timeout"""
        pass


class TestPluginCiscoIOSEnrichmentCisco6509(TestPluginCiscoIOSEnrichment, unittest.TestCase):
    """
    Test Cisco Device Metrics for 6509s.
    """
    resource_model = '6509-E'
    resource_id = '6509-E'
    snmp_community = '6509-E'
    results_data_file = '6509-E.results.json'
    plugin_class = plugin_enrichment_cisco_ios_device_metrics.CiscoIOSPluginEnrichmentMetrics


class TestPluginCiscoIOSEnrichment3560(TestPluginCiscoIOSEnrichment, unittest.TestCase):
    """
    Test Cisco Device Metrics for 3560s.
    """
    resource_model = '3560G-48TS-S'
    resource_id = '3560G-48TS-S'
    snmp_community = '3560G-48TS-S'
    results_data_file = '3560G-48TS-S.results.json'
    plugin_class = plugin_enrichment_cisco_ios_device_metrics.CiscoIOSPluginEnrichmentMetrics


class TestPluginCiscoIOSEnrichmentAttributeErrorBug(TestPluginCiscoIOSEnrichment, unittest.TestCase):
    """
    Test Cisco Device Metrics for a 4900M device which is raising an Attribute error.
    """
    resource_id = '4900M'
    snmp_community = '4900M'
    resource_model = '4900M'
    results_data_file = '4900M.results.json'
    plugin_class = plugin_enrichment_cisco_ios_device_metrics.CiscoIOSPluginEnrichmentMetrics
