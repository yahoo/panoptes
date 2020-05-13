import mock
import os
import unittest

from yahoo_panoptes.plugins.enrichment.generic.snmp.juniper import plugin_enrichment_juniper_device_metrics
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


class PluginJuniperDeviceMetricsEnrichment(helpers.SNMPEnrichmentPluginTestFramework):
    path = pwd
    plugin_conf = {
        'Core': {
            'name': 'Test Plugin',
            'module': 'test_plugin'
        },
        'main': {
            'execute_frequency': 60,
            'enrichment_ttl': 300,
            'resource_filter': 'resource_class = "network" AND resource_type = "juniper"',
            'polling_frequency': 300,
            'enrichment_schema_version': 0.2
        },
        'snmp': {
            'timeout': 5,
            'retries': 2
        },
        'enrichment': {
            'preload': 'self:metrics'
        },
        'x509': {
            'x509_secured_requests': 0
        }
    }
    plugin_class = plugin_enrichment_juniper_device_metrics.JuniperPluginEnrichmentDeviceMetrics
    use_enrichment = False

    def test_enrichment_plugin_timeout(self):
        """Test plugin raises error during timeout"""
        pass


class TestPluginMX960(PluginJuniperDeviceMetricsEnrichment, unittest.TestCase):
    results_data_file = 'mx960_results.json'
    snmp_community = 'mx960'


class TestPluginMX2020(PluginJuniperDeviceMetricsEnrichment, unittest.TestCase):
    results_data_file = 'mx2020_results.json'
    snmp_community = 'mx2020'


class TestPluginSRX(PluginJuniperDeviceMetricsEnrichment, unittest.TestCase):
    results_data_file = 'srx1400_results.json'
    snmp_community = 'srx1400'


class TestPluginQFX(PluginJuniperDeviceMetricsEnrichment, unittest.TestCase):
    results_data_file = 'qfx_results.json'
    snmp_community = 'qfx'


class TestPluginEX(PluginJuniperDeviceMetricsEnrichment, unittest.TestCase):
    results_data_file = 'ex_results.json'
    snmp_community = 'ex'


class TestPluginEX4300(PluginJuniperDeviceMetricsEnrichment, unittest.TestCase):
    resource_model = 'EX4300-48P'
    results_data_file = 'ex4300_results.json'
    snmp_community = 'ex'

