import os
import unittest

from tests.plugins.helpers import SNMPPollingPluginTestFramework, setup_module_default, tear_down_module_default
from yahoo_panoptes.plugins.polling.generic.snmp.plugin_polling_generic_snmp import \
    PluginPollingGenericSNMPMetrics

module_path = os.path.dirname(os.path.abspath(__file__))


def setUpModule():
    setup_module_default(module_path)


def tearDownModule():
    tear_down_module_default()


class TestMX960DeviceMetrics(SNMPPollingPluginTestFramework, unittest.TestCase):
    snmp_community = 'mx960'
    enrichment_data_file = 'mx960_enrichment'
    results_data_file = 'mx960_results.json'
    plugin_class = PluginPollingGenericSNMPMetrics
    path = os.path.dirname(os.path.abspath(__file__))
    plugin_conf = {
        'Core': {
            'name': 'Test Plugin',
            'module': 'test_plugin'
        },
        'main': {
            'execute_frequency': 60,
            'enrichment_ttl': 1440,
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
            'preload': 'self:metrics'
        },
        'x509': {
            'x509_secured_requests': 0
        }
    }

    def test_inactive_port(self):
        pass


class TestMX2020DeviceMetrics(TestMX960DeviceMetrics, unittest.TestCase):
    snmp_community = 'mx2020'
    enrichment_data_file = 'mx2020_enrichment'
    results_data_file = 'mx2020_results.json'


class TestSRX1400DeviceMetrics(TestMX960DeviceMetrics, unittest.TestCase):
    snmp_community = 'srx1400'
    enrichment_data_file = 'srx1400_enrichment'
    results_data_file = 'srx1400_results.json'


class TestEXDeviceMetrics(TestMX960DeviceMetrics, unittest.TestCase):
    snmp_community = 'ex'
    enrichment_data_file = 'ex_enrichment'
    results_data_file = 'ex_results.json'


class TestQFXDeviceMetrics(TestMX960DeviceMetrics, unittest.TestCase):
    snmp_community = 'qfx'
    enrichment_data_file = 'qfx_enrichment'
    results_data_file = 'qfx_results.json'
