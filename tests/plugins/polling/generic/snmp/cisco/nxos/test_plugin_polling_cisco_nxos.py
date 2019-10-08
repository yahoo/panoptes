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


class TestPluginPollingCiscoNXOS3048(SNMPPollingPluginTestFramework, unittest.TestCase):
    plugin_class = PluginPollingGenericSNMPMetrics
    path = os.path.dirname(os.path.abspath(__file__))

    snmp_community = '3048'
    results_data_file = '3048_results.json'
    enrichment_data_file = '3048_enrichment_data'

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


class TestPluginPollingCiscoNXOSn3k(TestPluginPollingCiscoNXOS3048, unittest.TestCase):
    snmp_community = 'n3k_3048T'
    results_data_file = 'n3k_3048T_results.json'
    enrichment_data_file = 'n3k_3048T_enrichment_data'


