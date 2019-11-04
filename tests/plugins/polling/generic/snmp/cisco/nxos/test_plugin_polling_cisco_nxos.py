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


class TestPluginPollingCiscoNXOSn3k(TestPluginPollingCiscoNXOS3048, unittest.TestCase):
    snmp_community = 'n3k_3048T'
    results_data_file = 'n3k_3048T_results.json'
    enrichment_data_file = 'n3k_3048T_enrichment_data'
