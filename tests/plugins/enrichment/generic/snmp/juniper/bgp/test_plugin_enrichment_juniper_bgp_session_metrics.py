import unittest
import os

from yahoo_panoptes.plugins.enrichment.generic.snmp.juniper.bgp.plugin_enrichment_bgp_session_metrics import \
    JuniperBGPInfoPluginEnrichmentMetrics

from tests.plugins.helpers import setup_module_default, tear_down_module_default, \
    SNMPEnrichmentPluginTestFramework
from mock import *

pwd = os.path.dirname(os.path.abspath(__file__))
mock_time = Mock()
mock_time.return_value = 1512629517.03121

mock_total_single_ports_oid = Mock()
mock_total_single_ports_oid.return_value = None

mock_source_pools = Mock()
mock_source_pools.return_value = None

mock_routing_engines = Mock()
mock_routing_engines.return_value = None


def setUpModule():
    return setup_module_default(plugin_pwd=pwd)


def tearDownModule():
    return tear_down_module_default()


class TestPluginJuniperBGPDeviceMetricsEnrichment(SNMPEnrichmentPluginTestFramework, unittest.TestCase):
    """
    Test Juniper BGP Metrics

    Note:
        44.144.154.142
        42.7.240.144
        2131:7fa:4011:4:5000:2a:400:5001
        2303:792:11:504:2:372c:0:6
        The above IP Addresses which are used in the snmprec file were randomly generated.
        Verizon Media is not associated with them.
    """
    path = pwd
    results_data_file = 'devicetype_location_results.json'
    snmp_community = 'devicetype_location'
    plugin_conf = {'Core': {'name': 'Test Plugin', 'module': 'test_plugin'},
                   'main': {'execute_frequency': '60', 'enrichment_ttl': '300',
                            'resource_filter': 'resource_class = "network" AND resource_type = "juniper"',
                            'polling_frequency': '300', 'enrichment_schema_version': '0.2'},
                   'snmp': {'timeout': 5, 'retries': 2},
                   'enrichment': {'preload': 'self:metrics'},
                   'x509': {'x509_secured_requests': 0},
                   }
    plugin_class = JuniperBGPInfoPluginEnrichmentMetrics
    use_enrichment = False

    def test_polling_plugin_timeout(self):
        """Test plugin raises error during timeout"""
        pass
