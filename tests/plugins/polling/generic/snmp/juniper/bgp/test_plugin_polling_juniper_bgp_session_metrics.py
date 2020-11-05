import os
import unittest

from tests.plugins.helpers import SNMPPollingPluginTestFramework, setup_module_default, tear_down_module_default
from yahoo_panoptes.plugins.polling.generic.snmp.plugin_polling_generic_snmp import \
    PluginPollingGenericSNMPMetrics

module_path = os.path.dirname(os.path.abspath(__file__))


def setUpModule():
    """
    Sets the default setup.

    Args:
    """
    setup_module_default(module_path)


def tearDownModule():
    """
    Tear down the module.

    Args:
    """
    tear_down_module_default()


class TestPluginPollingBGPMetrics(SNMPPollingPluginTestFramework, unittest.TestCase):
    """
    Note:
        44.144.154.142
        42.7.240.144
        2131:7fa:4011:4:5000:2a:400:5001
        2303:792:11:504:2:372c:0:6
        The above IP Addresses which are used in the public.snmprec file were randomly generated.
        Verizon Media is not associated with them.
    """
    plugin_class = PluginPollingGenericSNMPMetrics
    path = os.path.dirname(os.path.abspath(__file__))
    maxDiff = None
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
            'preload': 'self:metrics'
        },
        'x509': {'x509_secured_requests': 0}
    }

    def test_inactive_port(self):
        """
        Test if inactive port.

        Args:
            self: (todo): write your description
        """
        pass

    def test_no_service_active(self):
        """
        Test if the service is active.

        Args:
            self: (todo): write your description
        """
        pass