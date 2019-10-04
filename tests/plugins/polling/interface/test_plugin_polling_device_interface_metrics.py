import os
import unittest
from mock import Mock, patch

from tests.plugins.helpers import SNMPPollingPluginTestFramework, setup_module_default, tear_down_module_default
from yahoo_panoptes.plugins.polling.interface.plugin_polling_device_interface_metrics import \
    PluginPollingDeviceInterfaceMetrics, _MISSING_METRIC_VALUE, _INTERFACE_STATES
from yahoo_panoptes.plugins.polling.utilities.polling_status import DEVICE_METRICS_STATES
from yahoo_panoptes.framework.plugins.panoptes_base_plugin import PanoptesPluginRuntimeError

_MOCK_INTERFACE_ENTRY = '0'
pwd = os.path.dirname(os.path.abspath(__file__))


def setUpModule():
    return setup_module_default(plugin_pwd=pwd)


def tearDownModule():
    return tear_down_module_default()


class TestPluginPollingDeviceInterfaceMetrics(SNMPPollingPluginTestFramework, unittest.TestCase):
    plugin_conf = {
        'Core': {
            'name': 'Test Plugin',
            'module': 'test_plugin'
        },
        'main': {
            'execute_frequency': '60',
            'resource_filter': 'resource_class = "network"'
        },
        'snmp': {
            'timeout': 10,
            'retries': 1,
            'non_repeaters': 0,
            'max_repetitions': 25
        },
        'enrichment': {
            'preload': 'self:interface'
        },
        'x509': {'x509_secured_requests': 0}
    }

    plugin_class = PluginPollingDeviceInterfaceMetrics
    path = pwd

    def test_inactive_port(self):
        pass

    def test_no_service_active(self):
        """Tests a valid resource_endpoint with no service active"""
        self._resource_endpoint = '127.0.0.2'
        self._snmp_conf['timeout'] = self._snmp_failure_timeout
        self.set_panoptes_resource()
        self.set_plugin_context()

        plugin = self.plugin_class()

        if self._plugin_conf.get('enrichment'):
            if self._plugin_conf['enrichment'].get('preload'):
                result = plugin.run(self._plugin_context)
                result = self._remove_timestamps(result)

                self.assertEqual(len(result), 1)
                self.assertEqual(
                    result[0]['metrics'][0]['metric_value'],
                    DEVICE_METRICS_STATES.PING_FAILURE
                )

        self._resource_endpoint = '127.0.0.1'
        self._snmp_conf['timeout'] = self.snmp_timeout
        self.set_panoptes_resource()
        self.set_plugin_context()

    def test_missing_interface(self):
        plugin = self.plugin_class()
        plugin.run(self._plugin_context)
        self.assertEqual(plugin.get_bits_in(_MOCK_INTERFACE_ENTRY), _MISSING_METRIC_VALUE)
        self.assertEqual(plugin.get_bits_out(_MOCK_INTERFACE_ENTRY), _MISSING_METRIC_VALUE)
        self.assertEqual(plugin.get_total_packets_in(_MOCK_INTERFACE_ENTRY), _MISSING_METRIC_VALUE)
        self.assertEqual(plugin.get_total_packets_out(_MOCK_INTERFACE_ENTRY), _MISSING_METRIC_VALUE)

    def test_get_state_val(self):
        assert self.plugin_class._get_state_val('2') == _INTERFACE_STATES.DOWN
        assert self.plugin_class._get_state_val('1') == _INTERFACE_STATES.UP
        assert self.plugin_class._get_state_val('1234') == _INTERFACE_STATES.UNKNOWN

    def test_metric_not_number(self):
        mock_is_instance = Mock(return_value=False)
        plugin = self.plugin_class()
        with patch('yahoo_panoptes.plugins.polling.interface.plugin_polling_device_interface_metrics.'
                   'isinstance', mock_is_instance):
            results = plugin.run(self._plugin_context)
            self.assertEqual(len(results), 2)

    def test_iftable_stats_exception_handling(self):
        mock_get_type = Mock(side_effect=Exception)
        plugin = self.plugin_class()
        plugin.get_type = mock_get_type
        self.assertRaises(Exception, plugin.run(self._plugin_context))

    def test_ifxtable_stats_exception_handling(self):
        mock_get_bits_in = Mock(side_effect=Exception)
        plugin = self.plugin_class()
        plugin.get_bits_in = mock_get_bits_in
        self.assertRaises(Exception, plugin.run(self._plugin_context))

    def test_dot3table_stats_exception_handling(self):
        mock_get_errors_frame = Mock(side_effect=Exception)
        plugin = self.plugin_class()
        plugin.get_errors_frame = mock_get_errors_frame
        self.assertRaises(Exception, plugin.run(self._plugin_context))

