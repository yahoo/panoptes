import os
import unittest
from mock import Mock, patch

from tests.plugins.helpers import SNMPPollingPluginTestFramework, setup_module_default, tear_down_module_default
from yahoo_panoptes.plugins.polling.interface.plugin_polling_device_interface_metrics import \
    PluginPollingDeviceInterfaceMetrics, _MISSING_METRIC_VALUE, _INTERFACE_STATES

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
        'enrichment': {
            'preload': 'self:interface'
        }
    }

    plugin_class = PluginPollingDeviceInterfaceMetrics
    path = pwd

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
