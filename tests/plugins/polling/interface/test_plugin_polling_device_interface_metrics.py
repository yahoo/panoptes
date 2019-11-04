import os
import unittest
from mock import Mock, patch

from tests.plugins.helpers import SNMPPollingPluginTestFramework, setup_module_default, tear_down_module_default
from yahoo_panoptes.plugins.polling.interface.plugin_polling_device_interface_metrics import \
    PluginPollingDeviceInterfaceMetrics, _MISSING_METRIC_VALUE, _INTERFACE_STATES
from yahoo_panoptes.framework.plugins.panoptes_base_plugin import PanoptesPluginRuntimeError

_MOCK_INTERFACE_ENTRY = '0'
pwd = os.path.dirname(os.path.abspath(__file__))


def setUpModule():
    return setup_module_default(plugin_pwd=pwd)


def tearDownModule():
    return tear_down_module_default()


class TestPluginPollingDeviceInterfaceMetrics(SNMPPollingPluginTestFramework, unittest.TestCase):
    plugin_conf = {
        u'Core': {
            u'name': u'Test Plugin',
            u'module': u'test_plugin'
        },
        u'main': {
            u'execute_frequency': 60,
            u'resource_filter': u'resource_class = "network"'
        },
        u'snmp': {
            u'timeout': 10,
            u'retries': 1,
            u'non_repeaters': 0,
            u'max_repetitions': 25
        },
        u'enrichment': {
            u'preload': u'self:interface'
        },
        u'x509': {u'x509_secured_requests': 0}
    }

    plugin_metrics_function = "get_results"
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
        assert self.plugin_class._get_state_val(u'2') == _INTERFACE_STATES.DOWN
        assert self.plugin_class._get_state_val(u'1') == _INTERFACE_STATES.UP
        assert self.plugin_class._get_state_val(u'1234') == _INTERFACE_STATES.UNKNOWN

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

    def test_no_service_active(self):
        self._resource_endpoint = u'127.0.0.2'
        self._snmp_conf['timeout'] = self._snmp_failure_timeout
        self.set_panoptes_resource()
        self.set_plugin_context()

        # Actually get a new instance, else residual metrics may still exist from other tests
        plugin = self.plugin_class()
        results = plugin.run(self._plugin_context)

        if self.uses_polling_status is True:
            self.assertEqual(len(results.metrics_groups), 1)
        else:
            self.assertEqual(len(results.metrics_groups), 0)

        self._resource_endpoint = u'127.0.0.1'
        self._snmp_conf['timeout'] = self._snmp_timeout
        self.set_panoptes_resource()
        self.set_plugin_context()

    def test_invalid_resource_endpoint(self):
        self._resource_endpoint = u'127.0.0.257'
        self._snmp_conf['timeout'] = self._snmp_failure_timeout
        self.set_panoptes_resource()
        self.set_plugin_context()

        plugin = self.plugin_class()
        with self.assertRaises(PanoptesPluginRuntimeError):
            plugin.run(self._plugin_context)

        self._resource_endpoint = u'127.0.0.1'
        self._snmp_conf['timeout'] = self.snmp_timeout
        self.set_panoptes_resource()
        self.set_plugin_context()
