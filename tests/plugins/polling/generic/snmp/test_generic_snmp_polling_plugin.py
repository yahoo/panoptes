import os
import unittest

from tests.plugins.helpers import SNMPPollingPluginTestFramework, setup_module_default, tear_down_module_default
from yahoo_panoptes.plugins.polling.generic.snmp import plugin_polling_generic_snmp
from yahoo_panoptes.framework.utilities.helpers import ordered

module_path = os.path.dirname(os.path.abspath(__file__))


def setUpModule():
    setup_module_default(module_path)


def tearDownModule():
    tear_down_module_default()


class TestPluginPollingGenericSNMPFeatures(SNMPPollingPluginTestFramework, unittest.TestCase):
    """Test basic features of Generic SNMP Polling Plugin"""
    plugin_class = plugin_polling_generic_snmp.PluginPollingGenericSNMPMetrics
    path = module_path

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
        """See base class."""
        pass

    def test_no_service_active(self):
        """See base class."""
        pass

    def test_get_snmp_polling_var(self):
        """Test plugin_polling_generic_snmp.PluginPollingGenericSNMPMetrics._get_snmp_polling_var"""
        plugin = self._get_test_plugin_instance()

        # non_repeaters obtained from enrichment json
        self.assertEqual(plugin._get_snmp_polling_var(u'non_repeaters', 1), 0)

    def _get_test_plugin_instance(self):
        """Reset the plugin_instance and set basic configurations."""
        plugin = self.plugin_class()

        plugin._plugin_context = self._plugin_context
        plugin._enrichment = plugin._plugin_context.enrichment
        plugin._namespace = plugin._plugin_context.config['main']['namespace']
        plugin._device = plugin._plugin_context.data
        plugin._device_host = plugin._device.resource_endpoint
        plugin._logger = plugin._plugin_context.logger

        plugin._get_config()

        return plugin

    def test_add_defaults(self):
        """Ensure Exception is raised when _add_defaults is called with bad arguments."""
        plugin = self._get_test_plugin_instance()
        try:
            plugin._add_defaults(u"dummy", u"metics", dict())
        except Exception as e:
            expected_error_message = 'Error on "127.0.0.1" (None) in namespace "metrics": "target" must be of type ' \
                                     '"metrics" or "dimensions" but has value "dummy"'
            if hasattr(e, 'message'):
                assert e.message == expected_error_message
            else:
                assert str(e) == expected_error_message


class TestPluginPollingGenericSNMPFeaturesEnrichmentFromFile(TestPluginPollingGenericSNMPFeatures, unittest.TestCase):
    """Test plugin when enrichment is read in from a file."""
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
            u'polling_status_metric_name': u'polling_status'
        },
        u'snmp': {
            u'timeout': 10,
            u'retries': 1,
            u'non_repeaters': 0,
            u'max_repetitions': 25,
        },
        u'enrichment': {
            u'file': u'tests/plugins/polling/generic/snmp/data/enrichment.json.example'
        },
        u'x509': {u'x509_secured_requests': 0}
    }

    def test_no_service_active(self):
        """Tests a valid resource_endpoint with no service active"""
        self._resource_endpoint = u'192.0.2.1'  # Per RFC 5737
        self._snmp_conf[u'timeout'] = self._snmp_failure_timeout
        self.results_data_file = u"from_file_no_service_active_results.json"
        self.set_panoptes_resource()
        self.set_plugin_context()
        self.set_expected_results()

        plugin = self.plugin_class()

        if self._plugin_conf.get(u'enrichment'):
            if self._plugin_conf[u'enrichment'].get(u'file'):
                results = plugin.run(self._plugin_context)
                self.assertEqual(ordered(self._expected_results), ordered(self._remove_timestamps(results)))

        self._resource_endpoint = u'127.0.0.1'
        self._snmp_conf[u'timeout'] = self.snmp_timeout
        self.results_data_file = u"results.json"
        self.set_panoptes_resource()
        self.set_plugin_context()
        self.set_expected_results()


class TestPluginPollingGenericSNMPFeaturesMissingOIDs(TestPluginPollingGenericSNMPFeatures, unittest.TestCase):
    """Test plugin against missing cpu oids."""
    enrichment_data_file = "missing_cpu_oids_enrichment_data"
    results_data_file = "missing_cpu_oids_results.json"
    snmp_community = "missing_cpu_oids"

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

    def test_get_snmp_polling_var(self):
        """Test plugin_polling_generic_snmp.PluginPollingGenericSNMPMetrics._get_snmp_polling_var."""
        plugin = self._get_test_plugin_instance()

        self.assertEqual(plugin._get_snmp_polling_var(u'non_repeaters', 1), 0)

        # Check that non_repeaters is set to default value when no json or plugin config entry
        self.plugin_conf[u'snmp'].pop(u'non_repeaters')
        self.assertEqual(plugin._get_snmp_polling_var(u'non_repeaters', 1), 1)
