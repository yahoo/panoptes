import os
import unittest

from tests.plugins.helpers import SNMPPollingPluginTestFramework, setup_module_default, tear_down_module_default
from yahoo_panoptes.plugins.polling.generic.snmp import plugin_polling_generic_snmp
from yahoo_panoptes.framework.plugins import panoptes_base_plugin
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
    plugin_metrics_function = 'get_results'
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
        'enrichment': {
            'preload': 'self:metrics'
        }
    }

    def test_inactive_port(self):
        """See base class."""
        pass

    def test_get_snmp_polling_var(self):
        """Test plugin_polling_generic_snmp.PluginPollingGenericSNMPMetrics._get_snmp_polling_var"""
        plugin = self._get_test_plugin_instance()

        # non_repeaters obtained from enrichment json
        self.assertEqual(plugin._get_snmp_polling_var('non_repeaters', 1), 0)

    def _get_test_plugin_instance(self):
        """Reset the plugin_instance and set basic configurations."""

        plugin = self.plugin_class()

        plugin._plugin_context = self._plugin_context
        plugin._enrichment = plugin._plugin_context.enrichment
        plugin._namespace = plugin._plugin_context.config['main']['namespace']
        plugin._device = plugin._plugin_context.data
        plugin._host = plugin._device.resource_endpoint
        plugin._logger = plugin._plugin_context.logger

        plugin._get_config()

        return plugin

    def test_add_defaults(self):
        """Ensure Exception is raised when _add_defaults is called with bad arguments."""
        plugin = self._get_test_plugin_instance()
        try:
            plugin._add_defaults("metics", dict())
        except Exception as e:
            assert e.message == 'Error on "127.0.0.1" (None) in namespace "metrics": "target" must be of type ' \
                                '"metrics" or "dimensions" but instead is of type "metics"'


class TestPluginPollingGenericSNMPFeaturesEnrichmentFromFile(TestPluginPollingGenericSNMPFeatures, unittest.TestCase):
    """Test plugin when enrichment is read in from a file."""
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
            'file': 'tests/plugins/polling/generic/snmp/data/enrichment.json.example'
        }
    }

    def test_no_service_active(self):
        """Tests a valid resource_endpoint with no service active"""
        self._resource_endpoint = '127.0.0.2'
        self._snmp_conf['timeout'] = self._snmp_failure_timeout
        self.results_data_file = "from_file_no_service_active_results.json"
        self.set_panoptes_resource()
        self.set_plugin_context()
        self.set_expected_results()

        plugin = self.plugin_class()

        if self._plugin_conf.get('enrichment'):
            if self._plugin_conf['enrichment'].get('file'):
                results = plugin.run(self._plugin_context)
                self.assertEqual(ordered(self._expected_results), ordered(self._remove_timestamps(results)))

        self._resource_endpoint = '127.0.0.1'
        self._snmp_conf['timeout'] = self.snmp_timeout
        self.results_data_file = "results.json"
        self.set_panoptes_resource()
        self.set_plugin_context()
        self.set_expected_results()


class TestPluginPollingGenericSNMPFeaturesMissingOIDs(TestPluginPollingGenericSNMPFeatures, unittest.TestCase):
    """Test plugin against missing cpu oids."""
    enrichment_data_file = "missing_cpu_oids_enrichment_data"
    results_data_file = "missing_cpu_oids_results.json"
    snmp_community = "missing_cpu_oids"

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
        }
    }

    def test_get_snmp_polling_var(self):
        """Test plugin_polling_generic_snmp.PluginPollingGenericSNMPMetrics._get_snmp_polling_var."""
        plugin = self._get_test_plugin_instance()

        self.assertEqual(plugin._get_snmp_polling_var('non_repeaters', 1), 0)

        # Check that non_repeaters is set to default value when no json or plugin config entry
        self.plugin_conf['snmp'].pop('non_repeaters')
        self.assertEqual(plugin._get_snmp_polling_var('non_repeaters', 1), 1)
