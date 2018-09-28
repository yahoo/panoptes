import json
import os
import unittest
from mock import Mock, patch

from yahoo_panoptes.framework.utilities.helpers import ordered
from tests.plugins.helpers import DiscoveryPluginTestFramework
from yahoo_panoptes.plugins.discovery.plugin_discovery_from_json_file import PluginDiscoveryJSONFile
from yahoo_panoptes.discovery.panoptes_discovery_plugin import PanoptesDiscoveryPluginError

mock_time = Mock()
mock_time.return_value = 1512629517.03121


class TestPluginFromJSONDiscovery(DiscoveryPluginTestFramework, unittest.TestCase):

    plugin = PluginDiscoveryJSONFile()
    path = os.path.dirname(os.path.abspath(__file__))

    def __init__(self, test_name):
        super(TestPluginFromJSONDiscovery, self).__init__(test_name)

    def setUp(self):
        self.plugin = PluginDiscoveryJSONFile()
        super(TestPluginFromJSONDiscovery, self).setUp()

    @patch('yahoo_panoptes.framework.resources.time', mock_time)
    def test_basic_operations(self):
        self._plugin_context.config['main']['config_file'] = \
            os.path.join(os.path.abspath(self.path), 'data/test.json')
        result = self.plugin.run(self._plugin_context)

        self.assertEqual(ordered(json.loads(result.json)), ordered(self._expected_results))

    def test_nonexistent_file(self):
        self._plugin_context.config['main']['config_file'] = \
            os.path.join(os.path.abspath(self.path), 'data/bogus.json')
        with self.assertRaises(PanoptesDiscoveryPluginError):
            self.plugin.run(self._plugin_context)

    def test_bad_resource(self):
        self._plugin_context.config['main']['config_file'] = \
            os.path.join(os.path.abspath(self.path), 'data/bad_resource.json')
        with self.assertRaises(PanoptesDiscoveryPluginError):
            self.plugin.run(self._plugin_context)

    def test_bad_json(self):
        self._plugin_context.config['main']['config_file'] = \
            os.path.join(os.path.abspath(self.path), 'data/bad.json')
        with self.assertRaises(PanoptesDiscoveryPluginError):
            self.plugin.run(self._plugin_context)

    def test_mixed_good_bad_resources(self):
        self._plugin_context.config['main']['config_file'] = \
            os.path.join(os.path.abspath(self.path), 'data/one_bad_resource.json')
        result = self.plugin.run(self._plugin_context)
        self.assertEqual(len(result.resources), 2)

        self._plugin_context.config['main']['config_file'] = \
            os.path.join(os.path.abspath(self.path), 'data/one_bad_resource_in_middle.json')
        result = self.plugin.run(self._plugin_context)
        self.assertEqual(len(result.resources), 3)
