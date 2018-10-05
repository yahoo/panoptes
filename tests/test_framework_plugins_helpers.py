import unittest

from mock import create_autospec, patch

from yahoo_panoptes.framework.plugins.helpers import expires, time_limit
from yahoo_panoptes.framework.plugins.panoptes_base_plugin import PanoptesPluginInfo


class TestFrameworkPlugins(unittest.TestCase):
    def test_helpers(self):
        mock_panoptes_plugin_info = create_autospec(PanoptesPluginInfo)
        mock_panoptes_plugin_info.execute_frequency = 60

        self.assertEqual(expires(mock_panoptes_plugin_info), round(60 * 2))

        with self.assertRaises(AssertionError):
            expires(PanoptesPluginInfo)

        self.assertEqual(time_limit(mock_panoptes_plugin_info), round(60 * 1.25))
        mock_panoptes_plugin_info.execute_frequency = 5
        self.assertNotEqual(time_limit(mock_panoptes_plugin_info), 5 * 1.25)
        self.assertEqual(time_limit(mock_panoptes_plugin_info), round(5 * 1.25))

        with self.assertRaises(AssertionError):
            time_limit(PanoptesPluginInfo)
