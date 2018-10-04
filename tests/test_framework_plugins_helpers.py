import unittest

from mock import create_autospec, patch

from yahoo_panoptes.framework.plugins.helpers import expires, time_limit
from yahoo_panoptes.framework.plugins.panoptes_base_plugin import PanoptesPluginInfo


class TestFrameworkPlugins(unittest.TestCase):
    def test_helpers(self):
        mock_panoptes_plugin_info = create_autospec(PanoptesPluginInfo)
        mock_panoptes_plugin_info.execute_frequency = 60

        mock_plugin_agent_plugin_expires_multiple = 2
        with patch('yahoo.contrib.panoptes.framework.plugins.helpers.const.PLUGIN_AGENT_PLUGIN_EXPIRES_MULTIPLE',
                   mock_plugin_agent_plugin_expires_multiple):
            self.assertEqual(expires(mock_panoptes_plugin_info), 60*2)
            self.assertNotEqual(expires(mock_panoptes_plugin_info), 60*3)

        with self.assertRaises(AssertionError):
            expires(PanoptesPluginInfo)

        mock_plugin_agent_plugin_time_limit_multiple = 1.25
        with patch('yahoo.contrib.panoptes.framework.plugins.helpers.const.PLUGIN_AGENT_PLUGIN_TIME_LIMIT_MULTIPLE',
                   mock_plugin_agent_plugin_time_limit_multiple):
            self.assertEqual(time_limit(mock_panoptes_plugin_info), 60*1.25)
            mock_panoptes_plugin_info.execute_frequency = 5
            self.assertNotEqual(time_limit(mock_panoptes_plugin_info), 5*1.25)
            self.assertEqual(time_limit(mock_panoptes_plugin_info), round(5*1.25))

        with self.assertRaises(AssertionError):
            time_limit(PanoptesPluginInfo)
