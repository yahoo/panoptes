import unittest

from mock import create_autospec

from yahoo_panoptes.framework.const import PLUGIN_AGENT_PLUGIN_EXPIRES_MULTIPLE, \
    PLUGIN_AGENT_PLUGIN_TIME_LIMIT_MULTIPLE
from yahoo_panoptes.framework.plugins.helpers import expires, time_limit
from yahoo_panoptes.framework.plugins.panoptes_base_plugin import PanoptesPluginInfo


class TestFrameworkPlugins(unittest.TestCase):
    def test_helpers(self):
        mock_panoptes_plugin_info = create_autospec(PanoptesPluginInfo)
        mock_panoptes_plugin_info.execute_frequency = 5.75

        #  Assert 'round' is called by testing against non-integer product
        self.assertNotEqual(expires(mock_panoptes_plugin_info),
                            mock_panoptes_plugin_info.execute_frequency * PLUGIN_AGENT_PLUGIN_EXPIRES_MULTIPLE)
        self.assertEqual(expires(mock_panoptes_plugin_info),
                         round(mock_panoptes_plugin_info.execute_frequency * PLUGIN_AGENT_PLUGIN_EXPIRES_MULTIPLE))

        #  Test failure when called on a non-instance of PanoptesPluginInfo
        with self.assertRaises(AssertionError):
            expires(PanoptesPluginInfo)

        #  Assert 'round' is called by testing against non-integer product
        self.assertNotEqual(time_limit(mock_panoptes_plugin_info),
                            mock_panoptes_plugin_info.execute_frequency * PLUGIN_AGENT_PLUGIN_TIME_LIMIT_MULTIPLE)
        self.assertEqual(time_limit(mock_panoptes_plugin_info),
                         round(mock_panoptes_plugin_info.execute_frequency * PLUGIN_AGENT_PLUGIN_TIME_LIMIT_MULTIPLE))

        #  Test failure when called on a non-instance of PanoptesPluginInfo
        with self.assertRaises(AssertionError):
            time_limit(PanoptesPluginInfo)
