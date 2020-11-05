import unittest

from yahoo_panoptes.polling.polling_plugin import PanoptesPollingPlugin, PanoptesPollingPluginInfo,\
    PanoptesPollingPluginConfigurationError


class PanoptesConcretePollingPlugin(PanoptesPollingPlugin):
    def run(self, context):
        """
        Execute the given context.

        Args:
            self: (todo): write your description
            context: (dict): write your description
        """
        super(PanoptesConcretePollingPlugin, self).run(context)


class TestPollingPlugin(unittest.TestCase):
    def test_polling_plugin_info(self):
        """
        Test if the test information.

        Args:
            self: (todo): write your description
        """
        plugin_info = PanoptesPollingPluginInfo(plugin_name=u'test', plugin_path=u'test')
        with self.assertRaises(PanoptesPollingPluginConfigurationError):
            plugin_info.resource_filter

    def test_polling_plugin(self):
        """
        Test if the plugin is closed.

        Args:
            self: (todo): write your description
        """
        plugin = PanoptesConcretePollingPlugin()
        with self.assertRaises(NotImplementedError):
            plugin.run(None)
