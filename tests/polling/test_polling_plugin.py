import unittest

from yahoo_panoptes.polling.polling_plugin import PanoptesPollingPlugin, PanoptesPollingPluginInfo,\
    PanoptesPollingPluginConfigurationError


class PanoptesConcretePollingPlugin(PanoptesPollingPlugin):
    def run(self, context):
        super(PanoptesConcretePollingPlugin, self).run(context)


class TestPollingPlugin(unittest.TestCase):
    def test_polling_plugin_info(self):
        plugin_info = PanoptesPollingPluginInfo(plugin_name=u'test', plugin_path=u'test')
        with self.assertRaises(PanoptesPollingPluginConfigurationError):
            plugin_info.resource_filter

    def test_polling_plugin(self):
        plugin = PanoptesConcretePollingPlugin()
        with self.assertRaises(NotImplementedError):
            plugin.run(None)
