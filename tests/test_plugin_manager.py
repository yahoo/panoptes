"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""

import os
import unittest

from mock import patch

from yahoo_panoptes.framework.context import PanoptesContext
from yahoo_panoptes.framework.plugins.manager import PanoptesPluginManager
from yahoo_panoptes.polling.polling_plugin import PanoptesPollingPlugin

from test_framework import panoptes_mock_redis_strict_client, panoptes_mock_kazoo_client


class TestPanoptesPluginManagerContext(PanoptesContext):
    def __init__(self):
        my_dir = os.path.dirname(os.path.realpath(__file__))
        panoptes_test_conf_file = os.path.join(my_dir, 'config_files/test_panoptes_config.ini')
        super(TestPanoptesPluginManagerContext, self).__init__(config_file=panoptes_test_conf_file,
                                                               create_zookeeper_client=True)


class TestPanoptesPluginManager(unittest.TestCase):
    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_panoptes_plugin_manager(self):
        matching_test_plugins_found = 0

        context = TestPanoptesPluginManagerContext()
        plugin_manager = PanoptesPluginManager(plugin_type='polling',
                                               plugin_class=PanoptesPollingPlugin,
                                               panoptes_context=context)

        plugins = plugin_manager.getPluginsOfCategory('polling')

        self.assertGreater(len(plugins), 0)

        for plugin in plugins:
            if 'Test Polling Plugin' in plugin.name:
                matching_test_plugins_found += 1

        self.assertEqual(matching_test_plugins_found, 2)

        polling_plugin = plugin_manager.getPluginByName(name='Test Polling Plugin', category='polling')

        self.assertEqual(polling_plugin.name, 'Test Polling Plugin')
        self.assertEqual(os.path.split(polling_plugin.config_filename)[1], 'plugin_polling_test.panoptes-plugin')

        polling_plugin_second_instance = plugin_manager.getPluginByName(name='Test Polling Plugin Second Instance',
                                                                        category='polling')

        self.assertEqual(polling_plugin_second_instance.name, 'Test Polling Plugin Second Instance')
        self.assertEqual(os.path.split(polling_plugin_second_instance.config_filename)[1],
                         'plugin_polling_test_second_instance.panoptes-plugin')
