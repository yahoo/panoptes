"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
import os
import time
import unittest

from mock import patch, MagicMock
from yapsy.PluginInfo import PluginInfo

from yahoo_panoptes.framework.plugins.panoptes_base_plugin import PanoptesPluginInfo, PanoptesPluginInfoValidators, \
    PanoptesPluginConfigurationError, PanoptesBasePluginValidators, PanoptesBasePlugin
from yahoo_panoptes.polling.polling_plugin import PanoptesPollingPluginInfo
from yahoo_panoptes.framework.resources import PanoptesResource, PanoptesContext
from yahoo_panoptes.framework.plugins.runner import PanoptesPluginRunner

from .test_framework import PanoptesTestKeyValueStore, panoptes_mock_kazoo_client, panoptes_mock_redis_strict_client


class TestPanoptesPluginRunner(unittest.TestCase):
    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = _get_test_conf_file()
        self.__panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                                  key_value_store_class_list=[PanoptesTestKeyValueStore],
                                                  create_message_producer=False, async_message_producer=False,
                                                  create_zookeeper_client=True)
        self.__panoptes_resource = PanoptesResource(resource_site='test', resource_class='test',
                                                    resource_subclass='test',
                                                    resource_type='test', resource_id='test', resource_endpoint='test',
                                                    resource_plugin='test')

    def test_basic_operations(self):
        runner = PanoptesPluginRunner("plugin_name", "plugin_type", PanoptesBasePlugin, PanoptesPluginInfo, None,
                                      self.__panoptes_context, )


def _get_test_conf_file():
    my_dir = os.path.dirname(os.path.realpath(__file__))
    panoptes_test_conf_file = os.path.join(my_dir, 'config_files/test_panoptes_config.ini')

    return my_dir, panoptes_test_conf_file