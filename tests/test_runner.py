"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
import os
import time
import unittest

from mock import patch, MagicMock

from yahoo_panoptes.framework.plugins.panoptes_base_plugin import PanoptesPluginInfo, PanoptesPluginInfoValidators, \
    PanoptesPluginConfigurationError, PanoptesBasePluginValidators, PanoptesBasePlugin
from yahoo_panoptes.polling.polling_plugin import PanoptesPollingPluginInfo, PanoptesPollingPlugin
from yahoo_panoptes.framework.resources import PanoptesResource, PanoptesContext
from yahoo_panoptes.framework.plugins.manager import PanoptesPluginManager
from yahoo_panoptes.framework.plugins.runner import PanoptesPluginRunner, PanoptesPluginWithEnrichmentRunner
from yahoo_panoptes.framework.metrics import PanoptesMetric, PanoptesMetricType, PanoptesMetricsGroup, \
    PanoptesMetricsGroupSet, PanoptesMetricDimension

from .test_framework import PanoptesTestKeyValueStore, panoptes_mock_kazoo_client, panoptes_mock_redis_strict_client
from tests.plugins.polling.test.plugin_polling_test import PanoptesTestPollingPlugin


def generic_callback(context=None, results=None, plugin=None):
    pass


class PanoptesTestPlugin(PanoptesBasePlugin):
    pass


class TestPanoptesPluginRunner(unittest.TestCase):
    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = _get_test_conf_file()
        self.__panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                                  key_value_store_class_list=[PanoptesTestKeyValueStore],
                                                  create_message_producer=False, async_message_producer=False,
                                                  create_zookeeper_client=True)

    def test_basic_operations(self):
        runner = PanoptesPluginRunner("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                      None, self.__panoptes_context, PanoptesTestKeyValueStore,
                                      PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                      PanoptesMetricsGroupSet, generic_callback)

        # #  Ensure logging methods run:
        # runner.info(PanoptesTestPollingPlugin, "Test Info log message")
        # runner.warn(PanoptesTestPollingPlugin, "Test Warning log message")
        # runner.error(PanoptesTestPollingPlugin, "Test Error log message", Exception)
        # runner.exception(PanoptesTestPollingPlugin, "Test Exception log message")

        runner.execute_plugin()


class TestPanoptesPluginWithEnrichmentRunner(unittest.TestCase):
    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = _get_test_conf_file()
        self.__panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                                  key_value_store_class_list=[PanoptesTestKeyValueStore],
                                                  create_message_producer=False, async_message_producer=False,
                                                  create_zookeeper_client=True)

    def test_basic_operations(self):
        runner = PanoptesPluginWithEnrichmentRunner("Test Polling Plugin", "polling", PanoptesPollingPlugin,
                                                    PanoptesPluginInfo, None, self.__panoptes_context,
                                                    PanoptesTestKeyValueStore, PanoptesTestKeyValueStore,
                                                    PanoptesTestKeyValueStore, "plugin_logger",
                                                    PanoptesMetricsGroupSet, generic_callback)

        # #  Ensure logging methods run:
        # runner.info(PanoptesTestPollingPlugin, "Test Info log message")
        # runner.warn(PanoptesTestPollingPlugin, "Test Warning log message")
        # runner.error(PanoptesTestPollingPlugin, "Test Error log message", Exception)
        # runner.exception(PanoptesTestPollingPlugin, "Test Exception log message")

        runner.execute_plugin()



def _get_test_conf_file():
    my_dir = os.path.dirname(os.path.realpath(__file__))
    panoptes_test_conf_file = os.path.join(my_dir, 'config_files/test_panoptes_config.ini')

    return my_dir, panoptes_test_conf_file
