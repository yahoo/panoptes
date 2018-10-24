"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
import os
import unittest

from mock import patch, MagicMock

from yahoo_panoptes.framework.plugins.panoptes_base_plugin import PanoptesPluginInfo, PanoptesBasePlugin
from yahoo_panoptes.polling.polling_plugin import PanoptesPollingPlugin
from yahoo_panoptes.framework.resources import PanoptesContext, PanoptesResource
from yahoo_panoptes.framework.plugins.runner import PanoptesPluginRunner, PanoptesPluginWithEnrichmentRunner
from yahoo_panoptes.framework.metrics import PanoptesMetric, PanoptesMetricsGroupSet

from .test_framework import PanoptesTestKeyValueStore, panoptes_mock_kazoo_client, panoptes_mock_redis_strict_client


_TIMESTAMP = 1


def _callback(*args, **kwargs):
    pass


def _callback_no_args():
    pass


class PanoptesTestPlugin(PanoptesBasePlugin):
    name = None
    signature = None
    data = {}

    execute_now = True
    plugin_object = None

    def run(self, context):
        pass


class PanoptesTestPluginRaiseException():
    name = None
    version = None
    last_executed = None
    last_executed_age = None
    last_results = None
    last_results_age = None
    moduleMtime = None
    configMtime = None

    signature = None
    data = {}

    execute_now = True
    lock = "dummy"

    def run(self, context):
        raise Exception


class MockPluginExecuteNow():
    execute_now = False


class MockPluginLockException():
    name = None
    signature = None
    data = {}

    execute_now = True
    lock = MagicMock(side_effect=Exception)


class MockPluginLockNone():
    name = None
    signature = None
    data = {}

    execute_now = True
    lock = None


class TestPanoptesPluginRunner(unittest.TestCase):
    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = _get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                                  key_value_store_class_list=[PanoptesTestKeyValueStore],
                                                  create_message_producer=False, async_message_producer=False,
                                                  create_zookeeper_client=True)
        self._runner_class = PanoptesPluginRunner

    def test_basic_operations(self):
        runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                      None, self._panoptes_context, PanoptesTestKeyValueStore,
                                      PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                      PanoptesMetricsGroupSet, _callback)
        runner.execute_plugin()

        # Test callback failure
        runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                   None, self._panoptes_context, PanoptesTestKeyValueStore,
                                   PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                   PanoptesMetricsGroupSet, _callback_no_args)
        runner.execute_plugin()

        # Test non-existent plugin
        runner = self._runner_class("Non-existent Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                      None, self._panoptes_context, PanoptesTestKeyValueStore,
                                      PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                      PanoptesMetricsGroupSet, _callback)
        runner.execute_plugin()

        # Test bad plugin_type
        runner = self._runner_class("Test Polling Plugin", "bad", PanoptesPollingPlugin, PanoptesPluginInfo,
                                      None, self._panoptes_context, PanoptesTestKeyValueStore,
                                      PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                      PanoptesMetricsGroupSet, _callback)
        runner.execute_plugin()

        mock_get_plugin_by_name = MagicMock(return_value=MockPluginExecuteNow())
        with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginManager.getPluginByName',
                   mock_get_plugin_by_name):
            runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                        None, self._panoptes_context, PanoptesTestKeyValueStore,
                                        PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                        PanoptesMetricsGroupSet, _callback)
            runner.execute_plugin()

    def test_lock(self):
        mock_get_plugin_by_name = MagicMock(return_value=MockPluginLockNone())
        mock_get_context = MagicMock(return_value=self._panoptes_context)
        with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginManager.getPluginByName',
                   mock_get_plugin_by_name):
            with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginRunner._get_context', mock_get_context):
                runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                           None, self._panoptes_context, PanoptesTestKeyValueStore,
                                           PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                           PanoptesMetricsGroupSet, _callback)
                runner.execute_plugin()

    def test_lock_error(self):
        mock_plugin = MagicMock(return_value=PanoptesTestPlugin)
        mock_get_context = MagicMock(return_value=self._panoptes_context)
        with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginManager.getPluginByName',
                   mock_plugin):
            with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginRunner._get_context', mock_get_context):
                runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                           None, self._panoptes_context, PanoptesTestKeyValueStore,
                                           PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                           PanoptesMetricsGroupSet, _callback)
                runner.execute_plugin()

    def test_plugin_failure(self):
        mock_plugin = MagicMock(return_value=PanoptesTestPluginRaiseException)
        mock_get_context = MagicMock(return_value=self._panoptes_context)
        with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginManager.getPluginByName',
                   mock_plugin):
            with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginRunner._get_context', mock_get_context):
                runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                           None, self._panoptes_context, PanoptesTestKeyValueStore,
                                           PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                           PanoptesMetricsGroupSet, _callback)
                runner.execute_plugin()

    def test_plugin_wrong_result_type(self):
        runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                   None, self._panoptes_context, PanoptesTestKeyValueStore,
                                   PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                   PanoptesMetric, _callback)
        runner.execute_plugin()

    def test_logging_methods(self):
        runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                      None, self._panoptes_context, PanoptesTestKeyValueStore,
                                      PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                      PanoptesMetricsGroupSet, _callback)

        #  Ensure logging methods run:
        runner.info(PanoptesTestPlugin(), "Test Info log message")
        runner.warn(PanoptesTestPlugin(), "Test Warning log message")
        runner.error(PanoptesTestPlugin(), "Test Error log message", Exception)
        runner.exception(PanoptesTestPlugin(), "Test Exception log message")


class TestPanoptesPluginWithEnrichmentRunner(TestPanoptesPluginRunner):
    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = _get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                                 key_value_store_class_list=[PanoptesTestKeyValueStore],
                                                 create_message_producer=False, async_message_producer=False,
                                                 create_zookeeper_client=True)
        self._panoptes_resource = PanoptesResource(resource_site="test", resource_class="test", resource_subclass="test",
                                                   resource_type="test", resource_id="test", resource_endpoint="test",
                                                   resource_creation_timestamp=_TIMESTAMP, resource_plugin="test")
        self._runner_class = PanoptesPluginWithEnrichmentRunner

    def test_basic_operations(self):
        super(TestPanoptesPluginWithEnrichmentRunner, self).test_basic_operations()

        # Test with enrichment
        runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                    self._panoptes_resource, self._panoptes_context, PanoptesTestKeyValueStore,
                                    PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                    PanoptesMetricsGroupSet, _callback)
        runner.execute_plugin()

        # Test where enrichment is None
        runner = self._runner_class("Test Polling Plugin Second Instance", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                    self._panoptes_resource, self._panoptes_context, PanoptesTestKeyValueStore,
                                    PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                    PanoptesMetricsGroupSet, _callback)
        runner.execute_plugin()

    def test_lock(self):
        mock_get_plugin_by_name = MagicMock(return_value=MockPluginLockNone())
        mock_get_context = MagicMock(return_value=self._panoptes_context)
        with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginManager.getPluginByName',
                   mock_get_plugin_by_name):
            with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginWithEnrichmentRunner._get_context', mock_get_context):
                runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                           None, self._panoptes_context, PanoptesTestKeyValueStore,
                                           PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                           PanoptesMetricsGroupSet, _callback)
                runner.execute_plugin()

    def test_lock_error(self):
        mock_plugin = MagicMock(return_value=PanoptesTestPlugin)
        mock_get_context = MagicMock(return_value=self._panoptes_context)
        with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginManager.getPluginByName',
                   mock_plugin):
            with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginWithEnrichmentRunner._get_context', mock_get_context):
                runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                           None, self._panoptes_context, PanoptesTestKeyValueStore,
                                           PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                           PanoptesMetricsGroupSet, _callback)
                runner.execute_plugin()

    def test_plugin_failure(self):
        mock_plugin = MagicMock(return_value=PanoptesTestPluginRaiseException)
        mock_get_context = MagicMock(return_value=self._panoptes_context)
        with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginManager.getPluginByName',
                   mock_plugin):
            with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginWithEnrichmentRunner._get_context', mock_get_context):
                runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                           None, self._panoptes_context, PanoptesTestKeyValueStore,
                                           PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                           PanoptesMetricsGroupSet, _callback)
                runner.execute_plugin()

    def test_plugin_wrong_result_type(self):
        runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                   None, self._panoptes_context, PanoptesTestKeyValueStore,
                                   PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                   PanoptesMetric, _callback)
        runner.execute_plugin()

    def test_logging_methods(self):
        runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                      None, self._panoptes_context, PanoptesTestKeyValueStore,
                                      PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                      PanoptesMetricsGroupSet, _callback)

        #  Ensure logging methods run:
        runner.info(PanoptesTestPlugin(), "Test Info log message")
        runner.warn(PanoptesTestPlugin(), "Test Warning log message")
        runner.error(PanoptesTestPlugin(), "Test Error log message", Exception)
        runner.exception(PanoptesTestPlugin(), "Test Exception log message")

def _get_test_conf_file():
    my_dir = os.path.dirname(os.path.realpath(__file__))
    panoptes_test_conf_file = os.path.join(my_dir, 'config_files/test_panoptes_config.ini')

    return my_dir, panoptes_test_conf_file
