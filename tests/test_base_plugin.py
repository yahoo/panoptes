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
from yahoo_panoptes.polling.polling_plugin import PanoptesPollingPluginInfo, PanoptesPollingPluginConfigurationError
from yahoo_panoptes.framework.resources import PanoptesResource, PanoptesContext
from yahoo_panoptes.framework.utilities.helpers import get_module_mtime

from .test_framework import PanoptesTestKeyValueStore, panoptes_mock_kazoo_client, panoptes_mock_redis_strict_client
from .helpers import get_test_conf_file

_TIMESTAMP = round(time.time(), 5)
_LAST_EXECUTED_TEST_VALUE = 1458947997
_LAST_RESULTS_TEST_VALUE = 1458948005

mock_time = MagicMock(return_value=_TIMESTAMP)


class TestPanoptesBasePluginSubclass(PanoptesBasePlugin):
    def run(self, context):
        pass


class TestPanoptesBasePluginValidators(unittest.TestCase):
    def test_valid_plugin_class(self):
        self.assertFalse(PanoptesBasePluginValidators.valid_plugin_class(None))
        self.assertFalse(PanoptesBasePluginValidators.valid_plugin_class(PanoptesContext))
        self.assertTrue(PanoptesBasePluginValidators.valid_plugin_class(PanoptesBasePlugin))
        self.assertTrue(PanoptesBasePluginValidators.valid_plugin_class(TestPanoptesBasePluginSubclass))


class TestPanoptesPluginInfoValidators(unittest.TestCase):
    def test_valid_plugin_info_class(self):
        self.assertFalse(PanoptesPluginInfoValidators.valid_plugin_info_class(None))
        self.assertTrue(PanoptesPluginInfoValidators.valid_plugin_info_class(PanoptesPollingPluginInfo))
        self.assertTrue(PanoptesPluginInfoValidators.valid_plugin_info_class(PanoptesPluginInfo))
        self.assertFalse(PanoptesPluginInfoValidators.valid_plugin_info_class(PluginInfo))


def mock_metadata_kv_store():
    return MagicMock(side_effect=Exception)


class TestPanoptesPluginInfo(unittest.TestCase):
    @patch(u'redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch(u'kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                                 key_value_store_class_list=[PanoptesTestKeyValueStore],
                                                 create_message_producer=False, async_message_producer=False,
                                                 create_zookeeper_client=True)
        self._panoptes_resource = PanoptesResource(resource_site=u'test', resource_class=u'test',
                                                   resource_subclass=u'test',
                                                   resource_type=u'test', resource_id=u'test',
                                                   resource_endpoint=u'test',
                                                   resource_plugin=u'test')

    def test_plugin_info_moduleMtime(self):
        panoptes_plugin_info = PanoptesPluginInfo(u"plugin_name", u"path/to/plugin")
        panoptes_plugin_info.path = self.my_dir

        self.assertEqual(panoptes_plugin_info.moduleMtime, get_module_mtime(self.my_dir))

        with self.assertRaises(Exception):
            panoptes_plugin_info.path = 0
            panoptes_plugin_info.moduleMtime

    def test_plugin_info_configMtime(self):
        panoptes_plugin_info = PanoptesPluginInfo(u"plugin_name", u"path/to/plugin")
        panoptes_plugin_info.config_filename = self.my_dir
        self.assertEqual(panoptes_plugin_info.configMtime, int(os.path.getmtime(self.my_dir)))

        panoptes_plugin_info.config_filename = u"/non/existent/file"
        with self.assertRaises(PanoptesPluginConfigurationError):
            panoptes_plugin_info.configMtime

    def test_plugin_info_last_properties(self):
        panoptes_plugin_info = PanoptesPluginInfo(u"Test Polling Plugin",
                                                  u"tests/plugins/polling/test")

        #  Test last_results and last_executed return 0 on exception.
        with self.assertRaises(AssertionError):
            panoptes_plugin_info.last_executed = u"test"
        self.assertEqual(panoptes_plugin_info.last_executed, 0)

        with self.assertRaises(AssertionError):
            panoptes_plugin_info.last_results = u"test"
        self.assertEqual(panoptes_plugin_info.last_results, 0)

    def test_plugin_info_properties(self):
        panoptes_plugin_info = PanoptesPluginInfo(u"Test Polling Plugin",
                                                  u"tests/plugins/polling/test")

        with self.assertRaises(PanoptesPluginConfigurationError):
            panoptes_plugin_info.panoptes_context

        panoptes_plugin_info.panoptes_context = self._panoptes_context
        panoptes_plugin_info.kv_store_class = PanoptesTestKeyValueStore
        panoptes_plugin_info.last_executed = _LAST_EXECUTED_TEST_VALUE
        panoptes_plugin_info.config_filename = u"tests/plugins/polling/test/plugin_polling_test.panoptes-plugin"

        #  Test results_cache_age, execute_frequency, last_executed return 0 on exception.
        self.assertEqual(panoptes_plugin_info.results_cache_age, 0)
        self.assertEqual(panoptes_plugin_info.execute_frequency, 0)

        panoptes_plugin_info.details.read(panoptes_plugin_info.config_filename)

        self.assertEqual(panoptes_plugin_info.results_cache_age, 100)
        self.assertEqual(panoptes_plugin_info.execute_frequency, 60)

        #  Test last_executed setter cannot be reset after being read
        self.assertEqual(panoptes_plugin_info.last_executed, _LAST_EXECUTED_TEST_VALUE)
        panoptes_plugin_info.last_executed = 1
        self.assertNotEqual(panoptes_plugin_info.last_executed, 1)
        self.assertEqual(panoptes_plugin_info.last_executed, _LAST_EXECUTED_TEST_VALUE)

        with patch(u"yahoo_panoptes.framework.plugins.panoptes_base_plugin.time.time", mock_time):
            self.assertEqual(panoptes_plugin_info.last_executed_age, int(_TIMESTAMP - _LAST_EXECUTED_TEST_VALUE))

    def test_plugin_info_last_results(self):
        panoptes_plugin_info = PanoptesPluginInfo(u"plugin_name", u"path/to/plugin")
        panoptes_plugin_info.panoptes_context = self._panoptes_context
        panoptes_plugin_info.kv_store_class = PanoptesTestKeyValueStore

        panoptes_plugin_info.last_results = _LAST_RESULTS_TEST_VALUE
        self.assertEqual(panoptes_plugin_info.last_results, _LAST_RESULTS_TEST_VALUE)

        # Test value is cached.
        self.assertEqual(panoptes_plugin_info.last_results, _LAST_RESULTS_TEST_VALUE)

        #  Test last_results setter handles exception.
        with patch(u'yahoo_panoptes.framework.plugins.panoptes_base_plugin.PanoptesPluginInfo.metadata_kv_store',
                   mock_metadata_kv_store):
            panoptes_plugin_info.last_results = 1
            self.assertNotEqual(panoptes_plugin_info.last_results, 1)
            self.assertEqual(panoptes_plugin_info.last_results, _LAST_RESULTS_TEST_VALUE)

        #  Test last results age.
        with patch(u"yahoo_panoptes.framework.plugins.panoptes_base_plugin.time.time", mock_time):
            self.assertEqual(panoptes_plugin_info.last_results_age, int(_TIMESTAMP - _LAST_RESULTS_TEST_VALUE))

    def test_plugin_info_last_executed(self):
        panoptes_plugin_info = PanoptesPluginInfo(u"plugin_name", u"path/to/plugin")
        panoptes_plugin_info.panoptes_context = self._panoptes_context
        panoptes_plugin_info.kv_store_class = PanoptesTestKeyValueStore

        #  Test last_executed setter handles exception.
        with patch(u'yahoo_panoptes.framework.plugins.panoptes_base_plugin.PanoptesPluginInfo.metadata_kv_store',
                   mock_metadata_kv_store):
            panoptes_plugin_info.last_executed = 1
            self.assertNotEqual(panoptes_plugin_info.last_executed, 1)
            self.assertEqual(panoptes_plugin_info.last_executed, 0)

    def test_plugin_info_execute_now(self):
        with patch(u"yahoo_panoptes.framework.plugins.panoptes_base_plugin.time.time", mock_time):
            panoptes_plugin_info = PanoptesPluginInfo(u"plugin_name", u"path/to/plugin")
            panoptes_plugin_info.panoptes_context = self._panoptes_context
            panoptes_plugin_info.kv_store_class = PanoptesTestKeyValueStore
            panoptes_plugin_info.config_filename = u"tests/plugins/polling/test/plugin_polling_test.panoptes-plugin"
            panoptes_plugin_info.details.read(panoptes_plugin_info.config_filename)

            mock_moduleMtime = _TIMESTAMP - 1
            mock_configMtime = _TIMESTAMP - 2
            with patch(u'yahoo_panoptes.framework.plugins.panoptes_base_plugin.PanoptesPluginInfo.configMtime',
                       mock_configMtime):
                with patch(u'yahoo_panoptes.framework.plugins.panoptes_base_plugin.PanoptesPluginInfo.moduleMtime',
                           mock_moduleMtime):
                    # Ensure first if-block in execute_now returns False
                    panoptes_plugin_info.last_results = int(_TIMESTAMP)
                    panoptes_plugin_info.last_executed = int(_TIMESTAMP)
                    self.assertFalse(panoptes_plugin_info.execute_now)

                    # Ensure second if-block in execute_now returns False
                    panoptes_plugin_info._last_results = None
                    panoptes_plugin_info._last_executed = None

                    panoptes_plugin_info.last_results = int(_TIMESTAMP)
                    panoptes_plugin_info.last_executed = (int(_TIMESTAMP) - panoptes_plugin_info.execute_frequency)
                    self.assertFalse(panoptes_plugin_info.execute_now)

                    # Ensure returns True
                    panoptes_plugin_info._last_results = None
                    panoptes_plugin_info._last_executed = None

                    panoptes_plugin_info.last_results = (int(_TIMESTAMP) - panoptes_plugin_info.execute_frequency)
                    self.assertTrue(panoptes_plugin_info.execute_now)

    def test_plugin_info_lock(self):
        panoptes_plugin_info = PanoptesPluginInfo(u"plugin_name", u"path/to/plugin")
        panoptes_plugin_info.panoptes_context = self._panoptes_context
        self.assertIsNotNone(panoptes_plugin_info.lock)
        self.assertTrue(panoptes_plugin_info.lock.locked)

        #  Assert lock is cached
        self.assertIsNotNone(panoptes_plugin_info.lock)
        self.assertTrue(panoptes_plugin_info.lock.locked)

        panoptes_plugin_info_2 = PanoptesPluginInfo(u"plugin_name", u"path/to/plugin")
        panoptes_plugin_info_2.panoptes_context = self._panoptes_context

        #  Patch timeout to speed up test
        with patch(u'yahoo_panoptes.framework.plugins.panoptes_base_plugin.const.PLUGIN_AGENT_LOCK_ACQUIRE_TIMEOUT', 1):
            self.assertFalse(panoptes_plugin_info_2.lock.locked)

    def test_resource_filter(self):
        panoptes_plugin_info = PanoptesPollingPluginInfo(u"plugin_name", u"tests/plugins/polling/test")
        panoptes_plugin_info.panoptes_context = self._panoptes_context

        with self.assertRaises(PanoptesPollingPluginConfigurationError):
            panoptes_plugin_info.resource_filter

        panoptes_plugin_info.kv_store_class = PanoptesTestKeyValueStore
        panoptes_plugin_info.config_filename = u"tests/plugins/polling/test/plugin_polling_test.panoptes-plugin"
        panoptes_plugin_info.details.read(panoptes_plugin_info.config_filename)

        self.assertEqual(panoptes_plugin_info.resource_filter, u'resource_class = "system" AND '
                                                               u'resource_subclass = "internal" '
                                                               u'AND resource_type = "test"')
