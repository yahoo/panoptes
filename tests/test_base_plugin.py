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
    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                                 key_value_store_class_list=[PanoptesTestKeyValueStore],
                                                 create_message_producer=False, async_message_producer=False,
                                                 create_zookeeper_client=True)
        self._panoptes_resource = PanoptesResource(resource_site='test', resource_class='test',
                                                   resource_subclass='test',
                                                   resource_type='test', resource_id='test', resource_endpoint='test',
                                                   resource_plugin='test')

    def test_plugin_info_repr(self):
        panoptes_plugin_info = PanoptesPluginInfo("plugin_name", "path/to/plugin")
        panoptes_plugin_info.panoptes_context = self._panoptes_context
        panoptes_plugin_info.data = self._panoptes_resource
        panoptes_plugin_info.kv_store_class = PanoptesTestKeyValueStore
        panoptes_plugin_info.last_executed = _LAST_EXECUTED_TEST_VALUE
        panoptes_plugin_info.last_results = _LAST_RESULTS_TEST_VALUE

        repr_string = "PanoptesPluginInfo: Normalized name: plugin__name, Config file: None, " \
                      "Panoptes context: [PanoptesContext: KV Stores: [PanoptesTestKeyValueStore], " \
                      "Config: ConfigObj({'main': {'sites': ['local'], " \
                      "'plugins_extension': 'panoptes-plugin', 'plugins_skew': 1}, " \
                      "'log': {'config_file': 'tests/config_files/test_panoptes_logging.ini', 'rate': 1000, " \
                      "'per': 1, 'burst': 10000, " \
                      "'formatters': {'keys': ['root_log_format', 'log_file_format', 'discovery_plugins_format']}}, " \
                      "'redis': {'default': {'namespace': 'panoptes', 'shards': {'shard1': " \
                      "{'host': 'localhost', 'port': 6379, 'db': 0, 'password': '**'}}}}, " \
                      "'kafka': {'topic_key_delimiter': ':', 'topic_name_delimiter': '-', " \
                      "'brokers': {'broker1': {'host': 'localhost', 'port': 9092}}, " \
                      "'topics': {'metrics': {'raw_topic_name_suffix': 'metrics', " \
                      "'transformed_topic_name_suffix': 'processed'}}}, " \
                      "'zookeeper': {'connection_timeout': 30, 'servers': {'server1': " \
                      "{'host': 'localhost', 'port': 2181}}}, 'discovery': {" \
                      "'plugins_paths': ['tests/plugins/discovery'], 'plugin_scan_interval': 60, " \
                      "'celerybeat_max_loop_interval': 5}, " \
                      "'polling': {'plugins_paths': ['tests/plugins/polling'], 'plugin_scan_interval': 60, " \
                      "'celerybeat_max_loop_interval': 5}, " \
                      "'enrichment': {'plugins_paths': ['tests/plugins/enrichment'], " \
                      "'plugin_scan_interval': 60, 'celerybeat_max_loop_interval': 5}, " \
                      "'snmp': {'port': 10161, 'connection_factory_module': " \
                      "'yahoo_panoptes.framework.utilities.snmp.connection', 'connection_factory_class': " \
                      "'PanoptesSNMPConnectionFactory', 'community': '**', 'timeout': 5, 'retries': 1, " \
                      "'non_repeaters': 0, 'max_repetitions': 25, 'proxy_port': 10161, " \
                      "'community_string_key': 'snmp_community_string'}}), Redis pool set: False, " \
                      "Message producer set: False, Kafka client set: False, Zookeeper client set: False], " \
                      "KV store class: PanoptesTestKeyValueStore, Last executed timestamp: 1458947997, " \
                      "Last executed key: plugin_metadata:plugin__name:" \
                      "61547fbb304169f2a076016678bc9cca:last_executed, " \
                      "Last results timestamp: 1458948005, " \
                      "Last results key: plugin_metadata:plugin__name:" \
                      "61547fbb304169f2a076016678bc9cca:last_results, " \
                      "Data: Data object passed, " \
                      "Lock: Lock is set"
        self.assertEqual(repr(panoptes_plugin_info), repr_string)

    def test_plugin_info_moduleMtime(self):
        panoptes_plugin_info = PanoptesPluginInfo("plugin_name", "path/to/plugin")
        panoptes_plugin_info.path = self.my_dir

        self.assertEqual(panoptes_plugin_info.moduleMtime, get_module_mtime(self.my_dir))

        panoptes_plugin_info.path = 0
        with self.assertRaises(PanoptesPluginConfigurationError):
            panoptes_plugin_info.moduleMtime

    def test_plugin_info_configMtime(self):
        panoptes_plugin_info = PanoptesPluginInfo("plugin_name", "path/to/plugin")
        panoptes_plugin_info.config_filename = self.my_dir
        self.assertEqual(panoptes_plugin_info.configMtime, int(os.path.getmtime(self.my_dir)))

        panoptes_plugin_info.config_filename = "/non/existent/file"
        with self.assertRaises(PanoptesPluginConfigurationError):
            panoptes_plugin_info.configMtime

    def test_plugin_info_last_properties(self):
        panoptes_plugin_info = PanoptesPluginInfo("Test Polling Plugin",
                                                  "tests/plugins/polling/test")

        #  Test last_results and last_executed return 0 on exception.
        with self.assertRaises(AssertionError):
            panoptes_plugin_info.last_executed = "test"
        self.assertEqual(panoptes_plugin_info.last_executed, 0)

        with self.assertRaises(AssertionError):
            panoptes_plugin_info.last_results = "test"
        self.assertEqual(panoptes_plugin_info.last_results, 0)

    def test_plugin_info_properties(self):
        panoptes_plugin_info = PanoptesPluginInfo("Test Polling Plugin",
                                                  "tests/plugins/polling/test")

        with self.assertRaises(PanoptesPluginConfigurationError):
            panoptes_plugin_info.panoptes_context

        panoptes_plugin_info.panoptes_context = self._panoptes_context
        panoptes_plugin_info.kv_store_class = PanoptesTestKeyValueStore
        panoptes_plugin_info.last_executed = _LAST_EXECUTED_TEST_VALUE
        panoptes_plugin_info.config_filename = "tests/plugins/polling/test/plugin_polling_test.panoptes-plugin"

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

        with patch("yahoo_panoptes.framework.plugins.panoptes_base_plugin.time.time", mock_time):
            self.assertEqual(panoptes_plugin_info.last_executed_age, int(_TIMESTAMP - _LAST_EXECUTED_TEST_VALUE))

    def test_plugin_info_last_results(self):
        panoptes_plugin_info = PanoptesPluginInfo("plugin_name", "path/to/plugin")
        panoptes_plugin_info.panoptes_context = self._panoptes_context
        panoptes_plugin_info.kv_store_class = PanoptesTestKeyValueStore

        panoptes_plugin_info.last_results = _LAST_RESULTS_TEST_VALUE
        self.assertEqual(panoptes_plugin_info.last_results, _LAST_RESULTS_TEST_VALUE)

        # Test value is cached.
        self.assertEqual(panoptes_plugin_info.last_results, _LAST_RESULTS_TEST_VALUE)

        #  Test last_results setter handles exception.
        with patch('yahoo_panoptes.framework.plugins.panoptes_base_plugin.PanoptesPluginInfo.metadata_kv_store',
                   mock_metadata_kv_store):
            panoptes_plugin_info.last_results = 1
            self.assertNotEqual(panoptes_plugin_info.last_results, 1)
            self.assertEqual(panoptes_plugin_info.last_results, _LAST_RESULTS_TEST_VALUE)

        #  Test last results age.
        with patch("yahoo_panoptes.framework.plugins.panoptes_base_plugin.time.time", mock_time):
            self.assertEqual(panoptes_plugin_info.last_results_age, int(_TIMESTAMP - _LAST_RESULTS_TEST_VALUE))

    def test_plugin_info_last_executed(self):
        panoptes_plugin_info = PanoptesPluginInfo("plugin_name", "path/to/plugin")
        panoptes_plugin_info.panoptes_context = self._panoptes_context
        panoptes_plugin_info.kv_store_class = PanoptesTestKeyValueStore

        #  Test last_executed setter handles exception.
        with patch('yahoo_panoptes.framework.plugins.panoptes_base_plugin.PanoptesPluginInfo.metadata_kv_store',
                   mock_metadata_kv_store):
            panoptes_plugin_info.last_executed = 1
            self.assertNotEqual(panoptes_plugin_info.last_executed, 1)
            self.assertEqual(panoptes_plugin_info.last_executed, 0)

    def test_plugin_info_execute_now(self):
        with patch("yahoo_panoptes.framework.plugins.panoptes_base_plugin.time.time", mock_time):
            panoptes_plugin_info = PanoptesPluginInfo("plugin_name", "path/to/plugin")
            panoptes_plugin_info.panoptes_context = self._panoptes_context
            panoptes_plugin_info.kv_store_class = PanoptesTestKeyValueStore
            panoptes_plugin_info.config_filename = "tests/plugins/polling/test/plugin_polling_test.panoptes-plugin"
            panoptes_plugin_info.details.read(panoptes_plugin_info.config_filename)

            mock_moduleMtime = _TIMESTAMP - 1
            mock_configMtime = _TIMESTAMP - 2
            with patch('yahoo_panoptes.framework.plugins.panoptes_base_plugin.PanoptesPluginInfo.configMtime',
                       mock_configMtime):
                with patch('yahoo_panoptes.framework.plugins.panoptes_base_plugin.PanoptesPluginInfo.moduleMtime',
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
        panoptes_plugin_info = PanoptesPluginInfo("plugin_name", "path/to/plugin")
        panoptes_plugin_info.panoptes_context = self._panoptes_context
        self.assertIsNotNone(panoptes_plugin_info.lock)
        self.assertTrue(panoptes_plugin_info.lock.locked)

        #  Assert lock is cached
        self.assertIsNotNone(panoptes_plugin_info.lock)
        self.assertTrue(panoptes_plugin_info.lock.locked)

        panoptes_plugin_info_2 = PanoptesPluginInfo("plugin_name", "path/to/plugin")
        panoptes_plugin_info_2.panoptes_context = self._panoptes_context

        #  Patch timeout to speed up test
        with patch('yahoo_panoptes.framework.plugins.panoptes_base_plugin.const.PLUGIN_AGENT_LOCK_ACQUIRE_TIMEOUT', 1):
            self.assertFalse(panoptes_plugin_info_2.lock.locked)
