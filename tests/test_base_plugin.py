"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""

import collections
import glob
import json
import os
import time
import unittest
from logging import getLogger, _loggerClass

from mock import patch, Mock, MagicMock
from yapsy.PluginInfo import PluginInfo

from yahoo_panoptes.framework.plugins.panoptes_base_plugin import PanoptesPluginInfo, PanoptesPluginInfoValidators, \
    PanoptesPluginConfigurationError
from yahoo_panoptes.polling.polling_plugin import PanoptesPollingPluginInfo
from yahoo_panoptes.framework.resources import PanoptesResource, PanoptesContext
from yahoo_panoptes.framework.utilities.helpers import get_module_mtime

from .test_framework import PanoptesTestKeyValueStore, panoptes_mock_kazoo_client, panoptes_mock_redis_strict_client

_TIMESTAMP = round(time.time(), 5)


class TestPanoptesPluginInfoValidators(unittest.TestCase):
    def test_valid_plugin_info_class(self):
        self.assertFalse(PanoptesPluginInfoValidators.valid_plugin_info_class(None))
        self.assertTrue(PanoptesPluginInfoValidators.valid_plugin_info_class(PanoptesPollingPluginInfo))
        self.assertTrue(PanoptesPluginInfoValidators.valid_plugin_info_class(PanoptesPluginInfo))
        self.assertFalse(PanoptesPluginInfoValidators.valid_plugin_info_class(PluginInfo))


class TestPanoptesPluginInfo(unittest.TestCase):
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = _get_test_conf_file()

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_plugin_info_repr(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                           key_value_store_class_list=[PanoptesTestKeyValueStore],
                                           create_message_producer=False, async_message_producer=False,
                                           create_zookeeper_client=True)

        panoptes_resource = PanoptesResource(resource_site='test', resource_class='test',
                                             resource_subclass='test',
                                             resource_type='test', resource_id='test', resource_endpoint='test',
                                             resource_plugin='test')

        panoptes_plugin_info = PanoptesPluginInfo("plugin_name", "plugin_path")
        panoptes_plugin_info.panoptes_context = panoptes_context
        panoptes_plugin_info.data = panoptes_resource
        panoptes_plugin_info.kv_store_class = PanoptesTestKeyValueStore
        panoptes_plugin_info.last_executed = "1458947997"
        panoptes_plugin_info.last_results = "1458948005"

        repr_string = "PanoptesPluginInfo: Normalized name: plugin__name, Config file: None, "\
                      "Panoptes context: " \
                      "[PanoptesContext: KV Stores: [PanoptesTestKeyValueStore], "\
                      "Config: ConfigObj({'main': {'sites': " \
                      "['local'], 'plugins_extension': 'panoptes-plugin', 'plugins_skew': 1}, " \
                      "'log': " \
                      "{'config_file': 'tests/config_files/test_panoptes_logging.ini', " \
                      "'rate': 1000, " \
                      "'per': 1, " \
                      "'burst': 10000, " \
                      "'formatters': {'keys': ['root_log_format', 'log_file_format', 'discovery_plugins_format']}}, " \
                      "'redis': {'default': {'namespace': 'panoptes', "\
                      "'shards': {'shard1': {'host': 'localhost', 'port': 6379, 'db': 0, 'password': '**'}}}}, "\
                      "'kafka': {'topic_key_delimiter': ':', 'topic_name_delimiter': '-', " \
                      "'brokers': {'broker1': {'host': 'localhost', 'port': 9092}}, " \
                      "'topics': " \
                      "{'metrics': {'raw_topic_name_suffix': 'metrics', " \
                      "'transformed_topic_name_suffix': 'processed'}}}, " \
                      "'zookeeper': {'connection_timeout': 30, 'servers': {'server1': {'host': " \
                      "'localhost', 'port': 2181}}}, " \
                      "'discovery': " \
                      "{'plugins_path': 'tests/plugins/discovery', " \
                      "'plugin_scan_interval': 60, " \
                      "'celerybeat_max_loop_interval': 5}, " \
                      "'polling': " \
                      "{'plugins_path': 'tests/plugins/polling', " \
                      "'plugin_scan_interval': 60, " \
                      "'celerybeat_max_loop_interval': 5}, " \
                      "'enrichment': " \
                      "{'plugins_path': 'tests/plugins/enrichment', " \
                      "'plugin_scan_interval': 60, " \
                      "'celerybeat_max_loop_interval': 5}, " \
                      "'snmp': " \
                      "{'port': 10161, " \
                      "'connection_factory_module': 'yahoo_panoptes.framework.utilities.snmp.connection', " \
                      "'connection_factory_class': 'PanoptesSNMPConnectionFactory', " \
                      "'community': '**', 'timeout': 5, 'retries': 1, 'non_repeaters': 0, 'max_repetitions': 25, " \
                      "'proxy_port': 10161, 'community_string_key': 'snmp_community_string'}}), "\
                      "Redis pool set: False, " \
                      "Message producer set: False, " \
                      "Kafka client set: False, " \
                      "Zookeeper client set: False], " \
                      "KV store class: PanoptesTestKeyValueStore, " \
                      "Last executed timestamp: 1458947997, " \
                      "Last executed key: " \
                      "plugin_metadata:plugin__name:be7eabbca3b05b9aaa8c81201aa0ca3e:last_executed, " \
                      "Last results timestamp: 1458948005, " \
                      "Last results key: plugin_metadata:plugin__name:be7eabbca3b05b9aaa8c81201aa0ca3e:last_results, " \
                      "Data: Data object passed, " \
                      "Lock: Lock is set"
        self.assertEqual(repr(panoptes_plugin_info), repr_string)

    def test_plugin_info_moduleMtime(self):
        panoptes_plugin_info = PanoptesPluginInfo("plugin_name", "plugin_path")
        panoptes_plugin_info.path = self.my_dir

        self.assertEqual(panoptes_plugin_info.moduleMtime, get_module_mtime(self.my_dir))

        panoptes_plugin_info.path = 0
        with self.assertRaises(PanoptesPluginConfigurationError):
            panoptes_plugin_info.moduleMtime

    def test_plugin_info_configMtime(self):
        panoptes_plugin_info = PanoptesPluginInfo("plugin_name", "plugin_path")
        panoptes_plugin_info.config_filename = self.my_dir
        self.assertEqual(panoptes_plugin_info.configMtime, int(os.path.getmtime(self.my_dir)))

        panoptes_plugin_info.config_filename = "/non/existent/file"
        with self.assertRaises(PanoptesPluginConfigurationError):
            panoptes_plugin_info.configMtime

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_plugin_info_execute_frequency(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                           key_value_store_class_list=[PanoptesTestKeyValueStore],
                                           create_message_producer=False, async_message_producer=False,
                                           create_zookeeper_client=True)
        panoptes_plugin_info = PanoptesPluginInfo("plugin_name", "plugin_path")
        panoptes_plugin_info.panoptes_context = panoptes_context
        panoptes_plugin_info.kv_store_class = PanoptesTestKeyValueStore

        print "######: %s" % panoptes_plugin_info.execute_frequency

def _get_test_conf_file():
    my_dir = os.path.dirname(os.path.realpath(__file__))
    panoptes_test_conf_file = os.path.join(my_dir, 'config_files/test_panoptes_config.ini')

    return my_dir, panoptes_test_conf_file