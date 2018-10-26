"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
import copy
import os
import time
import unittest

from mock import patch, MagicMock, create_autospec
from yapsy.PluginInfo import PluginInfo

from yahoo_panoptes.framework.plugins.panoptes_base_plugin import PanoptesPluginInfo, PanoptesPluginInfoValidators, \
    PanoptesPluginConfigurationError, PanoptesBasePluginValidators, PanoptesBasePlugin, PanoptesPluginRuntimeError
from yahoo_panoptes.framework.plugins.base_snmp_plugin import PanoptesSNMPBasePlugin
from yahoo_panoptes.framework.plugins.context import PanoptesPluginWithEnrichmentContext
from yahoo_panoptes.polling.polling_plugin import PanoptesPollingPluginInfo
from yahoo_panoptes.framework.plugins.base_snmp_plugin import PanoptesPluginConfigurationError
from yahoo_panoptes.framework.resources import PanoptesResource, PanoptesContext
from yahoo_panoptes.framework.utilities.helpers import get_module_mtime
from yahoo_panoptes.framework.utilities.snmp.connection import PanoptesSNMPV2Connection, PanoptesSNMPConnectionFactory

from ..test_framework import PanoptesTestKeyValueStore, panoptes_mock_kazoo_client, panoptes_mock_redis_strict_client
from ..plugins.helpers import SNMPPluginTestFramework
from ..helpers import get_test_conf_file

_TIMESTAMP = round(time.time(), 5)

mock_time = MagicMock(return_value=_TIMESTAMP)

plugin_conf = {
        'Core': {
            'name': 'Test Plugin',
            'module': 'test_plugin'
        },
        'main': {
            'execute_frequency': '60',
            'resource_filter': 'resource_class = "network"'
        },
        'enrichment': {
            'preload': 'self:interface'
        }
    }

bad_plugin_conf = {
        'Core': {
            'name': 'Test Plugin',
            'module': 'test_plugin'
        },
        'enrichment': {
            'preload': 'self:interface'
        }
    }


def mock_metadata_kv_store():
    return MagicMock(side_effect=Exception)


class TestPanoptesSNMPBasePlugin(SNMPPluginTestFramework, unittest.TestCase):
    plugin_class = PanoptesSNMPBasePlugin
    path = os.path.dirname(os.path.abspath(__file__))

    def test_basic_operations(self):
        panoptes_snmp_base_plugin = self.plugin_class()

        panoptes_snmp_base_plugin.run(self._plugin_context)

        self.assertEqual(panoptes_snmp_base_plugin.plugin_context, self._plugin_context)
        self.assertEqual(panoptes_snmp_base_plugin.plugin_config, self._plugin_context.config)
        self.assertEqual(panoptes_snmp_base_plugin.resource, self._plugin_context.data)
        self.assertEqual(panoptes_snmp_base_plugin.enrichment, self._plugin_context.enrichment)
        self.assertEqual(panoptes_snmp_base_plugin.execute_frequency,
                         int(self._plugin_context.config['main']['execute_frequency']))
        self.assertEqual(panoptes_snmp_base_plugin.host, self._plugin_context.data.resource_endpoint)
        self.assertIsInstance(panoptes_snmp_base_plugin.snmp_connection, PanoptesSNMPV2Connection)

        # Test PanoptesSNMPPluginConfiguration Error
        mock_plugin_context = create_autospec(self._plugin_context)
        with self.assertRaises(PanoptesPluginConfigurationError):
            panoptes_snmp_base_plugin.run(mock_plugin_context)

        # Test Error in getting SNMP Connection
        mock_snmp_connection_class = create_autospec(PanoptesSNMPConnectionFactory)
        with patch('yahoo_panoptes.framework.plugins.base_snmp_plugin.PanoptesSNMPConnectionFactory',
                   mock_snmp_connection_class):
            with self.assertRaises(PanoptesPluginRuntimeError):
                panoptes_snmp_base_plugin.run(self._plugin_context)

        # Ensure log message hit if get_results does not return None

