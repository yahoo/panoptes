import os

import unittest

from yahoo_panoptes.plugins.enrichment.interface.juniper.plugin_enrichment_interface_juniper import \
    PluginEnrichmentJuniperInterface
from tests.plugins.helpers import setup_module_default, tear_down_module_default, SNMPEnrichmentPluginTestFramework

pwd = os.path.dirname(os.path.abspath(__file__))

def setUpModule():
    return setup_module_default(plugin_pwd=pwd)


def tearDownModule():
    return tear_down_module_default()


class TestJuniperQFX5200InterfaceEnrichmentPlugin(SNMPEnrichmentPluginTestFramework, unittest.TestCase):
    resource_id = 'router1'
    resource_plugin = 'router_discovery_plugin'
    resource_site = 'dc1'
    resource_class = 'network'
    resource_subclass = 'router'
    resource_type = 'juniper'

    plugin_class = PluginEnrichmentJuniperInterface
    path = pwd

    def setUp(self):
        super(TestJuniperQFX5200InterfaceEnrichmentPlugin, self).setUp()
        self._panoptes_resource.resource_metadata['model'] = 'QFX5200'
