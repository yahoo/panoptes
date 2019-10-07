import os

import unittest

from yahoo_panoptes.plugins.enrichment.interface.arista.plugin_enrichment_interface_arista import \
    PluginEnrichmentAristaInterface
from tests.plugins.helpers import setup_module_default, tear_down_module_default, SNMPEnrichmentPluginTestFramework

pwd = os.path.dirname(os.path.abspath(__file__))


def setUpModule():
    return setup_module_default(plugin_pwd=pwd)


def tearDownModule():
    return tear_down_module_default()


class TestAristaInterfaceEnrichmentPlugin(SNMPEnrichmentPluginTestFramework, unittest.TestCase):
    resource_id = 'switch1'
    resource_plugin = 'switch_discovery_plugin'
    resource_site = 'dc1'
    resource_class = 'network'
    resource_subclass = 'switch'
    resource_type = 'arista'

    plugin_class = PluginEnrichmentAristaInterface
    path = pwd
