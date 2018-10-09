import logging
import os
from unittest import TestCase

from mock import create_autospec

from yahoo_panoptes.framework.context import PanoptesContext
from yahoo_panoptes.framework.plugins.context import PanoptesPluginWithEnrichmentContext
from yahoo_panoptes.framework.resources import PanoptesResource
from yahoo_panoptes.framework.utilities.secrets import PanoptesSecretsStore
from yahoo_panoptes.framework.utilities.snmp.connection import PanoptesSNMPPluginConfiguration

panoptes_resource = PanoptesResource(
        resource_site='test_site',
        resource_class='test_class',
        resource_subclass='test_subclass',
        resource_type='test_type',
        resource_id='test_id',
        resource_endpoint='test_endpoint',
        resource_plugin='test_plugin'
)

plugin_conf = {
    'Core': {
        'name': 'Test Plugin',
        'module': 'test_plugin'
    },
    'main': {
        'execute_frequency': '60',
        'resource_filter': 'resource_class = "network"'
    }
}

path = os.path.dirname(os.path.realpath(__file__))
panoptes_context = PanoptesContext(config_file=os.path.join(path, 'config_files/test_panoptes_config.ini'))

secret_store = create_autospec(PanoptesSecretsStore, instance=True, spec_set=True)


class TestPanoptesSNMPPluginConfiguration(TestCase):
    def test_snmp_defaults(self):
        """Test that SNMP defaults from the Panoptes configuration file are used if no plugin specific SNMP config is
        provided and no site specific community secret is avalilable in the secrets store"""
        global secret_store

        secret_store.get_by_site.return_value = None

        plugin_context = create_autospec(
                PanoptesPluginWithEnrichmentContext, instance=True, spec_set=True,
                data=panoptes_resource,
                config=plugin_conf,
                snmp=panoptes_context.config_object.snmp_defaults,
                secrets=secret_store,
                logger=logging.getLogger(__name__)
        )

        snmp_configuration = PanoptesSNMPPluginConfiguration(plugin_context)

        self.assertEqual(snmp_configuration.port, 10161)
        self.assertEqual(snmp_configuration.proxy_port, 10161)
        self.assertEqual(snmp_configuration.connection_factory_module,
                         'yahoo_panoptes.framework.utilities.snmp.connection')
        self.assertEqual(snmp_configuration.connection_factory_class, 'PanoptesSNMPConnectionFactory')
        self.assertEqual(snmp_configuration.timeout, 5)
        self.assertEqual(snmp_configuration.retries, 1)
        self.assertEqual(snmp_configuration.non_repeaters, 0)
        self.assertEqual(snmp_configuration.max_repetitions, 25)
        self.assertEqual(snmp_configuration.community, 'public')
        self.assertEqual(snmp_configuration.community_string_key, 'snmp_community_string')

    def test_snmp_site_community(self):
        """
        Test that the site specific community string is used if present
        """
        global secret_store

        secret_store.get_by_site.return_value = 'test_site_community'

        plugin_context = create_autospec(
                PanoptesPluginWithEnrichmentContext, instance=True, spec_set=True,
                data=panoptes_resource,
                config=plugin_conf,
                snmp=panoptes_context.config_object.snmp_defaults,
                secrets=secret_store,
                logger=logging.getLogger(__name__)
        )

        snmp_configuration = PanoptesSNMPPluginConfiguration(plugin_context)
        self.assertEqual(snmp_configuration.community, 'test_site_community')

    def test_snmp_plugin_community(self):
        """
        Test that the plugin specific community string is used if present
        """
        global secret_store

        secret_store.get_by_site.return_value = 'test_site_community'

        plugin_conf_with_community = {
            'Core': {
                'name': 'Test Plugin',
                'module': 'test_plugin'
            },
            'main': {
                'execute_frequency': '60',
                'resource_filter': 'resource_class = "network"'
            },
            'snmp': {
                'community': 'test_plugin_community'
            }
        }

        plugin_context = create_autospec(
                PanoptesPluginWithEnrichmentContext, instance=True, spec_set=True,
                data=panoptes_resource,
                config=plugin_conf_with_community,
                snmp=panoptes_context.config_object.snmp_defaults,
                secrets=secret_store,
                logger=logging.getLogger(__name__)
        )

        snmp_configuration = PanoptesSNMPPluginConfiguration(plugin_context)
        self.assertEqual(snmp_configuration.community, 'test_plugin_community')

    def test_snmp_plugin_overrides(self):
        """
        Test that the plugin specific SNMP configuration is used if present
        """
        plugin_conf_with_overrides = {
            'Core': {
                'name': 'Test Plugin',
                'module': 'test_plugin'
            },
            'main': {
                'execute_frequency': '60',
                'resource_filter': 'resource_class = "network"'
            },
            'snmp': {
                'connection_factory_module': 'test_module',
                'connection_factory_class': 'test_class',
                'port': 162,
                'proxy_port': 10162,
                'timeout': 10,
                'retries': 2,
                'non_repeaters': 1,
                'max_repetitions': 10,
                'community_string_key': 'test_community_string_key'
            }
        }

        plugin_context = create_autospec(
                PanoptesPluginWithEnrichmentContext, instance=True, spec_set=True,
                data=panoptes_resource,
                config=plugin_conf_with_overrides,
                snmp=panoptes_context.config_object.snmp_defaults,
                secrets=secret_store,
                logger=logging.getLogger(__name__)
        )

        snmp_configuration = PanoptesSNMPPluginConfiguration(plugin_context)
        self.assertEqual(snmp_configuration.port, 162)
        self.assertEqual(snmp_configuration.proxy_port, 10162)
        self.assertEqual(snmp_configuration.connection_factory_module, 'test_module')
        self.assertEqual(snmp_configuration.connection_factory_class, 'test_class')
        self.assertEqual(snmp_configuration.timeout, 10)
        self.assertEqual(snmp_configuration.retries, 2)
        self.assertEqual(snmp_configuration.non_repeaters, 1)
        self.assertEqual(snmp_configuration.max_repetitions, 10)
        self.assertEqual(snmp_configuration.community_string_key, 'test_community_string_key')

    @staticmethod
    def _plugin_context_with_bad_snmp_configuration(**kwargs):
        plugin_conf_with_bad_configuration_value = {
            'Core': {
                'name': 'Test Plugin',
                'module': 'test_plugin'
            },
            'main': {
                'execute_frequency': '60',
                'resource_filter': 'resource_class = "network"'
            },
            'snmp': kwargs
        }

        return create_autospec(
                PanoptesPluginWithEnrichmentContext, instance=True, spec_set=True,
                data=panoptes_resource,
                config=plugin_conf_with_bad_configuration_value,
                snmp=panoptes_context.config_object.snmp_defaults,
                secrets=secret_store,
                logger=logging.getLogger(__name__)
        )

    def test_snmp_bad_configuration_values(self):
        """
        Test that 'bad' configuration values raise expected exceptions
        """
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(
                self._plugin_context_with_bad_snmp_configuration(connection_factory_module=0))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(
                self._plugin_context_with_bad_snmp_configuration(connection_factory_module=''))

        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(
                self._plugin_context_with_bad_snmp_configuration(connection_factory_class=0))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(
                self._plugin_context_with_bad_snmp_configuration(connection_factory_class=''))

        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(community=0))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(community=''))

        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(community_string_key=0))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(community_string_key=''))

        with self.assertRaises(ValueError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(port=''))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(port=-1))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(port=65536))

        with self.assertRaises(ValueError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(proxy_port=''))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(proxy_port=-1))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(proxy_port=65536))

        with self.assertRaises(ValueError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(timeout=''))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(timeout=0))

        with self.assertRaises(ValueError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(retries=''))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(retries=0))

        with self.assertRaises(ValueError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(non_repeaters=''))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(non_repeaters=-1))

        with self.assertRaises(ValueError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(max_repetitions=''))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(max_repetitions=0))
