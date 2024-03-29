import logging
import os
from unittest import TestCase

from mock import create_autospec, patch

from yahoo_panoptes.framework.context import PanoptesContext
from yahoo_panoptes.framework.plugins.context import PanoptesPluginWithEnrichmentContext
from yahoo_panoptes.framework.resources import PanoptesResource
from yahoo_panoptes.framework.utilities.secrets import PanoptesSecretsStore
from yahoo_panoptes.framework.utilities.snmp.connection import PanoptesSNMPPluginConfiguration
from yahoo_panoptes.plugins.helpers.snmp_connections import PanoptesSNMPSteamRollerAgentConnection

panoptes_resource = PanoptesResource(
        resource_site=u'test_site',
        resource_class=u'test_class',
        resource_subclass=u'test_subclass',
        resource_type=u'test_type',
        resource_id=u'test_id',
        resource_endpoint=u'test_endpoint',
        resource_plugin=u'test_plugin'
)

plugin_conf = {
    u'Core': {
        u'name': u'Test Plugin',
        u'module': u'test_plugin'
    },
    u'main': {
        u'execute_frequency': u'60',
        u'resource_filter': u'resource_class = "network"'
    }
}

path = os.path.dirname(os.path.realpath(__file__))
panoptes_context = PanoptesContext(config_file=os.path.join(path, u'config_files/test_panoptes_config.ini'))

secret_store = create_autospec(PanoptesSecretsStore, instance=True, spec_set=True)


class TestPanoptesSNMPPluginConfiguration(TestCase):
    def test_x509_defaults(self):
        """
        Test that x509 defaults from the Panoptes configuration file are used if no plugin specific SNMP config is
        provided
        """
        global secret_store

        secret_store.get_by_site.return_value = None

        plugin_context = create_autospec(
            PanoptesPluginWithEnrichmentContext, instance=True, spec_set=True,
            data=panoptes_resource,
            config=plugin_conf,
            snmp=panoptes_context.config_object.snmp_defaults,
            x509=panoptes_context.config_object.x509_defaults,
            secrets=secret_store,
            logger=logging.getLogger(__name__)
        )

        x509_configuration = PanoptesSNMPPluginConfiguration(plugin_context)

        self.assertEqual(x509_configuration.x509_secure_connection, 0)
        self.assertEqual(x509_configuration.x509_cert_file, u'/home/panoptes/x509/certs/panoptes.pem')
        self.assertEqual(x509_configuration.x509_key_file, u'/home/panoptes/x509/keys/panoptes.key')

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
                x509=panoptes_context.config_object.x509_defaults,
                secrets=secret_store,
                logger=logging.getLogger(__name__)
        )
        snmp_configuration = PanoptesSNMPPluginConfiguration(plugin_context)

        self.assertEqual(snmp_configuration.port, 10161)
        self.assertEqual(snmp_configuration.proxy_port, 10161)
        self.assertEqual(snmp_configuration.connection_factory_module,
                         u'yahoo_panoptes.plugins.helpers.snmp_connections')
        self.assertEqual(snmp_configuration.connection_factory_class, u'PanoptesSNMPConnectionFactory')
        self.assertEqual(snmp_configuration.timeout, 5)
        self.assertEqual(snmp_configuration.retries, 1)
        self.assertEqual(snmp_configuration.non_repeaters, 0)
        self.assertEqual(snmp_configuration.max_repetitions, 25)
        self.assertEqual(snmp_configuration.community, u'public')
        self.assertEqual(snmp_configuration.community_string_key, u'snmp_community_string')

    def test_snmp_site_community(self):
        """
        Test that the site specific community string is used if present
        """
        global secret_store

        secret_store.get_by_site.return_value = u'test_site_community'

        plugin_context = create_autospec(
                PanoptesPluginWithEnrichmentContext, instance=True, spec_set=True,
                data=panoptes_resource,
                config=plugin_conf,
                snmp=panoptes_context.config_object.snmp_defaults,
                x509=panoptes_context.config_object.x509_defaults,
                secrets=secret_store,
                logger=logging.getLogger(__name__)
        )

        snmp_configuration = PanoptesSNMPPluginConfiguration(plugin_context)
        self.assertEqual(snmp_configuration.community, u'test_site_community')

    def test_snmp_plugin_community(self):
        """
        Test that the plugin specific community string is used if present
        """
        global secret_store

        secret_store.get_by_site.return_value = u'test_site_community'

        plugin_conf_with_community = {
            u'Core': {
                u'name': 'Test Plugin',
                u'module': 'test_plugin'
            },
            u'main': {
                u'execute_frequency': u'60',
                u'resource_filter': u'resource_class = "network"'
            },
            u'snmp': {
                u'community': u'test_plugin_community'
            }
        }

        plugin_context = create_autospec(
                PanoptesPluginWithEnrichmentContext, instance=True, spec_set=True,
                data=panoptes_resource,
                config=plugin_conf_with_community,
                snmp=panoptes_context.config_object.snmp_defaults,
                x509=panoptes_context.config_object.x509_defaults,
                secrets=secret_store,
                logger=logging.getLogger(__name__)
        )

        snmp_configuration = PanoptesSNMPPluginConfiguration(plugin_context)
        self.assertEqual(snmp_configuration.community, u'test_plugin_community')

    def test_snmp_plugin_overrides(self):
        """
        Test that the plugin specific SNMP configuration is used if present
        """
        plugin_conf_with_overrides = {
            u'Core': {
                u'name': u'Test Plugin',
                u'module': u'test_plugin'
            },
            u'main': {
                u'execute_frequency': u'60',
                u'resource_filter': u'resource_class = "network"'
            },
            u'snmp': {
                u'connection_factory_module': u'test_module',
                u'connection_factory_class': u'test_class',
                u'port': 162,
                u'proxy_port': 10162,
                u'timeout': 10,
                u'retries': 2,
                u'non_repeaters': 1,
                u'max_repetitions': 10,
                u'community_string_key': u'test_community_string_key'
            }
        }

        plugin_context = create_autospec(
                PanoptesPluginWithEnrichmentContext, instance=True, spec_set=True,
                data=panoptes_resource,
                config=plugin_conf_with_overrides,
                snmp=panoptes_context.config_object.snmp_defaults,
                x509=panoptes_context.config_object.x509_defaults,
                secrets=secret_store,
                logger=logging.getLogger(__name__)
        )

        snmp_configuration = PanoptesSNMPPluginConfiguration(plugin_context)
        self.assertEqual(snmp_configuration.port, 162)
        self.assertEqual(snmp_configuration.proxy_port, 10162)
        self.assertEqual(snmp_configuration.connection_factory_module, u'test_module')
        self.assertEqual(snmp_configuration.connection_factory_class, u'test_class')
        self.assertEqual(snmp_configuration.timeout, 10)
        self.assertEqual(snmp_configuration.retries, 2)
        self.assertEqual(snmp_configuration.non_repeaters, 1)
        self.assertEqual(snmp_configuration.max_repetitions, 10)
        self.assertEqual(snmp_configuration.community_string_key, u'test_community_string_key')

    @staticmethod
    def _plugin_context_with_bad_snmp_configuration(**kwargs):
        plugin_conf_with_bad_configuration_value = {
            u'Core': {
                u'name': u'Test Plugin',
                u'module': u'test_plugin'
            },
            u'main': {
                u'execute_frequency': u'60',
                u'resource_filter': u'resource_class = "network"'
            },
            u'snmp': kwargs
        }

        return create_autospec(
                PanoptesPluginWithEnrichmentContext, instance=True, spec_set=True,
                data=panoptes_resource,
                config=plugin_conf_with_bad_configuration_value,
                snmp=panoptes_context.config_object.snmp_defaults,
                x509=panoptes_context.config_object.x509_defaults,
                secrets=secret_store,
                logger=logging.getLogger(__name__)
        )

    @staticmethod
    def _plugin_context_with_bad_x509_configuration(**kwargs):
        plugin_conf_with_bad_configuration_value = {
            u'Core': {
                u'name': u'Test Plugin',
                u'module': u'test_plugin'
            },
            u'main': {
                u'execute_frequency': u'60',
                u'resource_filter': u'resource_class = "network"'
            },
            u'x509': kwargs
        }

        return create_autospec(
                PanoptesPluginWithEnrichmentContext, instance=True, spec_set=True,
                data=panoptes_resource,
                config=plugin_conf_with_bad_configuration_value,
                snmp=panoptes_context.config_object.snmp_defaults,
                x509=panoptes_context.config_object.x509_defaults,
                secrets=secret_store,
                logger=logging.getLogger(__name__)
        )

    def test_x509_bad_configuration_values(self):
        # x509_secure_connection
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_x509_configuration(x509_secured_requests=5))

        # x509_cert_location
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_x509_configuration(x509_cert_location=u''))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_x509_configuration(x509_cert_location=0))
        # x509_cert_filename
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_x509_configuration(x509_cert_filename=u''))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_x509_configuration(x509_cert_filename=0))

        # x509_key_location
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_x509_configuration(x509_key_location=u''))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_x509_configuration(x509_key_location=0))
        # x509_key_filename
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_x509_configuration(x509_key_filename=u''))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_x509_configuration(x509_key_filename=0))

    def test_snmp_bad_configuration_values(self):
        """
        Test that 'bad' configuration values raise expected exceptions
        """
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(
                self._plugin_context_with_bad_snmp_configuration(connection_factory_module=0))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(
                self._plugin_context_with_bad_snmp_configuration(connection_factory_module=u''))

        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(
                self._plugin_context_with_bad_snmp_configuration(connection_factory_class=0))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(
                self._plugin_context_with_bad_snmp_configuration(connection_factory_class=u''))

        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(community=0))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(community=u''))

        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(community_string_key=0))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(community_string_key=u''))

        with self.assertRaises(ValueError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(port=u''))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(port=-1))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(port=65536))

        with self.assertRaises(ValueError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(proxy_port=u''))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(proxy_port=-1))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(proxy_port=65536))

        with self.assertRaises(ValueError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(timeout=u''))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(timeout=0))

        with self.assertRaises(ValueError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(retries=u''))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(retries=0))

        with self.assertRaises(ValueError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(non_repeaters=u''))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(non_repeaters=-1))

        with self.assertRaises(ValueError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(max_repetitions=u''))
        with self.assertRaises(AssertionError):
            PanoptesSNMPPluginConfiguration(self._plugin_context_with_bad_snmp_configuration(max_repetitions=0))

    @patch('yahoo_panoptes.framework.validators.PanoptesValidators.valid_readable_file', return_value=True)
    def test_steamroller_make_connection(self, _):
        """
        Tests that types are properly handled
        :return:
        """
        cert = '/home/panoptes/x509/certs/panoptes.pem'
        key = '/home/panoptes/x509/keys/panoptes.key'
        sessionObj = PanoptesSNMPSteamRollerAgentConnection._make_connection('1', cert, key)
        self.assertEqual(sessionObj.cert, (cert, key))

        sessionObj = PanoptesSNMPSteamRollerAgentConnection._make_connection('2', cert, key)
        self.assertEqual(sessionObj.cert, (cert, key))

        sessionObj = PanoptesSNMPSteamRollerAgentConnection._make_connection(1, cert, key)
        self.assertEqual(sessionObj.cert, (cert, key))

        sessionObj = PanoptesSNMPSteamRollerAgentConnection._make_connection(2, cert, key)
        self.assertEqual(sessionObj.cert, (cert, key))

        sessionObj = PanoptesSNMPSteamRollerAgentConnection._make_connection(0, cert, key)
        self.assertEqual(sessionObj.cert, None)

    def test_steamroller_serde_response(self):
        cert = '/home/panoptes/x509/certs/panoptes.pem'
        key = '/home/panoptes/x509/keys/panoptes.key'
        mock_connection = PanoptesSNMPSteamRollerAgentConnection(host='fw',
                                                                 port=443,
                                                                 timeout=30,
                                                                 retries=5,
                                                                 community='dummy',
                                                                 proxy_url='https://localhost:443',
                                                                 x509_secure_connection=1,
                                                                 x509_cert_file=cert,
                                                                 x509_key_file=key)

        response = {
            "responses": {
                "fw": {
                    ".1.3.6.1.4.1.2636.3.1.13.1.11": [
                        {
                            "oid": ".1.3.6.1.4.1.2636.3.1.13.1.11.9.1.8",
                            "result": "success",
                            "index": "0",
                            "value": "28",
                            "type": "GAUGE"
                        },
                        {
                            "oid": ".1.3.6.1.4.1.2636.3.1.13.1.11.9.1.9",
                            "result": "success",
                            "index": "0",
                            "value": "0",
                            "type": "GAUGE"
                        }
                    ]
                }
            },
            "guid": "dummy_value"
        }

        result = mock_connection._deserialize_response(response, 'BULKWALK', '.1.3.6.1.4.1.2636.3.1.13.1.11')

        self.assertEquals(len(result), 2)
        self.assertEquals(result[0].value, "28")
        self.assertEquals(result[0].index, '9.1.8.0')

        self.assertEquals(result[1].value, "0")
        self.assertEquals(result[1].index, '9.1.9.0')

        steamrollerRequest = mock_connection._create_request('BULKWALK', '.1.3.6.1.2.1.31.1.1.1.6', {})['requests'][0]
        self.assertEqual(steamrollerRequest['devices'][0], 'fw')
        self.assertEqual(steamrollerRequest['authentication']['type'], 'community')
        self.assertEqual(steamrollerRequest['authentication']['params']['community'], 'dummy')
        self.assertEqual(steamrollerRequest['phases']['.1.3.6.1.2.1.31.1.1.1.6']['operation'], 'BULKWALK')
