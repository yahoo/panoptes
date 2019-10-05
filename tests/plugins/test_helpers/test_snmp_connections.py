import time
import os
import unittest
from mock import patch

from tests.helpers import get_test_conf_file

from tests.test_framework import PanoptesTestKeyValueStore, panoptes_mock_redis_strict_client

from yahoo_panoptes.framework.context import PanoptesContext
from yahoo_panoptes.framework.plugins.context import PanoptesPluginContext
from yahoo_panoptes.framework.resources import PanoptesResource
from yahoo_panoptes.plugins.helpers.snmp_connections import PanoptesSNMPConnectionFactory, \
    PanoptesSNMPSteamRollerAgentConnection
from yahoo_panoptes.framework.utilities.key_value_store import PanoptesKeyValueStore
from yahoo_panoptes.framework.utilities.secrets import PanoptesSecretsStore
from yahoo_panoptes.framework.utilities.snmp.connection import PanoptesSNMPV2Connection, PanoptesSNMPException


class MockPanoptesContext(PanoptesContext):
    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    def __init__(self, config_file='tests/config_files/test_panoptes_config.ini'):
        super(MockPanoptesContext, self).__init__(
                key_value_store_class_list=[],
                create_zookeeper_client=False,
                config_file=config_file,
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


class TestPanoptesSecretsStore(PanoptesSecretsStore):
    def set(self, key, value, expire=None):
        PanoptesKeyValueStore.set(self, key, value, expire)

    def delete(self, key):
        PanoptesKeyValueStore.delete(self, key)


class TestPanoptesSNMPConnectionFactory(unittest.TestCase):
    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    def setUp(self):
        _, self.panoptes_test_conf_file = get_test_conf_file()
        self._my_path = os.path.dirname(os.path.abspath(__file__))
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file)
        self._resource = PanoptesResource(
            resource_class="test_class",
            resource_subclass="test_subclass",
            resource_type="test_type",
            resource_id="test_id",
            resource_endpoint="127.0.0.1",
            resource_site="test_site",
            resource_creation_timestamp=time.time(),
            resource_plugin="test_plugin"
        )
        self._key_value_store = PanoptesTestKeyValueStore(self._panoptes_context)
        self._secrets_store = TestPanoptesSecretsStore(self._panoptes_context)
        self._secrets_store.set(key='snmp_community_string:test_site', value='public')
        self._panoptes_plugin_context = PanoptesPluginContext(
            panoptes_context=self._panoptes_context,
            logger_name="test_logger",
            config=plugin_conf,
            key_value_store=self._key_value_store,
            secrets_store=self._secrets_store,
            data=self._resource
        )

    def test_get_snmp_connection(self):
        with self.assertRaises(AssertionError):
            PanoptesSNMPConnectionFactory.get_snmp_connection(plugin_context=None, resource=None)

        with self.assertRaises(AssertionError):
            PanoptesSNMPConnectionFactory.get_snmp_connection(plugin_context=self._panoptes_plugin_context,
                                                              resource=None)

        with self.assertRaises(AssertionError):
            PanoptesSNMPConnectionFactory.get_snmp_connection(plugin_context=None, resource=self._resource)

        with self.assertRaises(AssertionError):
            PanoptesSNMPConnectionFactory.get_snmp_connection(plugin_context=self._panoptes_plugin_context,
                                                              resource=self._resource, timeout='1')

        # Test that defaults (from the panoptes.ini) are set when no values are given
        connection = PanoptesSNMPConnectionFactory.get_snmp_connection(self._panoptes_plugin_context, self._resource)
        self.assertIsInstance(connection, PanoptesSNMPV2Connection)
        self.assertEqual(connection.host, '127.0.0.1')
        self.assertEqual(connection.port, 10161)
        self.assertEqual(connection.timeout, 5)
        self.assertEqual(connection.retries, 1)
        # Test that the community string is fetched and populated for the give site
        self.assertEqual(connection.community, 'public')

        # Test that when values for SNMP parameters are provided, the defaults are overwritten
        connection = PanoptesSNMPConnectionFactory.get_snmp_connection(self._panoptes_plugin_context, self._resource,
                                                                       timeout=1)
        self.assertIsInstance(connection, PanoptesSNMPV2Connection)
        self.assertEqual(connection.timeout, 1)

        connection = PanoptesSNMPConnectionFactory.get_snmp_connection(self._panoptes_plugin_context, self._resource,
                                                                       retries=2)
        self.assertIsInstance(connection, PanoptesSNMPV2Connection)
        self.assertEqual(connection.retries, 2)

        connection = PanoptesSNMPConnectionFactory.get_snmp_connection(self._panoptes_plugin_context, self._resource,
                                                                       port=10162)
        self.assertIsInstance(connection, PanoptesSNMPV2Connection)
        self.assertEqual(connection.port, 10162)

        # Test that the community suffixed is, er, suffixed
        connection = PanoptesSNMPConnectionFactory.get_snmp_connection(self._panoptes_plugin_context, self._resource,
                                                                       community_suffix='123')
        self.assertIsInstance(connection, PanoptesSNMPV2Connection)
        self.assertEqual(connection.community, 'public@123')

        # Test passing various combinations of x509 parameters
        key_file = os.path.join(self._my_path, 'key.pem')
        cert_file = os.path.join(self._my_path, 'certificate.pem')

        # Test that when x509_secure_connection is not set, the key and cert files are not needed
        connection = PanoptesSNMPConnectionFactory.get_snmp_connection(self._panoptes_plugin_context, self._resource,
                                                                       x509_secure_connection=0)
        self.assertIsInstance(connection, PanoptesSNMPV2Connection)

        # Test when x509_secure_connection is set, but the key and cert files aren't readable
        with self.assertRaises(PanoptesSNMPException):
            PanoptesSNMPConnectionFactory.get_snmp_connection(self._panoptes_plugin_context,
                                                              self._resource,
                                                              x509_secure_connection=1
                                                              )

        # Test when x509_secure_connection is set, but the key file isn't readable
        with self.assertRaises(PanoptesSNMPException):
            PanoptesSNMPConnectionFactory.get_snmp_connection(self._panoptes_plugin_context,
                                                              self._resource,
                                                              x509_secure_connection=1,
                                                              x509_key_file=key_file
                                                              )

        # Test when x509_secure_connection is set, but the cert file isn't readable
        with self.assertRaises(PanoptesSNMPException):
            PanoptesSNMPConnectionFactory.get_snmp_connection(self._panoptes_plugin_context,
                                                              self._resource,
                                                              x509_secure_connection=1,
                                                              x509_cert_file=cert_file
                                                              )

        # Test when x509_secure_connection is set, and both the key and cert files are readable
        connection = PanoptesSNMPConnectionFactory.get_snmp_connection(self._panoptes_plugin_context,
                                                                       self._resource,
                                                                       x509_secure_connection=1,
                                                                       x509_key_file=key_file,
                                                                       x509_cert_file=cert_file)
        self.assertIsInstance(connection, PanoptesSNMPV2Connection)

        # Test that when the snmp_proxy_hosts metadata key is set, we get PanoptesSNMPSteamRollerAgentConnection
        self._resource.add_metadata('snmp_proxy_hosts', '127.0.0.1:10161')
        connection = PanoptesSNMPConnectionFactory.get_snmp_connection(self._panoptes_plugin_context, self._resource)
        self.assertIsInstance(connection, PanoptesSNMPSteamRollerAgentConnection)

        # Test that an PanoptesSNMPException is thrown if the community string for the given site is empty
        # Note that this should be the last test - otherwise other tests would fail
        self._secrets_store.delete('snmp_community_string:test_site')
        with self.assertRaises(PanoptesSNMPException):
            PanoptesSNMPConnectionFactory.get_snmp_connection(self._panoptes_plugin_context, self._resource)
