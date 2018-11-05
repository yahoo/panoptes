"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
import unittest

from mock import patch, MagicMock
from yahoo_panoptes.framework.utilities.secrets import PanoptesSecretsStore
from yahoo_panoptes.framework.resources import PanoptesContext
from yahoo_panoptes.framework.const import SECRETS_MANAGER_KEY_VALUE_NAMESPACE

from .test_framework import panoptes_mock_redis_strict_client
from .helpers import get_test_conf_file


class TestPanoptesSecretsStore(unittest.TestCase):
    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file)

    def test_basic_operations(self):
        secrets_store = PanoptesSecretsStore(self._panoptes_context)
        self.assertEqual(secrets_store.namespace, SECRETS_MANAGER_KEY_VALUE_NAMESPACE)

        # Test bad input
        with self.assertRaises(AssertionError):
            secrets_store.get_by_site(secret_name="", site="test_site")
        with self.assertRaises(AssertionError):
            secrets_store.get_by_site(secret_name="secret", site="")

        super(PanoptesSecretsStore, secrets_store).set(key="secret:test_site", value="test_secret")
        self.assertEqual(secrets_store.get_by_site("secret", "test_site"), "test_secret")

        # Test get exceptions
        mock_get = MagicMock(side_effect=Exception)
        with patch('yahoo_panoptes.framework.utilities.secrets.PanoptesKeyValueStore.get',
                   mock_get):
            with self.assertRaises(Exception):
                secrets_store.get_by_site("secret", "test_site")\

        # Test fallback to default
        super(PanoptesSecretsStore, secrets_store).set(key="secret:default", value="test_secret_default")

        temp_reference = super(PanoptesSecretsStore, secrets_store).get

        def side_effect(self, key):
            original_get = temp_reference
            if "default" in key:
                return original_get(key=key)
            else:
                raise Exception

        with patch('yahoo_panoptes.framework.utilities.secrets.PanoptesKeyValueStore.get',
                   side_effect):
            self.assertEqual(secrets_store.get_by_site("secret", "test_site"), "test_secret_default")
            with self.assertRaises(Exception):
                secrets_store.get_by_site("secret", "test_site", fallback_to_default=False)

    def test_shadowed_methods(self):
        """
        Tests that set, delete, set_add, and set_members are correctly shadowed since they should not be used for
        PanoptesSecretsStore.
        """
        secrets_store = PanoptesSecretsStore(self._panoptes_context)

        # Set up state using parent's set
        super(PanoptesSecretsStore, secrets_store).set(key="secret:test_site", value="test_secret")
        self.assertEqual(super(PanoptesSecretsStore, secrets_store).get(key="secret:test_site"), "test_secret")

        # Test set does not change state
        secrets_store.set(key="secret:test_site", value="test_secret2")
        self.assertNotEqual(super(PanoptesSecretsStore, secrets_store).get(key="secret:test_site"), "test_secret2")

        # Test delete
        secrets_store.delete(key="secret:test_site")
        self.assertEqual(super(PanoptesSecretsStore, secrets_store).get(key="secret:test_site"), "test_secret")

        # Test set operations

        # Set up state using parent's set_add
        super(PanoptesSecretsStore, secrets_store).set_add("set", "a")

        # Test set_add does not change state
        secrets_store.set_add("set", "b")
        self.assertSetEqual(super(PanoptesSecretsStore, secrets_store).set_members("set"), {"a"})

        # Test set_members returns None
        self.assertIsNone(secrets_store.set_members("set"))
