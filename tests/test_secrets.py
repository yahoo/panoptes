import unittest

from mock import patch
from yahoo_panoptes.framework.utilities.secrets import PanoptesSecretsStore
from yahoo_panoptes.framework.resources import PanoptesContext
from yahoo_panoptes.framework.utilities.key_value_store import PanoptesKeyValueStore, PanoptesKeyValueStoreValidators
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
