"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
import unittest

from mock import patch, MagicMock

from yahoo_panoptes.framework.resources import PanoptesContext
from yahoo_panoptes.framework.utilities.key_value_store import PanoptesKeyValueStore, PanoptesKeyValueStoreException, \
    PanoptesKeyValueStoreValidators

from .test_framework import PanoptesTestKeyValueStore, panoptes_mock_redis_strict_client
from .helpers import get_test_conf_file


class TestPanoptesKeyValueStore(unittest.TestCase):
    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file)

    def test_basic_operations(self):
        pass
