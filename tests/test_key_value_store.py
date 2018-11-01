"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
import math
import time
import unittest

from mock import patch

from yahoo_panoptes.framework.resources import PanoptesContext
from yahoo_panoptes.framework.utilities.key_value_store import PanoptesKeyValueStore, PanoptesKeyValueStoreValidators

from .test_framework import panoptes_mock_redis_strict_client
from .helpers import get_test_conf_file


class TestPanoptesKeyValueStore(unittest.TestCase):
    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file)

    def test_basic_operations(self):
        kv_store = PanoptesKeyValueStore(self._panoptes_context, "test_namespace")

        self.assertEqual(kv_store.namespace, "test_namespace")

        # Test get/set
        with self.assertRaises(AssertionError):
            kv_store.get(1)
        self.assertIsNone(kv_store.get("test"))

        with self.assertRaises(AssertionError):
            kv_store.set("test", 1)
        with self.assertRaises(AssertionError):
            kv_store.set("test", "test", 0)
        with self.assertRaises(AssertionError):
            kv_store.set("test", "test", True)

        kv_store.set("test", "test")
        self.assertEqual(kv_store.get("test"), "test")

        # Test getset performs upsert
        self.assertEqual(kv_store.getset("test", "test2"), "test")
        self.assertEqual(kv_store.get("test"), "test2")

        kv_store.set("test2", "test2")
        kv_store.set("test3", "test3")

        # Test find_keys pattern
        self.assertEqual(kv_store.find_keys("test"), ["test"])
        self.assertEqual(kv_store.find_keys("test*"), ['test', 'test2', 'test3'])

        # Test ttl
        with self.assertRaises(AssertionError):
            kv_store.ttl(1)

        start = time.time()
        kv_store.set("test", "test", 10 ** 10)
        ttl = kv_store.ttl("test")
        end = time.time()
        self.assertAlmostEqual(ttl, 10 ** 10, delta=math.ceil(end - start))

        # Test delete
        with self.assertRaises(AssertionError):
            kv_store.delete(1)
        kv_store.delete("test2")
        self.assertIsNone(kv_store.get("test2"))

        # Test set operations
        kv_store.set_add("set", "a")
        kv_store.set_add("set", "b")
        kv_store.set_add("set", "c")
        kv_store.set_add("set", "c")

        self.assertSetEqual(kv_store.set_members("set"), {"a", "b", "c"})


class TestPanoptesKeyValueStoreValidators(unittest.TestCase):
    def test_basic_operations(self):
        self.assertFalse(PanoptesKeyValueStoreValidators.valid_kv_store_class(None))
        self.assertTrue(PanoptesKeyValueStoreValidators.valid_kv_store_class(PanoptesKeyValueStore))
        self.assertFalse(PanoptesKeyValueStoreValidators.valid_kv_store_class(PanoptesContext))
