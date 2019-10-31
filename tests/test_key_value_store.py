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
    @patch(u'redis.StrictRedis', panoptes_mock_redis_strict_client)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file)

    def test_basic_operations(self):
        kv_store = PanoptesKeyValueStore(self._panoptes_context, u"test_namespace")

        self.assertEqual(kv_store.namespace, u"test_namespace")

        # Test get/set
        with self.assertRaises(AssertionError):
            kv_store.get(1)
        self.assertIsNone(kv_store.get(u"test"))

        with self.assertRaises(AssertionError):
            kv_store.set(u"test", 1)
        with self.assertRaises(AssertionError):
            kv_store.set(u"test", u"test", 0)
        with self.assertRaises(AssertionError):
            kv_store.set(u"test", u"test", True)

        kv_store.set(u"test", u"test")
        self.assertEqual(kv_store.get(u"test"), u"test")

        # Test getset performs upsert
        self.assertEqual(kv_store.getset(u"test", u"test2"), u"test")
        self.assertEqual(kv_store.get(u"test"), u"test2")

        kv_store.set(u"test2", u"test2")
        kv_store.set(u"test3", u"test3")

        # Test find_keys pattern
        self.assertEqual(kv_store.find_keys(u"test"), [u"test"])
        self.assertEqual(kv_store.find_keys(u"test*"), [u'test', u'test2', u'test3'])

        # Test ttl
        with self.assertRaises(AssertionError):
            kv_store.ttl(1)

        start = time.time()
        kv_store.set(u"test", u"test", 10 ** 10)
        ttl = kv_store.ttl(u"test")
        end = time.time()
        self.assertAlmostEqual(ttl, 10 ** 10, delta=math.ceil(end - start))

        # Test delete
        with self.assertRaises(AssertionError):
            kv_store.delete(1)
        kv_store.delete(u"test2")
        self.assertIsNone(kv_store.get(u"test2"))

        # Test set operations
        kv_store.set_add(u"set", u"a")
        kv_store.set_add(u"set", u"b")
        kv_store.set_add(u"set", u"c")
        kv_store.set_add(u"set", u"c")

        self.assertSetEqual(kv_store.set_members(u"set"), {u"a", u"b", u"c"})


class TestPanoptesKeyValueStoreValidators(unittest.TestCase):
    def test_basic_operations(self):
        self.assertFalse(PanoptesKeyValueStoreValidators.valid_kv_store_class(None))
        self.assertTrue(PanoptesKeyValueStoreValidators.valid_kv_store_class(PanoptesKeyValueStore))
        self.assertFalse(PanoptesKeyValueStoreValidators.valid_kv_store_class(PanoptesContext))
