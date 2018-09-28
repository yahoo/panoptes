"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""

import unittest

from yahoo_panoptes.framework.utilities.helpers import *


class TestHelpers(unittest.TestCase):
    def test_normalize_plugin_name(self):
        self.assertEqual(normalize_plugin_name('Test Plugin Name'), 'Test_Plugin_Name')
        self.assertEqual(normalize_plugin_name('Test/Plugin/Name'), 'Test_Plugin_Name')
        self.assertEqual(normalize_plugin_name('Test/Plugin Name'), 'Test_Plugin_Name')
        self.assertEqual(normalize_plugin_name('Test.Plugin.Name'), 'Test_Plugin_Name')
        self.assertEqual(normalize_plugin_name('Test Plugin.Name'), 'Test_Plugin_Name')
        self.assertEqual(normalize_plugin_name('Test/Plugin.Name'), 'Test_Plugin_Name')
        self.assertEqual(normalize_plugin_name('Test_Plugin.Name'), 'Test__Plugin_Name')
        self.assertEqual(normalize_plugin_name('Test.Plugin_Name'), 'Test_Plugin__Name')
        self.assertEqual(normalize_plugin_name('Test_Plugin_Name'), 'Test__Plugin__Name')
        self.assertEqual(normalize_plugin_name('Test!_Plugin.Name'), 'Test___Plugin_Name')
        self.assertEqual(normalize_plugin_name('Test.Plugin!#Name'), 'Test_Plugin__Name')
        with self.assertRaises(AssertionError):
            normalize_plugin_name(None)
            normalize_plugin_name(1)

    def test_get_module_mtime(self):
        self.assertGreater(get_module_mtime('.'), 0)
        my_module_path = os.path.splitext(os.path.abspath(__file__))[0]
        self.assertGreater(get_module_mtime(my_module_path), 0)
        self.assertEqual(get_module_mtime('/none/existent/module/path'), 0)
        with self.assertRaises(AssertionError):
            get_module_mtime(None)
            get_module_mtime(1)

    def test_get_hostname(self):
        self.assertIsInstance(get_hostname(), str)

    def test_resolve_hostnames(self):
        self.assertEqual(resolve_hostnames(['localhost'], 1), [('localhost', '127.0.0.1')])
        self.assertEqual(resolve_hostnames(['non.existent.host'], 1), [('non.existent.host', None)])
        with self.assertRaises(AssertionError):
            resolve_hostnames(None, 1)
        with self.assertRaises(AssertionError):
            resolve_hostnames(['localhost'], '1')

    def test_get_od_tid(self):
        self.assertGreater(get_os_tid(), 0)

    def test_get_calling_module_name(self):
        self.assertRegexpMatches(get_calling_module_name(1), '.*test_helpers$')
        with self.assertRaises(AssertionError):
            get_calling_module_name('1')

    def test_convert_kv_str_to_dict(self):
        with self.assertRaises(AssertionError):
            convert_kv_str_to_dict(kv=None, prefix='prefix')

        with self.assertRaises(AssertionError):
            convert_kv_str_to_dict(kv='key|value', prefix=None)

        with self.assertRaises(AssertionError):
            convert_kv_str_to_dict(kv='key|value', prefix='prefix', kv_delimiter=None)

        with self.assertRaises(AssertionError):
            convert_kv_str_to_dict(kv='key|value', prefix='prefix', kv_delimiter='|', prefix_delimiter=None)

        self.assertEquals(convert_kv_str_to_dict(
                kv='metro|WA State|region|Western US|continent|North America|latitude|47.615|longitude|-122.339',
                prefix='geo'), {'geo_metro': 'WA State', 'geo_region': 'Western US', 'geo_continent': 'North America',
                                'geo_latitude': '47.615', 'geo_longitude': '-122.339'})

    def test_transform_index_ipv6_address(self):
        self.assertEquals(transform_index_ipv6_address('32.1.73.152.0.88.206.3.0.0.0.0.0.0.0.1'),
                          '2001:4998:58:ce03::1')
        self.assertEquals(transform_index_ipv6_address('254.128.0.0.0.0.0.0.0.5.115.255.254.160.0.3'),
                          'fe80::5:73ff:fea0:3')

    def test_transform_octet_to_mac(self):
        self.assertEquals(transform_octet_to_mac(u'\xe4\xc7"\xdbJ\x08'), 'E4:C7:22:DB:4A:08')
        self.assertEquals(transform_octet_to_mac(u'\xe4\xc7"\xdbJ\t'), 'E4:C7:22:DB:4A:09')


if __name__ == '__main__':
    unittest.main()
