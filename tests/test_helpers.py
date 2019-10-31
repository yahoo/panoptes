"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""

import unittest
from yahoo_panoptes.framework.utilities.helpers import *
from tests.helpers import get_test_conf_file


class TestHelpers(unittest.TestCase):

    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = get_test_conf_file()

    def test_normalize_plugin_name(self):
        self.assertEqual(normalize_plugin_name(u'Test Plugin Name'), u'Test_Plugin_Name')
        self.assertEqual(normalize_plugin_name(u'Test/Plugin/Name'), u'Test_Plugin_Name')
        self.assertEqual(normalize_plugin_name(u'Test/Plugin Name'), u'Test_Plugin_Name')
        self.assertEqual(normalize_plugin_name(u'Test.Plugin.Name'), u'Test_Plugin_Name')
        self.assertEqual(normalize_plugin_name(u'Test Plugin.Name'), u'Test_Plugin_Name')
        self.assertEqual(normalize_plugin_name(u'Test/Plugin.Name'), u'Test_Plugin_Name')
        self.assertEqual(normalize_plugin_name(u'Test_Plugin.Name'), u'Test__Plugin_Name')
        self.assertEqual(normalize_plugin_name(u'Test.Plugin_Name'), u'Test_Plugin__Name')
        self.assertEqual(normalize_plugin_name(u'Test_Plugin_Name'), u'Test__Plugin__Name')
        self.assertEqual(normalize_plugin_name(u'Test!_Plugin.Name'), u'Test___Plugin_Name')
        self.assertEqual(normalize_plugin_name(u'Test.Plugin!#Name'), u'Test_Plugin__Name')
        with self.assertRaises(AssertionError):
            normalize_plugin_name(None)
            normalize_plugin_name(1)

    def test_get_module_mtime(self):
        self.assertGreater(get_module_mtime(u'.'), 0)
        my_module_path = os.path.splitext(os.path.abspath(__file__))[0]
        self.assertGreater(get_module_mtime(my_module_path), 0)
        self.assertEqual(get_module_mtime(u'/none/existent/module/path'), 0)
        with self.assertRaises(AssertionError):
            get_module_mtime(None)
            get_module_mtime(1)

    def test_get_hostname(self):
        self.assertIsInstance(get_hostname(), str)

    def test_resolve_hostnames(self):
        self.assertEqual(resolve_hostnames([u'localhost'], 1), [(u'localhost', u'127.0.0.1')])
        self.assertEqual(resolve_hostnames([u'non.existent.host'], 1), [(u'non.existent.host', None)])
        with self.assertRaises(AssertionError):
            resolve_hostnames(None, 1)
        with self.assertRaises(AssertionError):
            resolve_hostnames([u'localhost'], u'1')

    def test_get_od_tid(self):
        self.assertGreater(get_os_tid(), 0)

    def test_get_calling_module_name(self):
        self.assertRegexpMatches(get_calling_module_name(1), u'.*test_helpers$')
        with self.assertRaises(AssertionError):
            get_calling_module_name(u'1')

    def test_convert_kv_str_to_dict(self):
        with self.assertRaises(AssertionError):
            convert_kv_str_to_dict(kv=None, prefix=u'prefix')

        with self.assertRaises(AssertionError):
            convert_kv_str_to_dict(kv=u'key|value', prefix=None)

        with self.assertRaises(AssertionError):
            convert_kv_str_to_dict(kv=u'key|value', prefix=u'prefix', kv_delimiter=None)

        with self.assertRaises(AssertionError):
            convert_kv_str_to_dict(kv=u'key|value', prefix=u'prefix', kv_delimiter=u'|', prefix_delimiter=None)

        self.assertEquals(convert_kv_str_to_dict(
                kv=u'metro|WA State|region|Western US|continent|North America|latitude|47.615|longitude|-122.339',
                prefix=u'geo'), {u'geo_metro': u'WA State', u'geo_region': u'Western US', u'geo_continent':
                                 u'North America', u'geo_latitude': u'47.615', u'geo_longitude': u'-122.339'})

    def test_transform_index_ipv6_address(self):
        self.assertEquals(transform_index_ipv6_address(u'32.1.73.152.0.88.206.3.0.0.0.0.0.0.0.1'),
                          u'2001:4998:58:ce03::1')
        self.assertEquals(transform_index_ipv6_address(u'254.128.0.0.0.0.0.0.0.5.115.255.254.160.0.3'),
                          u'fe80::5:73ff:fea0:3')

    def test_transform_octet_to_mac(self):
        self.assertEquals(transform_octet_to_mac(u'\xe4\xc7"\xdbJ\x08'), u'E4:C7:22:DB:4A:08')
        self.assertEquals(transform_octet_to_mac(u'\xe4\xc7"\xdbJ\t'), u'E4:C7:22:DB:4A:09')

    def test_celsius_conversion(self):
        self.assertEqual(convert_celsius_to_fahrenheit(0), 32)
        self.assertEqual(convert_celsius_to_fahrenheit(100), 212)
        self.assertEqual(convert_celsius_to_fahrenheit(200), 392)
        self.assertEqual(convert_celsius_to_fahrenheit(-100), -148)

    def test_ip_version(self):

        # v4 Address
        self.assertEqual(get_ip_version(u'255.255.255.255'), 4)
        self.assertEqual(get_ip_version(u'6.0.0.0'), 4)
        self.assertEqual(get_ip_version(u'214.0.0.0'), 4)
        self.assertEqual(get_ip_version(u'192.168.1.1'), 4)

        # v6 Address
        self.assertEqual(get_ip_version(u'2001:db8:3333:4444:5555:6666:7777:8888'), 6)
        self.assertEqual(get_ip_version(u'2001:db8:3333:4444:CCCC:DDDD:EEEE:FFFF'), 6)
        self.assertEqual(get_ip_version(u'::'), 6)
        self.assertEqual(get_ip_version(u'2001:db8::'), 6)
        self.assertEqual(get_ip_version(u'::1234:5678'), 6)

        for bad_ip in [u'', u':', u'255.255.255.255.255']:
            with self.assertRaises(ValueError):
                get_ip_version(bad_ip)

    def test_dns_resolution(self):

        with self.assertRaises(AssertionError):
            get_hostnames(u'98.137.246.8', 5)

        with self.assertRaises(AssertionError):
            get_hostnames(u'98.137.246.8', 0)

        self.assertEqual(get_hostnames([u'127.0.0.1'], 1), {u'127.0.0.1': u'localhost'})

        bcast = get_hostnames([u'255.255.255.255'], 1)
        self.assertTrue(bcast[u'255.255.255.255'] in [u'broadcasthost', u'unknown-255-255-255-255'])

    def test_unknown_hostname(self):

        self.assertEqual(unknown_hostname(u'127.0.0.1'), u'unknown-127-0-0-1')
        self.assertEqual(unknown_hostname(u'255.255.255.255'), u'unknown-255-255-255-255')

    def test_capture_fd_redirect(self):

        stderr = CaptureStdErr()
        stdout = CaptureStdOut()

        estream = stderr.__enter__()
        sys.stderr.write(u'getpanoptes.io/docs/getting-started')
        stderr.__exit__()

        ostream = stdout.__enter__()
        sys.stdout.write(u'getpanoptes.io')
        stdout.__exit__()

        self.assertEqual(ostream, [u'getpanoptes.io'])
        self.assertEqual(estream, [u'getpanoptes.io/docs/getting-started'])

    def test_transform_dotted_decimal_to_mac(self):

        self.assertEqual(transform_dotted_decimal_to_mac(u'126.2.196.127.168.46.531'), u'7E:02:C4:7F:A8:2E:213')

    def test_netmask_to_cidr(self):

        self.assertEqual(convert_netmask_to_cidr(u'0.0.0.0'), 0)
        self.assertEqual(convert_netmask_to_cidr(u'255.0.0.0'), 8)
        self.assertEqual(convert_netmask_to_cidr(u'255.255.0.0'), 16)
        self.assertEqual(convert_netmask_to_cidr(u'255.255.255.0'), 24)
        self.assertEqual(convert_netmask_to_cidr(u'255.255.255.255'), 32)

    def test_config_file_validator(self):

        spec_path = u"{}/config_files/spec/test_panoptes_configspec.ini".format(self.my_dir)
        bad_file = u"{}/config_files/test_panoptes_logging.ini".format(self.my_dir)

        # Test Bad Path
        with self.assertRaises(PanoptesConfigurationParsingError):
            parse_config_file(u'/bad/path', u'?/bad/path')

        # Test Bad File
        with self.assertRaises(PanoptesConfigurationParsingError):
            parse_config_file(bad_file, spec_path)

    def test_mmh3_hash_lib(self):

        self.assertEqual(unsigned_mmh3(u"loofzqgugp"), 279167916)
        self.assertEqual(unsigned_mmh3(u"aliyzhliwj"), 3024468606)
        self.assertEqual(unsigned_mmh3(u"ybbjayiitx"), 3256418505)
        self.assertEqual(unsigned_mmh3(u"cvsshywjsn"), 1360741730)
        self.assertEqual(unsigned_mmh3(u"drwfeqgxza"), 2547591695)
        self.assertEqual(unsigned_mmh3(u"cwhkagexku"), 2250297940)
        self.assertEqual(unsigned_mmh3(u"hbfnzemztl"), 3639147325)
        self.assertEqual(unsigned_mmh3(u"zlafkcoynl"), 1055376886)
        self.assertEqual(unsigned_mmh3(u"ipuxlfrbcr"), 4136803275)
        self.assertEqual(unsigned_mmh3(u"cwkyakgnlr"), 433154673)
        self.assertEqual(unsigned_mmh3(u"wsachwgrcd"), 3720140426)
        self.assertEqual(unsigned_mmh3(u"dqhqvovhce"), 4247286627)
        self.assertEqual(unsigned_mmh3(u"czctdflbcu"), 3605659707)
        self.assertEqual(unsigned_mmh3(u"ysxsxhybju"), 1892411859)
        self.assertEqual(unsigned_mmh3(u"mdrwpudirs"), 645279226)
        self.assertEqual(unsigned_mmh3(u"hgkcripxnx"), 1616481172)

        self.assertEqual(unsigned_mmh3("loofzqgugp"), 279167916)
        self.assertEqual(unsigned_mmh3("aliyzhliwj"), 3024468606)
        self.assertEqual(unsigned_mmh3("ybbjayiitx"), 3256418505)
        self.assertEqual(unsigned_mmh3("cvsshywjsn"), 1360741730)
        self.assertEqual(unsigned_mmh3("drwfeqgxza"), 2547591695)
        self.assertEqual(unsigned_mmh3("cwhkagexku"), 2250297940)
        self.assertEqual(unsigned_mmh3("hbfnzemztl"), 3639147325)
        self.assertEqual(unsigned_mmh3("zlafkcoynl"), 1055376886)
        self.assertEqual(unsigned_mmh3("ipuxlfrbcr"), 4136803275)
        self.assertEqual(unsigned_mmh3("cwkyakgnlr"), 433154673)
        self.assertEqual(unsigned_mmh3("wsachwgrcd"), 3720140426)
        self.assertEqual(unsigned_mmh3("dqhqvovhce"), 4247286627)
        self.assertEqual(unsigned_mmh3("czctdflbcu"), 3605659707)
        self.assertEqual(unsigned_mmh3("ysxsxhybju"), 1892411859)
        self.assertEqual(unsigned_mmh3("mdrwpudirs"), 645279226)
        self.assertEqual(unsigned_mmh3("hgkcripxnx"), 1616481172)


if __name__ == '__main__':
    unittest.main()
