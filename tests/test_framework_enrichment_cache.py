"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""

from builtins import object
import unittest
import json
import os

from mock import patch, Mock
from tests.test_framework import PanoptesMockRedis

from yahoo_panoptes.framework.context import PanoptesContext
from yahoo_panoptes.framework.resources import PanoptesResource
from yahoo_panoptes.framework.enrichment import PanoptesEnrichmentCacheError, PanoptesEnrichmentCache, \
    PanoptesEnrichmentCacheKeyValueStore

mock_time = Mock()
mock_time.return_value = 1521668419.881953


def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in list(obj.items()))
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj


def _get_test_conf_file():
    my_dir = os.path.dirname(os.path.realpath(__file__))
    panoptes_test_conf_file = os.path.join(my_dir, u'config_files/test_panoptes_config.ini')

    return my_dir, panoptes_test_conf_file


class MockLogger(object):
    def __init__(self):
        self._mock_error = Mock()
        self._mock_debug = Mock()

    @property
    def error(self):
        return self._mock_error

    @property
    def debug(self):
        return self._mock_debug


class TestPanoptesEnrichmentCache(unittest.TestCase):
    @patch(u'redis.StrictRedis', PanoptesMockRedis)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = _get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                                 key_value_store_class_list=[PanoptesEnrichmentCacheKeyValueStore])

        self._enrichment_kv = self._panoptes_context.get_kv_store(PanoptesEnrichmentCacheKeyValueStore)

        self._panoptes_resource = PanoptesResource(resource_site=u'test_site',
                                                   resource_class=u'test_class',
                                                   resource_subclass=u'test_subclass',
                                                   resource_type=u'test_type',
                                                   resource_id=u'test_resource_id',
                                                   resource_endpoint=u'test_endpoint',
                                                   resource_plugin=u'test_plugin')

        self._plugin_conf = {u'Core': {u'name': u'Heartbeat Enrichment Plugin',
                                       u'module': u'plugin_enrichment_heartbeat'},
                             u'main':
                                 {u'execute_frequency': u'60',
                                  u'enrichment_ttl': u'300',
                                  u'resource_filter': u'resource_class = "system" AND resource_subclass = '
                                                     u'"internal" AND resource_type = "heartbeat"'},
                             u'enrichment':
                                 {u'preload': u'self:test_namespace'}
                             }

        self._enrichment_kv.set(u'test_resource_id:test_namespace',
                                u'{{"data": {{"heartbeat": {{"timestamp": 1521668419}}}}, '
                                u'"metadata": {{"_enrichment_group_creation_timestamp": {:.5f}, '
                                u'"_enrichment_ttl": 300, "_execute_frequency": 60}}}}'.format(mock_time.return_value))
        self._enrichment_kv.set(
            u'test_resource_id01:interface',
            u'{{"data": {{"1226": {{"ifType": "l3ipvlan", "ifAlias": "<not set>", "ifPhysAddress": '
            u'"00:1f:a0:13:8c:16", "ifDescr": "Virtual Ethernet 226", "ifName": "Virtual Ethernet 226", '
            u'"ifMtu": "1500"}}, "1209": {{"ifType": "l3ipvlan", "ifAlias": "<not set>", "ifPhysAddress": '
            u'"00:1f:a0:13:8c:15", "ifDescr": "Virtual Ethernet 209", "ifName": "Virtual Ethernet 209", '
            u'"ifMtu": "1500"}}}}, "metadata": {{"_enrichment_group_creation_timestamp": {:.5f}, '
            u'"_enrichment_ttl": 300, "_execute_frequency": 60}}}}'.format(mock_time.return_value))

        self._enrichment_kv.set(
            u'test_resource_id02:interface',
            u'{{"data": {{"2226": {{"ifType": "l3ipvlan", "ifAlias": "<not set>", "ifPhysAddress": '
            u'"00:1f:a0:13:8c:16", "ifDescr": "Virtual Ethernet 326", "ifName": "Virtual Ethernet 326", '
            u'"ifMtu": "1500"}}, "2209": {{"ifType": "l3ipvlan", "ifAlias": "<not set>", "ifPhysAddress": '
            u'"00:1f:a0:13:8c:15", "ifDescr": "Virtual Ethernet 309", "ifName": "Virtual Ethernet 309", '
            u'"ifMtu": "1500"}}}}, "metadata": {{"_enrichment_group_creation_timestamp": {:.5f}, '
            u'"_enrichment_ttl": 300, "_execute_frequency": 60}}}}'.format(mock_time.return_value))

        self._enrichment_kv.set(u'asn_api:namespace01',
                                u'{{"data": {{"asn_data01": {{"data": 101}}}}, '
                                u'"metadata": {{"_enrichment_group_creation_timestamp": {:.5f}, '
                                u'"_enrichment_ttl": 300, "_execute_frequency": 60}}}}'.format(mock_time.return_value))
        self._enrichment_kv.set(u'asn_api:namespace02',
                                u'{{"data": {{"asn_data02": {{"data": 202}}}}, '
                                u'"metadata": {{"_enrichment_group_creation_timestamp": {:.5f}, '
                                u'"_enrichment_ttl": 300, "_execute_frequency": 60}}}}'.format(mock_time.return_value))
        self._enrichment_kv.set(u'asn_api:namespace03',
                                u'{{"data": {{"asn_data03": {{"data": 303}}}}, '
                                u'"metadata": {{"_enrichment_group_creation_timestamp": {:.5f}, '
                                u'"_enrichment_ttl": 300, "_execute_frequency": 60}}}}'.format(mock_time.return_value))
        self._enrichment_kv.set(u'enrichment:asn_api:namespace05', u'bad_data')

    def test_enrichment_cache(self):
        """Test enrichment resource attributes"""
        with self.assertRaises(AssertionError):
            PanoptesEnrichmentCache(u'non_panoptes_context', self._plugin_conf, self._panoptes_resource)

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentCache(self._panoptes_context, u'non_plugin_conf', self._panoptes_resource)

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentCache(self._panoptes_context, self._plugin_conf, u'non_panoptes_resource')

        #  Test with bad key-value store
        mock_panoptes_enrichment_cache_key_value_store = Mock(side_effect=Exception)
        with patch(u'yahoo_panoptes.framework.enrichment.PanoptesEnrichmentCacheKeyValueStore',
                   mock_panoptes_enrichment_cache_key_value_store):
            with self.assertRaises(Exception):
                PanoptesEnrichmentCache(self._panoptes_context, self._plugin_conf, self._panoptes_resource)

    def test_enrichment_cache_preload01(self):
        """Test enrichment resource with preload conf self:test_namespace"""

        result = u'{{"test_resource_id": {{"test_namespace": {{"heartbeat": {{"timestamp": {}}}}}}}}}'.format(
            int(mock_time.return_value))

        enrichment_cache = PanoptesEnrichmentCache(self._panoptes_context,
                                                   self._plugin_conf,
                                                   self._panoptes_resource)

        self.assertEqual(ordered(enrichment_cache.__dict__[u'_enrichment_data']),
                         ordered(json.loads(result)))

    def test_enrichment_cache_preload02(self):
        """Test enrichment resource with preload conf"""
        plugin_conf = {u'Core': {u'name': u'Test Plugin 01'},
                       u'enrichment': {u'preload': u'self:test_namespace, '
                                                   u'asn_api:*, '
                                                   u'test_resource_id01:interface, '
                                                   u'no_data_resource_id:test_namespace, '
                                                   u'test_resource_id01:no_data_namespace'}
                       }

        results01 = u"""{{"asn_api": {{"namespace02": {{"asn_data02": {{"data": 202}}}}, "namespace03": {{"asn_data03":
        {{"data": 303}}}}, "namespace01": {{"asn_data01": {{"data": 101}}}}}}, "test_resource_id": {{"test_namespace":
        {{"heartbeat": {{"timestamp": {}}}}}}}, "test_resource_id01": {{"interface": {{"1226": {{"ifType": "l3ipvlan",
        "ifAlias": "<not set>", "ifPhysAddress": "00:1f:a0:13:8c:16", "ifDescr": "Virtual Ethernet 226", "ifName":
        "Virtual Ethernet 226", "ifMtu": "1500"}}, "1209": {{"ifType": "l3ipvlan", "ifAlias": "<not set>",
        "ifPhysAddress": "00:1f:a0:13:8c:15", "ifDescr": "Virtual Ethernet 209", "ifName": "Virtual Ethernet 209",
        "ifMtu": "1500"}}}}}}}}""".format(int(mock_time.return_value))

        enrichment_cache = PanoptesEnrichmentCache(self._panoptes_context,
                                                   plugin_conf,
                                                   self._panoptes_resource)

        self.assertEqual(ordered(enrichment_cache.__dict__[u'_enrichment_data']),
                         ordered(json.loads(results01)))

        #  Test an error while scanning kv store is handled correctly
        mock_find_keys = Mock(side_effect=Exception)
        mock_logger = MockLogger()
        with patch(u'yahoo_panoptes.framework.enrichment.PanoptesEnrichmentCacheKeyValueStore.find_keys',
                   mock_find_keys):
            with patch(u'yahoo_panoptes.framework.enrichment.PanoptesContext.logger', mock_logger):
                PanoptesEnrichmentCache(self._panoptes_context, plugin_conf, self._panoptes_resource)
                self.assertEqual(mock_logger.error.call_count, 3)

        mock_kv_store_get = Mock(side_effect=IOError)
        with patch(u'yahoo_panoptes.framework.enrichment.PanoptesEnrichmentCacheKeyValueStore.get',
                   mock_kv_store_get):
            with self.assertRaises(IOError):
                PanoptesEnrichmentCache(self._panoptes_context, plugin_conf, self._panoptes_resource)

    def test_enrichment_cache_parse_conf(self):
        """Test enrichment resource parse conf"""

        result01 = [(u'asn_api', u'*'), (u'self', u'test_namespace'), (u'test_resource_id01', u'interface')]

        plugin_conf = {u'Core': {u'name': u'Test Plugin 01'},
                       u'enrichment': {u'preload': u'self:test_namespace, asn_api:*, test_resource_id01:interface'}
                       }

        enrichment_cache = PanoptesEnrichmentCache(self._panoptes_context,
                                                   plugin_conf,
                                                   self._panoptes_resource)
        self.assertEqual(sorted(result01), sorted(enrichment_cache.__dict__[u'_preload_conf']))

        plugin_conf_with_dup = {u'Core': {u'name': u'Test Plugin 01'},
                                u'enrichment': {u'preload': u'self:test_namespace, asn_api:*, '
                                                            u'test_resource_id01:interface, asn_api:*, '
                                                            u'test_resource_id01:interface'}}

        enrichment_cache = PanoptesEnrichmentCache(self._panoptes_context,
                                                   plugin_conf_with_dup,
                                                   self._panoptes_resource)
        self.assertEqual(sorted(result01), sorted(enrichment_cache.__dict__[u'_preload_conf']))

    def test_enrichment_cache_parse_conf_bad(self):
        """Test enrichment resource parse bad conf"""

        plugin_conf01 = {u'Core': {u'name': u'Test Plugin 01'},
                         u'enrichment': {}
                         }
        with self.assertRaises(PanoptesEnrichmentCacheError):
            PanoptesEnrichmentCache(self._panoptes_context, plugin_conf01, self._panoptes_resource)

        plugin_conf02 = {u'Core': {u'name': u'Test Plugin 01'},
                         u'enrichment': {u'preload': {}}
                         }
        with self.assertRaises(PanoptesEnrichmentCacheError):
            PanoptesEnrichmentCache(self._panoptes_context, plugin_conf02, self._panoptes_resource)

        plugin_conf03 = {u'Core': {u'name': u'Test Plugin 01'},
                         u'enrichment': {u'preload': {u'self:test_namespace, asn_api:'}}
                         }
        with self.assertRaises(PanoptesEnrichmentCacheError):
            PanoptesEnrichmentCache(self._panoptes_context, plugin_conf03, self._panoptes_resource)

        plugin_conf04 = {u'Core': {u'name': u'Test Plugin 01'},
                         u'enrichment': {u'preload': {u'self:test_namespace, :'}}
                         }
        with self.assertRaises(PanoptesEnrichmentCacheError):
            PanoptesEnrichmentCache(self._panoptes_context, plugin_conf04, self._panoptes_resource)

        plugin_conf05 = {u'Core': {u'name': u'Test Plugin 01'},
                         u'enrichment': {u'preload': {u'self:test_namespace, '}}
                         }
        with self.assertRaises(PanoptesEnrichmentCacheError):
            PanoptesEnrichmentCache(self._panoptes_context, plugin_conf05, self._panoptes_resource)

        plugin_conf06 = {u'Core': {u'name': u'Test Plugin 01'},
                         u'enrichment': {u'preload': {u'self:test_namespace, :namespace01'}}
                         }
        with self.assertRaises(PanoptesEnrichmentCacheError):
            PanoptesEnrichmentCache(self._panoptes_context, plugin_conf06, self._panoptes_resource)

    def test_enrichment_cache_get_enrichment(self):
        """Test enrichment resource get"""
        plugin_conf = {u'Core': {u'name': u'Test Plugin 01'},
                       u'enrichment': {u'preload': u'self:test_namespace, '
                                                   u'asn_api:*, '
                                                   u'test_resource_id01:interface, '
                                                   u'no_data_resource_id:test_namespace, '
                                                   u'test_resource_id01:no_data_namespace'}
                       }

        enrichment_cache = PanoptesEnrichmentCache(self._panoptes_context,
                                                   plugin_conf,
                                                   self._panoptes_resource)

        result01 = {u'heartbeat': {u'timestamp': int(mock_time.return_value)}}
        enrichment_data = enrichment_cache.get_enrichment(u'test_resource_id', u'test_namespace')
        self.assertEqual(sorted(list(enrichment_data)), sorted(result01))

        enrichment_data = enrichment_cache.get_enrichment(u'self', u'test_namespace')
        self.assertEqual(sorted(list(enrichment_data)), sorted(result01))

        result02 = {u'1209': {u'ifType': u'l3ipvlan', u'ifAlias': u'<not set>', u'ifPhysAddress': u'00:1f:a0:13:8c:15',
                              u'ifDescr': u'Virtual Ethernet 209', u'ifName': u'Virtual Ethernet 209',
                              u'ifMtu': u'1500'},
                    u'1226': {u'ifType': u'l3ipvlan', u'ifAlias': u'<not set>', u'ifPhysAddress': u'00:1f:a0:13:8c:16',
                              u'ifDescr': u'Virtual Ethernet 226', u'ifName': u'Virtual Ethernet 226',
                              u'ifMtu': u'1500'}}

        enrichment_data = enrichment_cache.get_enrichment(u'test_resource_id01', u'interface')
        self.assertEqual(sorted(list(enrichment_data)), sorted(result02))

        with self.assertRaises(PanoptesEnrichmentCacheError):
            enrichment_cache.get_enrichment(u'no_data_resource_id', u'test_namespace')

        with self.assertRaises(PanoptesEnrichmentCacheError):
            enrichment_cache.get_enrichment(u'test_resource_id01', u'no_data_namespace')

        with self.assertRaises(AssertionError):
            enrichment_cache.get_enrichment(u'test_resource_id01', u'')

        with self.assertRaises(AssertionError):
            enrichment_cache.get_enrichment(u'', u'test_namespace')

        with self.assertRaises(AssertionError):
            enrichment_cache.get_enrichment(u'', u'*')

        # Test fallback preload
        result03 = {u'2209': {u'ifType': u'l3ipvlan', u'ifAlias': u'<not set>', u'ifPhysAddress': u'00:1f:a0:13:8c:15',
                              u'ifDescr': u'Virtual Ethernet 309', u'ifName': u'Virtual Ethernet 309',
                              u'ifMtu': u'1500'},
                    u'2226': {u'ifType': u'l3ipvlan', u'ifAlias': u'<not set>', u'ifPhysAddress': u'00:1f:a0:13:8c:16',
                              u'ifDescr': u'Virtual Ethernet 326', u'ifName': u'Virtual Ethernet 326',
                              u'ifMtu': u'1500'}}

        enrichment_data03 = enrichment_cache.get_enrichment(u'test_resource_id02', u'interface')
        self.assertEqual(sorted(list(enrichment_data03)), sorted(result03))

    def test_enrichment_cache_get_enrichment_value(self):
        """Test enrichment resource get_enrichment_value"""
        plugin_conf = {u'Core': {u'name': u'Test Plugin 01'},
                       u'enrichment': {u'preload': u'self:test_namespace, '
                                                   u'asn_api:*, '
                                                   u'test_resource_id01:interface, '
                                                   u'no_data_resource_id:test_namespace, '
                                                   u'test_resource_id01:no_data_namespace'}
                       }

        enrichment_cache = PanoptesEnrichmentCache(self._panoptes_context,
                                                   plugin_conf,
                                                   self._panoptes_resource)

        result01 = {u'timestamp': int(mock_time.return_value)}
        enrich_data = enrichment_cache.get_enrichment_value(u'self', u'test_namespace', u'heartbeat')
        self.assertEqual(sorted(result01), sorted(enrich_data))

        enrich_data = enrichment_cache.get_enrichment_value(u'test_resource_id', u'test_namespace', u'heartbeat')
        self.assertEqual(sorted(result01), sorted(enrich_data))

        result02 = {u'data': 101}
        enrich_data = enrichment_cache.get_enrichment_value(u'asn_api', u'namespace01', u'asn_data01')
        self.assertEqual(sorted(result02), sorted(enrich_data))

        result03 = {u'ifType': u'l3ipvlan', u'ifAlias': u'<not set>', u'ifPhysAddress': u'00:1f:a0:13:8c:15',
                    u'ifDescr': u'Virtual Ethernet 209', u'ifName': u'Virtual Ethernet 209', u'ifMtu': u'1500'}
        enrich_data = enrichment_cache.get_enrichment_value(u'test_resource_id01', u'interface', u'1209')
        self.assertEqual(sorted(result03), sorted(enrich_data))

        with self.assertRaises(PanoptesEnrichmentCacheError):
            enrichment_cache.get_enrichment_value(u'no_data_resource_id', u'test_namespace', u'no_data')

        with self.assertRaises(AssertionError):
            enrichment_cache.get_enrichment_value(u'no_data_resource_id', u'test_namespace', u'')

        with self.assertRaises(AssertionError):
            enrichment_cache.get_enrichment_value(u'no_data_resource_id', u'', u'no_data')

        with self.assertRaises(AssertionError):
            enrichment_cache.get_enrichment_value(u'', u'test_namespace', u'no_data')

        # Test fallback preload
        result04 = {u'ifType': u'l3ipvlan', u'ifAlias': u'<not set>', u'ifPhysAddress': u'00:1f:a0:13:8c:15',
                    u'ifDescr': u'Virtual Ethernet 309', u'ifName': u'Virtual Ethernet 309', u'ifMtu': u'1500'}

        enrich_data = enrichment_cache.get_enrichment_value(u'test_resource_id02', u'interface', u'2209')
        self.assertEqual(sorted(result04), sorted(enrich_data))

    def test_enrichment_cache_get_enrichment_keys(self):
        """Test enrichment resource get_enrichment_keys"""
        plugin_conf = {u'Core': {u'name': u'Test Plugin 01'},
                       u'enrichment': {u'preload': u'self:test_namespace, '
                                                   u'asn_api:*, '
                                                   u'test_resource_id01:interface, '
                                                   u'no_data_resource_id:test_namespace, '
                                                   u'test_resource_id01:no_data_namespace'}
                       }

        enrichment_cache = PanoptesEnrichmentCache(self._panoptes_context,
                                                   plugin_conf,
                                                   self._panoptes_resource)

        result01 = [u'1226', u'1209']
        enrichment_data = enrichment_cache.get_enrichment_keys(u'test_resource_id01', u'interface')
        self.assertListEqual(enrichment_data, result01)

        with self.assertRaises(PanoptesEnrichmentCacheError):
            enrichment_cache.get_enrichment_keys(u'test_resource_id01', u'no_data_namespace')

        result02 = [u'heartbeat']
        enrichment_data = enrichment_cache.get_enrichment_keys(u'test_resource_id', u'test_namespace')
        self.assertListEqual(enrichment_data, result02)

        enrichment_data = enrichment_cache.get_enrichment_keys(u'self', u'test_namespace')
        self.assertListEqual(enrichment_data, result02)

        # Test fallback preload
        result03 = [u'2226', u'2209']
        enrichment_data03 = enrichment_cache.get_enrichment_keys(u'test_resource_id02', u'interface')
        self.assertListEqual(enrichment_data03, result03)
