"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""

import collections
import glob
import json
import time
import unittest
from logging import getLogger, _loggerClass

from mock import patch, Mock, MagicMock
from mockredis import MockRedis
from redis.exceptions import TimeoutError
from zake.fake_client import FakeClient

from yahoo_panoptes.framework.configuration_manager import *
from yahoo_panoptes.framework.const import RESOURCE_MANAGER_RESOURCE_EXPIRE
from yahoo_panoptes.framework.context import *
from yahoo_panoptes.framework.plugins.panoptes_base_plugin import PanoptesPluginInfo
from yahoo_panoptes.framework.resources import PanoptesResource, PanoptesResourceSet, PanoptesResourceDSL, \
    PanoptesContext, ParseException, ParseResults, PanoptesResourcesKeyValueStore, PanoptesResourceStore, \
    PanoptesResourceCache, PanoptesResourceError, PanoptesResourceCacheException, PanoptesResourceEncoder
from yahoo_panoptes.framework.utilities.helpers import ordered
from yahoo_panoptes.framework.utilities.key_value_store import PanoptesKeyValueStore

_TIMESTAMP = round(time.time(), 5)


class PanoptesMockRedis(MockRedis):
    def __init__(self, bad_connection=False, timeout=False, **kwargs):
        if bad_connection:
            raise ConnectionError
        super(PanoptesMockRedis, self).__init__(**kwargs)
        self.connection_pool = 'mockredis connection pool'
        self.timeout = timeout

    def get(self, key):
        if self.timeout:
            raise TimeoutError
        else:
            return super(PanoptesMockRedis, self).get(key)

    def set(self, key, value, ex=None, px=None, nx=False, xx=False):
        if self.timeout:
            raise TimeoutError
        else:
            return super(PanoptesMockRedis, self).set(key, value, ex=ex, px=px, nx=nx, xx=xx)


class PanoptesTestKeyValueStore(PanoptesKeyValueStore):
    """
    A custom Key/Value store for the Discovery Manager which uses the namespace demarcated for Discovery Manager
    """

    def __init__(self, panoptes_context):
        super(PanoptesTestKeyValueStore, self).__init__(panoptes_context, 'panoptes_test')


def panoptes_mock_redis_strict_client(**kwargs):
    return PanoptesMockRedis(strict=True)


def panoptes_mock_redis_strict_client_bad_connection(**kwargs):
    return PanoptesMockRedis(bad_connection=True)


def panoptes_mock_redis_strict_client_timeout(**kwargs):
    return PanoptesMockRedis(timeout=True)


def panoptes_mock_kazoo_client(**kwargs):
    return FakeClient()


class MockKafkaClient(object):
    def __init__(self, kafka_brokers):
        self._kafka_brokers = kafka_brokers
        self._brokers = set()

        for broker in self._kafka_brokers:
            self._brokers.add(broker)

    @property
    def brokers(self):
        return self._brokers if len(self._brokers) > 0 else None


class MockZookeeperClient(object):
    def __init__(self):
        self._lock = None

    @property
    def Lock(self):
        return self._lock


class TestResources(unittest.TestCase):
    def setUp(self):
        self.__panoptes_resource_metadata = {'test': 'test', '_resource_ttl': '604800'}
        self.__panoptes_resource = PanoptesResource(resource_site='test', resource_class='test',
                                                    resource_subclass='test',
                                                    resource_type='test', resource_id='test', resource_endpoint='test',
                                                    resource_plugin='test',
                                                    resource_creation_timestamp=_TIMESTAMP,
                                                    resource_ttl=RESOURCE_MANAGER_RESOURCE_EXPIRE)
        self.__panoptes_resource.add_metadata('test', 'test')
        self.__panoptes_resource_set = PanoptesResourceSet()
        mock_valid_timestamp = Mock(return_value=True)
        with patch('yahoo_panoptes.framework.resources.PanoptesValidators.valid_timestamp',
                   mock_valid_timestamp):
            self.__panoptes_resource_set.resource_set_creation_timestamp = _TIMESTAMP
        self.my_dir, self.panoptes_test_conf_file = _get_test_conf_file()

    def test_panoptes_resource(self):
        panoptes_resource_metadata = self.__panoptes_resource_metadata
        panoptes_resource = self.__panoptes_resource

        self.assertIsInstance(panoptes_resource, PanoptesResource)
        self.assertEqual(panoptes_resource.resource_site, 'test')
        self.assertEqual(panoptes_resource.resource_class, 'test')
        self.assertEqual(panoptes_resource.resource_subclass, 'test')
        self.assertEqual(panoptes_resource.resource_type, 'test')
        self.assertEqual(panoptes_resource.resource_id, 'test')
        self.assertEqual(panoptes_resource.resource_endpoint, 'test')
        self.assertEqual(panoptes_resource.resource_metadata, panoptes_resource_metadata)
        self.assertEqual(panoptes_resource.resource_ttl, str(RESOURCE_MANAGER_RESOURCE_EXPIRE))
        self.assertEqual(panoptes_resource, panoptes_resource)
        self.assertFalse(panoptes_resource == '1')
        self.assertIsInstance(str(panoptes_resource), str)

        with self.assertRaises(AssertionError):
            panoptes_resource.add_metadata(None, 'test')
        with self.assertRaises(AssertionError):
            panoptes_resource.add_metadata('', 'test')
        with self.assertRaises(ValueError):
            panoptes_resource.add_metadata('1', 'test')
        with self.assertRaises(ValueError):
            panoptes_resource.add_metadata('key', 'test|')
        with self.assertRaises(AssertionError):
            PanoptesResource(resource_site='', resource_class='test', resource_subclass='test',
                             resource_type='test', resource_id='test', resource_endpoint='test')
        with self.assertRaises(AssertionError):
            PanoptesResource(resource_site='test', resource_class='', resource_subclass='test',
                             resource_type='test', resource_id='test', resource_endpoint='test')
        with self.assertRaises(AssertionError):
            PanoptesResource(resource_site='test', resource_class='test', resource_subclass='',
                             resource_type='test', resource_id='test', resource_endpoint='test')
        with self.assertRaises(AssertionError):
            PanoptesResource(resource_site='test', resource_class='test', resource_subclass='test',
                             resource_type='', resource_id='test', resource_endpoint='test')
        with self.assertRaises(AssertionError):
            PanoptesResource(resource_site='test', resource_class='test', resource_subclass='test',
                             resource_type='test', resource_id='', resource_endpoint='test')
        with self.assertRaises(AssertionError):
            PanoptesResource(resource_site=None, resource_class='test', resource_subclass='test',
                             resource_type='test', resource_id='test', resource_endpoint='test')
        with self.assertRaises(AssertionError):
            PanoptesResource(resource_site='test', resource_class=None, resource_subclass='test',
                             resource_type='test', resource_id='test', resource_endpoint='test')
        with self.assertRaises(AssertionError):
            PanoptesResource(resource_site='test', resource_class='test', resource_subclass=None,
                             resource_type='test', resource_id='test', resource_endpoint='test')
        with self.assertRaises(AssertionError):
            PanoptesResource(resource_site='test', resource_class='test', resource_subclass='test',
                             resource_type=None, resource_id='test', resource_endpoint='test')
        with self.assertRaises(AssertionError):
            PanoptesResource(resource_site='test', resource_class='test', resource_subclass='test',
                             resource_type='test', resource_id=None, resource_endpoint='test')
        with self.assertRaises(AssertionError):
            PanoptesResource(resource_site='test', resource_class='test', resource_subclass='test',
                             resource_type='test', resource_id='test', resource_endpoint=None)

        # Test json and raw representations of PanoptesResource
        panoptes_resource_2 = PanoptesResource(resource_site='test', resource_class='test',
                                               resource_subclass='test',
                                               resource_type='test', resource_id='test', resource_endpoint='test',
                                               resource_creation_timestamp=_TIMESTAMP,
                                               resource_plugin='test')
        self.assertEqual(panoptes_resource_2.resource_creation_timestamp, _TIMESTAMP)
        panoptes_resource_2_json = {
            u'resource_site': u'test',
            u'resource_id': u'test',
            u'resource_class': u'test',
            u'resource_plugin': u'test',
            u'resource_creation_timestamp': _TIMESTAMP,
            u'resource_subclass': u'test',
            u'resource_endpoint': u'test',
            u'resource_metadata': {
                u'_resource_ttl': u'604800'
            },
            u'resource_type': u'test'
        }
        self.assertEqual(ordered(json.loads(panoptes_resource_2.json)), ordered(panoptes_resource_2_json))
        panoptes_resource_2_raw = collections.OrderedDict(
            [('resource_site', 'test'),
             ('resource_class', 'test'),
             ('resource_subclass', 'test'),
             ('resource_type', 'test'),
             ('resource_id', 'test'),
             ('resource_endpoint', 'test'),
             ('resource_metadata', collections.OrderedDict(
                 [('_resource_ttl', '604800')])
              ),
             ('resource_creation_timestamp', _TIMESTAMP),
             ('resource_plugin', 'test')])
        self.assertEqual(panoptes_resource_2.raw, panoptes_resource_2_raw)

        # Test resource creation from dict
        with open('tests/test_resources/input/resource_one.json') as f:
            resource_specs = json.load(f)
        resource_from_json = PanoptesResource.resource_from_dict(resource_specs['resources'][0])
        panoptes_resource_3 = PanoptesResource(resource_site="test_site", resource_class="network",
                                               resource_subclass="test_subclass", resource_type="test_type",
                                               resource_id="test_id_1", resource_endpoint="test_endpoint_1",
                                               resource_plugin="key")
        self.assertEqual(resource_from_json, panoptes_resource_3)

    def test_panoptes_resource_set(self):
        panoptes_resource = self.__panoptes_resource
        panoptes_resource_set = self.__panoptes_resource_set
        self.assertEqual(panoptes_resource_set.add(panoptes_resource), None)
        self.assertEqual(len(panoptes_resource_set), 1)
        self.assertEqual(type(panoptes_resource_set.resources), set)
        self.assertIsInstance(str(panoptes_resource_set), str)
        self.assertIsInstance(iter(panoptes_resource_set), collections.Iterable)
        self.assertEqual(panoptes_resource_set.next(), panoptes_resource)
        self.assertEqual(panoptes_resource_set.remove(panoptes_resource), None)
        self.assertEqual(len(panoptes_resource_set), 0)
        self.assertEqual(panoptes_resource_set.resource_set_creation_timestamp, _TIMESTAMP)
        with self.assertRaises(AssertionError):
            panoptes_resource_set.resource_set_creation_timestamp = 0

        panoptes_resource_2 = PanoptesResource(resource_site='test', resource_class='test',
                                               resource_subclass='test',
                                               resource_type='test', resource_id='test2', resource_endpoint='test',
                                               resource_plugin='test',
                                               resource_ttl=RESOURCE_MANAGER_RESOURCE_EXPIRE,
                                               resource_creation_timestamp=_TIMESTAMP)
        panoptes_resource_set.add(panoptes_resource)
        panoptes_resource_set.add(panoptes_resource_2)
        self.assertEqual(len(panoptes_resource_set.get_resources_by_site()['test']), 2)
        self.assertEqual(panoptes_resource_set.resource_set_schema_version, "0.1")

        panoptes_resource_set_json = {
            u'resource_set_creation_timestamp': _TIMESTAMP,
            u'resource_set_schema_version': u'0.1',
            u'resources': [
                {
                    u'resource_site': u'test',
                    u'resource_class': u'test',
                    u'resource_subclass': u'test',
                    u'resource_type': u'test',
                    u'resource_id': u'test2',
                    u'resource_endpoint': u'test',
                    u'resource_metadata': {
                        u'_resource_ttl': u'604800'
                    },
                    u'resource_creation_timestamp': _TIMESTAMP,
                    u'resource_plugin': u'test'
                },
                {
                    u'resource_site': u'test',
                    u'resource_class': u'test',
                    u'resource_subclass': u'test',
                    u'resource_type': u'test',
                    u'resource_id': u'test',
                    u'resource_endpoint': u'test',
                    u'resource_metadata': {
                        u'_resource_ttl': u'604800',
                        u'test': u'test'
                    },
                    u'resource_creation_timestamp': _TIMESTAMP,
                    u'resource_plugin': u'test'
                }
            ]
        }
        self.assertEqual(ordered(panoptes_resource_set_json), ordered(json.loads(panoptes_resource_set.json)))

    def test_panoptes_resources_key_value_store(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file)
        panoptes_resources_kv_store = PanoptesResourcesKeyValueStore(panoptes_context)
        self.assertEqual(panoptes_resources_kv_store.redis_group, const.RESOURCE_MANAGER_REDIS_GROUP)
        self.assertEqual(panoptes_resources_kv_store.namespace, const.RESOURCE_MANAGER_KEY_VALUE_NAMESPACE)

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    def test_panoptes_resource_store(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                           key_value_store_class_list=[PanoptesTestKeyValueStore])
        with self.assertRaises(Exception):
            PanoptesResourceStore(panoptes_context)

        mock_kv_store = Mock(return_value=PanoptesTestKeyValueStore(panoptes_context))
        with patch('yahoo_panoptes.framework.resources.PanoptesContext.get_kv_store', mock_kv_store):
            panoptes_resource_store = PanoptesResourceStore(panoptes_context)
            panoptes_resource_store.add_resource("test_plugin_signature", self.__panoptes_resource)
            resource_key = "plugin|test|site|test|class|test|subclass|test|type|test|id|test|endpoint|test"
            resource_value = panoptes_resource_store.get_resource(resource_key)
            self.assertEqual(self.__panoptes_resource, resource_value)

            panoptes_resource_2 = PanoptesResource(resource_site='test', resource_class='test',
                                                   resource_subclass='test',
                                                   resource_type='test', resource_id='test2', resource_endpoint='test',
                                                   resource_plugin='test',
                                                   resource_ttl=RESOURCE_MANAGER_RESOURCE_EXPIRE,
                                                   resource_creation_timestamp=_TIMESTAMP)
            panoptes_resource_store.add_resource("test_plugin_signature", panoptes_resource_2)
            self.assertIn(self.__panoptes_resource, panoptes_resource_store.get_resources())
            self.assertIn(panoptes_resource_2, panoptes_resource_store.get_resources())

            panoptes_resource_store.delete_resource("test_plugin_signature", panoptes_resource_2)
            self.assertNotIn(panoptes_resource_2, panoptes_resource_store.get_resources())

            panoptes_resource_3 = PanoptesResource(resource_site='test', resource_class='test',
                                                   resource_subclass='test',
                                                   resource_type='test', resource_id='test3', resource_endpoint='test',
                                                   resource_plugin='test3',
                                                   resource_ttl=RESOURCE_MANAGER_RESOURCE_EXPIRE,
                                                   resource_creation_timestamp=_TIMESTAMP)
            panoptes_resource_store.add_resource("test_plugin_signature", panoptes_resource_3)
            self.assertIn(panoptes_resource_3, panoptes_resource_store.get_resources(site='test', plugin_name='test3'))
            self.assertNotIn(self.__panoptes_resource,
                             panoptes_resource_store.get_resources(site='test', plugin_name='test3'))

            # Test key not found
            mock_find_keys = Mock(
                return_value=['dummy',
                              'plugin|test|site|test|class|test|subclass|test|type|test|id|test|endpoint|test'])
            with patch('yahoo_panoptes.framework.resources.PanoptesKeyValueStore.find_keys',
                       mock_find_keys):
                self.assertEqual(1, len(panoptes_resource_store.get_resources()))

            # Test resource store methods raise correct errors
            mock_get = Mock(side_effect=Exception)
            with patch('yahoo_panoptes.framework.resources.PanoptesKeyValueStore.get', mock_get):
                with self.assertRaises(PanoptesResourceError):
                    panoptes_resource_store.get_resource('test3')

            # Test bad input
            with self.assertRaises(AssertionError):
                panoptes_resource_store.get_resource("")
            with self.assertRaises(AssertionError):
                panoptes_resource_store.get_resource(1)

            with self.assertRaises(AssertionError):
                panoptes_resource_store.add_resource("", panoptes_resource_2)
            with self.assertRaises(AssertionError):
                panoptes_resource_store.add_resource("test_plugin_signature", None)
            with self.assertRaises(AssertionError):
                panoptes_resource_store.add_resource("test_plugin_signature", PanoptesResourceStore(panoptes_context))

            with self.assertRaises(AssertionError):
                panoptes_resource_store.delete_resource("", panoptes_resource_2)
            with self.assertRaises(AssertionError):
                panoptes_resource_store.delete_resource("test_plugin_signature", None)
            with self.assertRaises(AssertionError):
                panoptes_resource_store.delete_resource("test_plugin_signature",
                                                        PanoptesResourceStore(panoptes_context))

            with self.assertRaises(AssertionError):
                panoptes_resource_store.get_resources("", "test_plugin_name")
            with self.assertRaises(AssertionError):
                panoptes_resource_store.get_resources("test_site", "")
            with self.assertRaises(AssertionError):
                panoptes_resource_store.get_resources(1, "test_plugin_name")
            with self.assertRaises(AssertionError):
                panoptes_resource_store.get_resources("test_site", 1)

            # Test non-existent key
            with self.assertRaises(PanoptesResourceError):
                panoptes_resource_store.get_resource('tes')

            mock_set = Mock(side_effect=Exception)
            with patch('yahoo_panoptes.framework.resources.PanoptesKeyValueStore.set', mock_set):
                with self.assertRaises(PanoptesResourceError):
                    panoptes_resource_store.add_resource("test_plugin_signature", panoptes_resource_2)

            mock_delete = Mock(side_effect=Exception)
            with patch('yahoo_panoptes.framework.resources.PanoptesKeyValueStore.delete', mock_delete):
                with self.assertRaises(PanoptesResourceError):
                    panoptes_resource_store.delete_resource("test_plugin_signature", panoptes_resource_2)

            with self.assertRaises(PanoptesResourceError):
                panoptes_resource_store._deserialize_resource("tes", "null")

            with self.assertRaises(PanoptesResourceError):
                panoptes_resource_store._deserialize_resource(resource_key, "null")

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    def test_resource_dsl_parsing(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file)

        test_query = 'resource_class = "network" AND resource_subclass = "load-balancer"'

        test_result = (
            'SELECT resources.*, group_concat(key,"|"), group_concat(value,"|") ' +
            'FROM resources ' +
            'LEFT JOIN resource_metadata ON resources.id = resource_metadata.id ' +
            'WHERE (resources.resource_class = "network" ' +
            'AND resources.resource_subclass = "load-balancer") ' +
            'GROUP BY resource_metadata.id ' +
            'ORDER BY resource_metadata.id'
        )

        panoptes_resource_dsl = PanoptesResourceDSL(test_query, panoptes_context)
        self.assertEqual(panoptes_resource_dsl.sql, test_result)

        # This very long query tests all code paths with the DSL parser:
        test_query = 'resource_class = "network" AND resource_subclass = "load-balancer" OR \
                resource_metadata.os_version LIKE "4%" AND resource_site NOT IN ("test_site") \
                AND resource_endpoint IN ("test1","test2") AND resource_type != "a10" OR ' \
                     'resource_metadata.make NOT LIKE \
                "A10%" AND resource_metadata.model NOT IN ("test1", "test2")'

        test_result = (
            'SELECT resources.*,group_concat(key,"|"),group_concat(value,"|") FROM (SELECT resource_metadata.id ' +
            'FROM resources,resource_metadata WHERE (resources.resource_class = "network" ' +
            'AND resources.resource_subclass = "load-balancer" ' +
            'AND resources.resource_site NOT IN ("test_site") ' +
            'AND resources.resource_endpoint IN ("test1","test2") ' +
            'AND resources.resource_type != "a10" ' +
            'AND ((resource_metadata.key = "os_version" ' +
            'AND resource_metadata.value LIKE "4%")) ' +
            'AND resource_metadata.id = resources.id) ' +
            'UNION SELECT resource_metadata.id ' +
            'FROM resources,resource_metadata WHERE (resource_metadata.key = "make" ' +
            'AND resource_metadata.value NOT LIKE "A10%") ' +
            'AND resource_metadata.id = resources.id ' +
            'INTERSECT SELECT resource_metadata.id ' +
            'FROM resources,resource_metadata WHERE (resource_metadata.key = "model" ' +
            'AND resource_metadata.value NOT IN ("test1","test2")) ' +
            'AND resource_metadata.id = resources.id ' +
            'GROUP BY resource_metadata.id ' +
            'ORDER BY resource_metadata.id) AS filtered_resources, ' +
            'resources, resource_metadata WHERE resources.id = filtered_resources.id ' +
            'AND resource_metadata.id = filtered_resources.id GROUP BY resource_metadata.id')

        panoptes_resource_dsl = PanoptesResourceDSL(test_query, panoptes_context)
        self.assertEqual(panoptes_resource_dsl.sql, test_result)

        panoptes_resource_dsl = PanoptesResourceDSL('resource_site = "local"', panoptes_context)
        self.assertIsInstance(panoptes_resource_dsl.tokens, ParseResults)

        with self.assertRaises(AssertionError):
            PanoptesResourceDSL(None, panoptes_context)
        with self.assertRaises(AssertionError):
            PanoptesResourceDSL('', None)
        with self.assertRaises(AssertionError):
            PanoptesResourceDSL('', panoptes_context)
        with self.assertRaises(ParseException):
            PanoptesResourceDSL('resources_site = local', panoptes_context)


class TestPanoptesResourceEncoder(unittest.TestCase):
    def setUp(self):
        self.__panoptes_resource = PanoptesResource(resource_site='test', resource_class='test',
                                                    resource_subclass='test',
                                                    resource_type='test', resource_id='test', resource_endpoint='test',
                                                    resource_plugin='test',
                                                    resource_creation_timestamp=_TIMESTAMP,
                                                    resource_ttl=RESOURCE_MANAGER_RESOURCE_EXPIRE)

    def test_panoptes_resource_encoder(self):
        resource_encoder = PanoptesResourceEncoder()
        self.assertEqual(resource_encoder.default(set()), list(set()))
        self.assertEqual(resource_encoder.default(self.__panoptes_resource),
                         self.__panoptes_resource.__dict__['_PanoptesResource__data'])
        with self.assertRaises(TypeError):
            resource_encoder.default(dict())


class TestPanoptesResourceCache(unittest.TestCase):
    def setUp(self):
        self.__panoptes_resource = PanoptesResource(resource_site='test', resource_class='test',
                                                    resource_subclass='test',
                                                    resource_type='test', resource_id='test', resource_endpoint='test',
                                                    resource_plugin='test',
                                                    resource_creation_timestamp=_TIMESTAMP,
                                                    resource_ttl=RESOURCE_MANAGER_RESOURCE_EXPIRE)
        self.my_dir, self.panoptes_test_conf_file = _get_test_conf_file()

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    def test_resource_cache(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                           key_value_store_class_list=[PanoptesTestKeyValueStore])

        #  Test PanoptesResourceCache methods when setup_resource_cache not yet called
        panoptes_resource_cache = PanoptesResourceCache(panoptes_context)
        test_query = 'resource_class = "network"'
        with self.assertRaises(PanoptesResourceCacheException):
            panoptes_resource_cache.get_resources(test_query)
        panoptes_resource_cache.close_resource_cache()

        panoptes_resource = self.__panoptes_resource
        panoptes_resource.add_metadata("metadata_key1", "test")
        panoptes_resource.add_metadata("metadata_key2", "test")

        kv = panoptes_context.get_kv_store(PanoptesTestKeyValueStore)
        serialized_key, serialized_value = PanoptesResourceStore._serialize_resource(panoptes_resource)
        kv.set(serialized_key, serialized_value)

        mock_kv_store = Mock(return_value=kv)

        with patch('yahoo_panoptes.framework.resources.PanoptesContext.get_kv_store', mock_kv_store):
            # Test errors when setting up resource cache
            mock_resource_store = Mock(side_effect=Exception)
            with patch('yahoo_panoptes.framework.resources.PanoptesResourceStore', mock_resource_store):
                with self.assertRaises(PanoptesResourceCacheException):
                    panoptes_resource_cache.setup_resource_cache()

            mock_connect = Mock(side_effect=Exception)
            with patch('yahoo_panoptes.framework.resources.sqlite3.connect', mock_connect):
                with self.assertRaises(PanoptesResourceCacheException):
                    panoptes_resource_cache.setup_resource_cache()

            mock_get_resources = Mock(side_effect=Exception)
            with patch('yahoo_panoptes.framework.resources.PanoptesResourceStore.get_resources', mock_get_resources):
                with self.assertRaises(PanoptesResourceCacheException):
                    panoptes_resource_cache.setup_resource_cache()

            # Test basic operations
            panoptes_resource_cache.setup_resource_cache()
            self.assertIsInstance(panoptes_resource_cache.get_resources('resource_class = "network"'),
                                  PanoptesResourceSet)
            self.assertEqual(len(panoptes_resource_cache.get_resources('resource_class = "network"')), 0)
            self.assertIn(panoptes_resource, panoptes_resource_cache.get_resources('resource_class = "test"'))
            self.assertEqual(len(panoptes_resource_cache._cached_resources), 2)

            panoptes_resource_cache.close_resource_cache()

            # Mock PanoptesResourceStore.get_resources to return Resources that otherwise couldn't be constructed:
            mock_resources = PanoptesResourceSet()
            mock_resources.add(panoptes_resource)
            bad_panoptes_resource = PanoptesResource(resource_site='test', resource_class='test',
                                                     resource_subclass='test',
                                                     resource_type='test', resource_id='test2',
                                                     resource_endpoint='test',
                                                     resource_plugin='test',
                                                     resource_creation_timestamp=_TIMESTAMP,
                                                     resource_ttl=RESOURCE_MANAGER_RESOURCE_EXPIRE)
            bad_panoptes_resource.__dict__['_PanoptesResource__data']['resource_metadata']['*'] = "test"
            bad_panoptes_resource.__dict__['_PanoptesResource__data']['resource_metadata']['**'] = "test"
            mock_resources.add(bad_panoptes_resource)
            mock_get_resources = Mock(return_value=mock_resources)
            with patch('yahoo_panoptes.framework.resources.PanoptesResourceStore.get_resources', mock_get_resources):
                panoptes_resource_cache.setup_resource_cache()
                self.assertEqual(len(panoptes_resource_cache.get_resources('resource_class = "test"')), 1)


class TestPanoptesContext(unittest.TestCase):
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = _get_test_conf_file()

    def test_context_config_file(self):
        # Test invalid inputs for config_file
        with self.assertRaises(AssertionError):
            PanoptesContext('')

        with self.assertRaises(AssertionError):
            PanoptesContext(1)

        with self.assertRaises(PanoptesContextError):
            PanoptesContext(config_file='non.existent.config.file')

        # Test that the default config file is loaded if no config file is present in the arguments or environment
        with patch('yahoo_panoptes.framework.const.DEFAULT_CONFIG_FILE_PATH', self.panoptes_test_conf_file):
            panoptes_context = PanoptesContext()
            self.assertEqual(panoptes_context.config_object.redis_urls[0].url, 'redis://:password@localhost:6379/0')
            del panoptes_context

        # Test that the config file from environment is loaded, if present
        os.environ[const.CONFIG_FILE_ENVIRONMENT_VARIABLE] = self.panoptes_test_conf_file
        panoptes_context = PanoptesContext()
        self.assertEqual(panoptes_context.config_object.redis_urls[0].url, 'redis://:password@localhost:6379/0')
        del panoptes_context

        #  Test bad config configuration files
        for f in glob.glob(os.path.join(self.my_dir, 'config_files/test_panoptes_config_bad_*.ini')):
            with self.assertRaises(PanoptesContextError):
                print 'Going to load bad configuration file: %s' % f
                PanoptesContext(f)

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    def test_context(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file)
        self.assertIsInstance(panoptes_context, PanoptesContext)
        self.assertEqual(panoptes_context.config_object.redis_urls[0].url, 'redis://:password@localhost:6379/0')
        self.assertEqual(str(panoptes_context.config_object.redis_urls[0]), 'redis://:**@localhost:6379/0')
        self.assertEqual(panoptes_context.config_object.zookeeper_servers, set(['localhost:2181']))
        self.assertEqual(panoptes_context.config_object.kafka_brokers, set(['localhost:9092']))
        self.assertIsInstance(panoptes_context.config_dict, dict)
        self.assertIsInstance(panoptes_context.logger, _loggerClass)
        self.assertIsInstance(panoptes_context.redis_pool, MockRedis)
        with self.assertRaises(AttributeError):
            panoptes_context.kafka_client
        with self.assertRaises(AttributeError):
            panoptes_context.zookeeper_client
        del panoptes_context

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client_bad_connection)
    def test_context_redis_bad_connection(self):
        with self.assertRaises(ConnectionError):
            PanoptesContext(self.panoptes_test_conf_file)

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    def test_context_key_value_store(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                           key_value_store_class_list=[PanoptesTestKeyValueStore])
        self.assertIsInstance(panoptes_context, PanoptesContext)
        kv = panoptes_context.get_kv_store(PanoptesTestKeyValueStore)
        self.assertIsInstance(kv, PanoptesTestKeyValueStore)
        self.assertTrue(kv.set('test', 'test'))
        self.assertEqual(kv.get('test'), 'test')
        self.assertEqual(kv.get('non.existent.key'), None)

        with self.assertRaises(AssertionError):
            kv.set(None, None)
        with self.assertRaises(AssertionError):
            kv.set('test', None)
        with self.assertRaises(AssertionError):
            kv.set(None, 'test')
        with self.assertRaises(AssertionError):
            kv.set(1, 'test')
        with self.assertRaises(AssertionError):
            kv.set('test', 1)

        with self.assertRaises(PanoptesContextError):
            PanoptesContext(self.panoptes_test_conf_file, key_value_store_class_list=[None])
        with self.assertRaises(PanoptesContextError):
            PanoptesContext(self.panoptes_test_conf_file, key_value_store_class_list=['test'])
        with self.assertRaises(PanoptesContextError):
            PanoptesContext(self.panoptes_test_conf_file, key_value_store_class_list=[PanoptesMockRedis])

        panoptes_context = PanoptesContext(self.panoptes_test_conf_file)

        with self.assertRaises(PanoptesContextError):
            panoptes_context.get_kv_store(PanoptesTestKeyValueStore)

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client_timeout)
    def test_context_key_value_store_timeout(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                           key_value_store_class_list=[PanoptesTestKeyValueStore])
        kv = panoptes_context.get_kv_store(PanoptesTestKeyValueStore)
        with self.assertRaises(TimeoutError):
            kv.set('test', 'test')
        with self.assertRaises(TimeoutError):
            kv.get('test')

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_context_message_bus(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file, create_zookeeper_client=True)
        self.assertIsInstance(panoptes_context, PanoptesContext)
        self.assertIsInstance(panoptes_context.zookeeper_client, FakeClient)

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_context_del_methods(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                           key_value_store_class_list=[PanoptesTestKeyValueStore],
                                           create_message_producer=False, async_message_producer=False,
                                           create_zookeeper_client=True)
        panoptes_context.__del__()
        with self.assertRaises(AttributeError):
            kv_stores = panoptes_context.__kv_stores
        with self.assertRaises(AttributeError):
            redis_pool = panoptes_context.__redis_pool
        with self.assertRaises(AttributeError):
            message_producer = panoptes_context.__message_producer
        with self.assertRaises(AttributeError):
            kafka_client = panoptes_context.__kafka_client
        with self.assertRaises(AttributeError):
            zookeeper_client = panoptes_context.__zookeeper_client

        panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                           key_value_store_class_list=[PanoptesTestKeyValueStore],
                                           create_message_producer=False, async_message_producer=False,
                                           create_zookeeper_client=True)
        with self.assertRaises(AttributeError):
            del panoptes_context.__kv_stores
            panoptes_context.__del__()
        with self.assertRaises(AttributeError):
            del panoptes_context.__redis_pool
            panoptes_context.__del__()
        with self.assertRaises(AttributeError):
            del panoptes_context.__message_producer
            panoptes_context.__del__()
        with self.assertRaises(AttributeError):
            del panoptes_context.__kafka_client
            panoptes_context.__del__()
        with self.assertRaises(AttributeError):
            del panoptes_context.__zookeeper_client
            panoptes_context.__del__()

    def test_root_logger_error(self):
        mock_get_logger = Mock(side_effect=Exception)
        with patch('yahoo_panoptes.framework.context.logging.getLogger', mock_get_logger):
            with self.assertRaises(PanoptesContextError):
                PanoptesContext(self.panoptes_test_conf_file)

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_message_producer(self):
        mock_kafka_client = MagicMock(return_value=MockKafkaClient(kafka_brokers={'localhost:9092'}))
        with patch('yahoo_panoptes.framework.context.KafkaClient', mock_kafka_client):
            panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                               create_message_producer=True, async_message_producer=False)

            self.assertIsNotNone(panoptes_context.message_producer)

            #  Test error in message queue producer
            mock_panoptes_message_queue_producer = Mock(side_effect=Exception)
            with patch('yahoo_panoptes.framework.context.PanoptesMessageQueueProducer',
                       mock_panoptes_message_queue_producer):
                with self.assertRaises(PanoptesContextError):
                    PanoptesContext(self.panoptes_test_conf_file,
                                    create_message_producer=True, async_message_producer=True)

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_get_kafka_client(self):
        mock_kafka_client = MockKafkaClient(kafka_brokers={'localhost:9092'})
        mock_kafka_client_init = Mock(return_value=mock_kafka_client)
        with patch('yahoo_panoptes.framework.context.KafkaClient', mock_kafka_client_init):
            panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                               create_message_producer=True, async_message_producer=False)
            self.assertEqual(panoptes_context.kafka_client, mock_kafka_client)

    def test_get_panoptes_logger(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file)
        assert isinstance(panoptes_context._get_panoptes_logger(), logging.Logger)

        #  Test error raised when instantiating logger fails
        mock_get_child = Mock(side_effect=Exception)
        with patch('yahoo_panoptes.framework.context.PanoptesContext._PanoptesContext__rootLogger.getChild',
                   mock_get_child):
            with self.assertRaises(PanoptesContextError):
                PanoptesContext(self.panoptes_test_conf_file)

    @patch("tests.test_framework.PanoptesTestKeyValueStore.__init__")
    def test_get_kv_store_error(self, mock_init):
        mock_init.side_effect = Exception
        with self.assertRaises(PanoptesContextError):
            PanoptesContext(self.panoptes_test_conf_file,
                            key_value_store_class_list=[PanoptesTestKeyValueStore])

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_zookeeper_client(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                           create_zookeeper_client=True)
        assert isinstance(panoptes_context.zookeeper_client, FakeClient)

        mock_kazoo_client = Mock(side_effect=Exception)
        with patch('yahoo_panoptes.framework.context.kazoo.client.KazooClient', mock_kazoo_client):
            with self.assertRaises(PanoptesContextError):
                PanoptesContext(self.panoptes_test_conf_file,
                                create_zookeeper_client=True)

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    def test_get_redis_connection(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file)
        #  Test get_redis_connection
        with self.assertRaises(IndexError):
            panoptes_context.get_redis_connection("default", shard=1)
        self.assertIsNotNone(panoptes_context.get_redis_connection("dummy", shard=1))

        #  Test redis shard count error
        self.assertEqual(panoptes_context.get_redis_shard_count('dummy'), 1)
        with self.assertRaises(KeyError):
            panoptes_context.get_redis_shard_count('dummy', fallback_to_default=False)

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_get_lock(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                           create_zookeeper_client=True)

        #  Test bad input
        with self.assertRaises(AssertionError):
            panoptes_context.get_lock('path/to/node', 1, 1, "identifier")
        with self.assertRaises(AssertionError):
            panoptes_context.get_lock('/path/to/node', 0, 1, "identifier")
        with self.assertRaises(AssertionError):
            panoptes_context.get_lock('/path/to/node', 1, -1, "identifier")
        with self.assertRaises(AssertionError):
            panoptes_context.get_lock('/path/to/node', 1, 1)
        #  Test non-callable listener
        with self.assertRaises(AssertionError):
            panoptes_context.get_lock("/path/to/node", timeout=1, retries=1, identifier="test", listener=object())

        #  Test lock acquisition/release among multiple contenders
        lock = panoptes_context.get_lock("/path/to/node", timeout=1, retries=1, identifier="test")
        self.assertIsNotNone(lock)
        lock.release()

        lock2 = panoptes_context.get_lock("/path/to/node", timeout=1, retries=0, identifier="test")
        self.assertIsNotNone(lock2)

        lock3 = panoptes_context.get_lock("/path/to/node", timeout=1, retries=1, identifier="test")
        self.assertIsNone(lock3)
        lock2.release()

        #  Test adding a listener for the lock once acquired
        lock4 = panoptes_context.get_lock("/path/to/node", timeout=1, retries=1, identifier="test", listener=object)
        self.assertIsNotNone(lock4)

        mock_zookeeper_client = MockZookeeperClient()
        with patch('yahoo_panoptes.framework.context.PanoptesContext.zookeeper_client', mock_zookeeper_client):
            self.assertIsNone(panoptes_context.get_lock("/path/to/node", timeout=5, retries=1, identifier="test"))


class TestPanoptesConfiguration(unittest.TestCase):
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = _get_test_conf_file()

    def test_configuration(self):
        logger = getLogger(__name__)

        with self.assertRaises(AssertionError):
            PanoptesConfig(logger=logger)

        mock_config = Mock(side_effect=ConfigObjError)
        with patch('yahoo_panoptes.framework.configuration_manager.ConfigObj', mock_config):
            with self.assertRaises(ConfigObjError):
                PanoptesConfig(logger=logger, conf_file=self.panoptes_test_conf_file)

        test_config = PanoptesConfig(logger=logger, conf_file=self.panoptes_test_conf_file)
        _SNMP_DEFAULTS = {'retries': 1, 'timeout': 5, 'community': 'public', 'proxy_port': 10161,
                          'community_string_key': 'snmp_community_string', 'non_repeaters': 0, 'max_repetitions': 25,
                          'connection_factory_class': 'PanoptesSNMPConnectionFactory', 'port': 10161,
                          'connection_factory_module': 'yahoo_panoptes.framework.utilities.snmp.connection'}
        self.assertEqual(test_config.snmp_defaults, _SNMP_DEFAULTS)
        self.assertSetEqual(test_config.sites, {'local'})

        #  Test exception is raised when plugin_type is not specified in config file
        mock_plugin_types = ['dummy']
        with patch('yahoo_panoptes.framework.configuration_manager.const.PLUGIN_TYPES', mock_plugin_types):
            with self.assertRaises(Exception):
                PanoptesConfig(logger=logger, conf_file=self.panoptes_test_conf_file)


class TestPanoptesRedisConnectionConfiguration(unittest.TestCase):
    def test_basic_operations(self):
        panoptes_redis_connection_config = PanoptesRedisConnectionConfiguration(group="test_group",
                                                                                namespace="test_namespace",
                                                                                shard="test_shard",
                                                                                host="test_host",
                                                                                port=123,
                                                                                db="test_db",
                                                                                password=None)
        assert repr(panoptes_redis_connection_config) == panoptes_redis_connection_config.url


class TestPanoptesPluginInfo(unittest.TestCase):
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = _get_test_conf_file()

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_plugininfo_repr(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                           key_value_store_class_list=[PanoptesTestKeyValueStore],
                                           create_message_producer=False, async_message_producer=False,
                                           create_zookeeper_client=True)

        panoptes_resource = PanoptesResource(resource_site='test', resource_class='test',
                                             resource_subclass='test',
                                             resource_type='test', resource_id='test', resource_endpoint='test',
                                             resource_plugin='test')

        panoptes_plugininfo = PanoptesPluginInfo("plugin_name", "plugin_path")
        panoptes_plugininfo.panoptes_context = panoptes_context
        panoptes_plugininfo.data = panoptes_resource
        panoptes_plugininfo.kv_store_class = PanoptesTestKeyValueStore
        panoptes_plugininfo.last_executed = "1458947997"
        panoptes_plugininfo.last_results = "1458948005"

        repr_string = "PanoptesPluginInfo: Normalized name: plugin__name, Config file: None, "\
                      "Panoptes context: " \
                      "[PanoptesContext: KV Stores: [PanoptesTestKeyValueStore], "\
                      "Config: ConfigObj({'main': {'sites': " \
                      "['local'], 'plugins_extension': 'panoptes-plugin', 'plugins_skew': 1}, " \
                      "'log': " \
                      "{'config_file': 'tests/config_files/test_panoptes_logging.ini', " \
                      "'rate': 1000, " \
                      "'per': 1, " \
                      "'burst': 10000, " \
                      "'formatters': {'keys': ['root_log_format', 'log_file_format', 'discovery_plugins_format']}}, " \
                      "'redis': {'default': {'namespace': 'panoptes', "\
                      "'shards': {'shard1': {'host': 'localhost', 'port': 6379, 'db': 0, 'password': '**'}}}}, "\
                      "'kafka': {'topic_key_delimiter': ':', 'topic_name_delimiter': '-', " \
                      "'brokers': {'broker1': {'host': 'localhost', 'port': 9092}}, " \
                      "'topics': " \
                      "{'metrics': {'raw_topic_name_suffix': 'metrics', " \
                      "'transformed_topic_name_suffix': 'processed'}}}, " \
                      "'zookeeper': {'connection_timeout': 30, 'servers': {'server1': {'host': " \
                      "'localhost', 'port': 2181}}}, " \
                      "'discovery': " \
                      "{'plugins_paths': ['tests/plugins/discovery'], " \
                      "'plugin_scan_interval': 60, " \
                      "'celerybeat_max_loop_interval': 5}, " \
                      "'polling': " \
                      "{'plugins_paths': ['tests/plugins/polling'], " \
                      "'plugin_scan_interval': 60, " \
                      "'celerybeat_max_loop_interval': 5}, " \
                      "'enrichment': " \
                      "{'plugins_paths': ['tests/plugins/enrichment'], " \
                      "'plugin_scan_interval': 60, " \
                      "'celerybeat_max_loop_interval': 5}, " \
                      "'snmp': " \
                      "{'port': 10161, " \
                      "'connection_factory_module': 'yahoo_panoptes.framework.utilities.snmp.connection', " \
                      "'connection_factory_class': 'PanoptesSNMPConnectionFactory', " \
                      "'community': '**', 'timeout': 5, 'retries': 1, 'non_repeaters': 0, 'max_repetitions': 25, " \
                      "'proxy_port': 10161, 'community_string_key': 'snmp_community_string'}}), "\
                      "Redis pool set: False, " \
                      "Message producer set: False, " \
                      "Kafka client set: False, " \
                      "Zookeeper client set: False], " \
                      "KV store class: PanoptesTestKeyValueStore, " \
                      "Last executed timestamp: 1458947997, " \
                      "Last executed key: " \
                      "plugin_metadata:plugin__name:be7eabbca3b05b9aaa8c81201aa0ca3e:last_executed, " \
                      "Last results timestamp: 1458948005, " \
                      "Last results key: plugin_metadata:plugin__name:be7eabbca3b05b9aaa8c81201aa0ca3e:last_results, " \
                      "Data: Data object passed, " \
                      "Lock: Lock is set"
        self.assertEqual(repr(panoptes_plugininfo), repr_string)


def _get_test_conf_file():
    my_dir = os.path.dirname(os.path.realpath(__file__))
    panoptes_test_conf_file = os.path.join(my_dir, 'config_files/test_panoptes_config.ini')

    return my_dir, panoptes_test_conf_file


if __name__ == '__main__':
    unittest.main()
