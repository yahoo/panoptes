"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
from __future__ import absolute_import

from builtins import str
from builtins import object
import collections
import glob
import json
import time
import unittest

from celery import Celery
from celery.schedules import crontab
from datetime import datetime, timedelta
from logging import getLogger, _loggerClass
from mock import patch, Mock, MagicMock
from mockredis import MockRedis
from redis.exceptions import TimeoutError
from zake.fake_client import FakeClient

from yahoo_panoptes.framework.utilities.snmp.mibs import base
from yahoo_panoptes.framework.celery_manager import PanoptesCeleryError, PanoptesCeleryConfig, \
    PanoptesCeleryValidators, PanoptesCeleryInstance, PanoptesCeleryPluginScheduler
from yahoo_panoptes.framework.configuration_manager import *
from yahoo_panoptes.framework.const import RESOURCE_MANAGER_RESOURCE_EXPIRE
from yahoo_panoptes.framework.context import *
from yahoo_panoptes.framework.resources import PanoptesResource, PanoptesResourceSet, PanoptesResourceDSL, \
    PanoptesContext, ParseException, ParseResults, PanoptesResourcesKeyValueStore, PanoptesResourceStore, \
    PanoptesResourceCache, PanoptesResourceError, PanoptesResourceCacheException, PanoptesResourceEncoder
from yahoo_panoptes.framework.metrics import PanoptesMetricType
from yahoo_panoptes.framework.utilities.helpers import ordered
from yahoo_panoptes.framework.utilities.key_value_store import PanoptesKeyValueStore

from .mock_kafka_consumer import MockKafkaConsumer
from tests.mock_panoptes_producer import MockPanoptesKeyedProducer
from .helpers import get_test_conf_file

_TIMESTAMP = round(time.time(), 5)
DUMMY_TIME = 1569967062.65

mock_time = Mock()
mock_time.return_value = DUMMY_TIME


def _get_test_conf_file():
    my_dir = os.path.dirname(os.path.realpath(__file__))
    panoptes_test_conf_file = os.path.join(my_dir, u'config_files/test_panoptes_config.ini')

    return my_dir, panoptes_test_conf_file


class PanoptesMockRedis(MockRedis):
    def __init__(self, bad_connection=False, timeout=False, **kwargs):
        if bad_connection:
            raise ConnectionError
        super(PanoptesMockRedis, self).__init__(**kwargs)
        self.connection_pool = u'mockredis connection pool'
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
        super(PanoptesTestKeyValueStore, self).__init__(panoptes_context, u'panoptes_test')


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

    def ensure_topic_exists(self, topic):
        pass

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
        self.__panoptes_resource_metadata = {u'test': u'test', u'_resource_ttl': u'604800'}
        self.__panoptes_resource = PanoptesResource(resource_site=u'test', resource_class=u'test',
                                                    resource_subclass=u'test',
                                                    resource_type=u'test', resource_id=u'test',
                                                    resource_endpoint=u'test',
                                                    resource_plugin=u'test',
                                                    resource_creation_timestamp=_TIMESTAMP,
                                                    resource_ttl=RESOURCE_MANAGER_RESOURCE_EXPIRE)
        self.__panoptes_resource.add_metadata(u'test', u'test')
        self.__panoptes_resource_set = PanoptesResourceSet()
        mock_valid_timestamp = Mock(return_value=True)
        with patch(u'yahoo_panoptes.framework.resources.PanoptesValidators.valid_timestamp',
                   mock_valid_timestamp):
            self.__panoptes_resource_set.resource_set_creation_timestamp = _TIMESTAMP
        self.my_dir, self.panoptes_test_conf_file = _get_test_conf_file()

    def test_panoptes_resource(self):
        panoptes_resource_metadata = self.__panoptes_resource_metadata
        panoptes_resource = self.__panoptes_resource

        self.assertIsInstance(panoptes_resource, PanoptesResource)
        self.assertEqual(panoptes_resource.resource_site, u'test')
        self.assertEqual(panoptes_resource.resource_class, u'test')
        self.assertEqual(panoptes_resource.resource_subclass, u'test')
        self.assertEqual(panoptes_resource.resource_type, u'test')
        self.assertEqual(panoptes_resource.resource_id, u'test')
        self.assertEqual(panoptes_resource.resource_endpoint, u'test')
        self.assertEqual(panoptes_resource.resource_metadata, panoptes_resource_metadata)
        self.assertEqual(panoptes_resource.resource_ttl, str(RESOURCE_MANAGER_RESOURCE_EXPIRE))
        self.assertEqual(panoptes_resource, panoptes_resource)
        self.assertFalse(panoptes_resource == u'1')
        self.assertIsInstance(str(panoptes_resource), str)

        with self.assertRaises(AssertionError):
            panoptes_resource.add_metadata(None, u'test')
        with self.assertRaises(AssertionError):
            panoptes_resource.add_metadata(u'', u'test')
        with self.assertRaises(ValueError):
            panoptes_resource.add_metadata(u'1', u'test')
        with self.assertRaises(ValueError):
            panoptes_resource.add_metadata(u'key', u'test|')
        with self.assertRaises(AssertionError):
            PanoptesResource(resource_site=u'', resource_class=u'test', resource_subclass=u'test',
                             resource_type=u'test', resource_id=u'test', resource_endpoint=u'test')
        with self.assertRaises(AssertionError):
            PanoptesResource(resource_site=u'test', resource_class=u'', resource_subclass=u'test',
                             resource_type=u'test', resource_id=u'test', resource_endpoint=u'test')
        with self.assertRaises(AssertionError):
            PanoptesResource(resource_site=u'test', resource_class=u'test', resource_subclass=u'',
                             resource_type=u'test', resource_id=u'test', resource_endpoint=u'test')
        with self.assertRaises(AssertionError):
            PanoptesResource(resource_site=u'test', resource_class=u'test', resource_subclass=u'test',
                             resource_type=u'', resource_id=u'test', resource_endpoint=u'test')
        with self.assertRaises(AssertionError):
            PanoptesResource(resource_site=u'test', resource_class=u'test', resource_subclass=u'test',
                             resource_type=u'test', resource_id=u'', resource_endpoint=u'test')
        with self.assertRaises(AssertionError):
            PanoptesResource(resource_site=None, resource_class=u'test', resource_subclass=u'test',
                             resource_type=u'test', resource_id=u'test', resource_endpoint=u'test')
        with self.assertRaises(AssertionError):
            PanoptesResource(resource_site=u'test', resource_class=None, resource_subclass=u'test',
                             resource_type=u'test', resource_id=u'test', resource_endpoint=u'test')
        with self.assertRaises(AssertionError):
            PanoptesResource(resource_site=u'test', resource_class=u'test', resource_subclass=None,
                             resource_type=u'test', resource_id=u'test', resource_endpoint=u'test')
        with self.assertRaises(AssertionError):
            PanoptesResource(resource_site=u'test', resource_class=u'test', resource_subclass=u'test',
                             resource_type=None, resource_id=u'test', resource_endpoint=u'test')
        with self.assertRaises(AssertionError):
            PanoptesResource(resource_site=u'test', resource_class=u'test', resource_subclass=u'test',
                             resource_type=u'test', resource_id=None, resource_endpoint=u'test')
        with self.assertRaises(AssertionError):
            PanoptesResource(resource_site=u'test', resource_class=u'test', resource_subclass=u'test',
                             resource_type=u'test', resource_id=u'test', resource_endpoint=None)

        # Test json and raw representations of PanoptesResource
        panoptes_resource_2 = PanoptesResource(resource_site=u'test', resource_class=u'test',
                                               resource_subclass=u'test',
                                               resource_type=u'test', resource_id=u'test', resource_endpoint=u'test',
                                               resource_creation_timestamp=_TIMESTAMP,
                                               resource_plugin=u'test')
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
            [(u'resource_site', u'test'),
             (u'resource_class', u'test'),
             (u'resource_subclass', u'test'),
             (u'resource_type', u'test'),
             (u'resource_id', u'test'),
             (u'resource_endpoint', u'test'),
             (u'resource_metadata', collections.OrderedDict(
                 [(u'_resource_ttl', u'604800')])
              ),
             (u'resource_creation_timestamp', _TIMESTAMP),
             (u'resource_plugin', u'test')])
        self.assertEqual(panoptes_resource_2.raw, panoptes_resource_2_raw)

        # Test resource creation from dict
        with open(u'tests/test_resources/input/resource_one.json') as f:
            resource_specs = json.load(f)
        resource_from_json = PanoptesResource.resource_from_dict(resource_specs[u'resources'][0])
        panoptes_resource_3 = PanoptesResource(resource_site=u"test_site", resource_class=u"network",
                                               resource_subclass=u"test_subclass", resource_type=u"test_type",
                                               resource_id=u"test_id_1", resource_endpoint=u"test_endpoint_1",
                                               resource_plugin=u"key")
        self.assertEqual(resource_from_json, panoptes_resource_3)

    def test_panoptes_resource_set(self):
        panoptes_resource = self.__panoptes_resource
        panoptes_resource_set = self.__panoptes_resource_set
        self.assertEqual(panoptes_resource_set.add(panoptes_resource), None)
        self.assertEqual(len(panoptes_resource_set), 1)
        self.assertEqual(type(panoptes_resource_set.resources), set)
        self.assertIsInstance(str(panoptes_resource_set), str)
        self.assertIsInstance(iter(panoptes_resource_set), collections.Iterable)
        self.assertEqual(next(panoptes_resource_set), panoptes_resource)
        self.assertEqual(panoptes_resource_set.remove(panoptes_resource), None)
        self.assertEqual(len(panoptes_resource_set), 0)
        self.assertEqual(panoptes_resource_set.resource_set_creation_timestamp, _TIMESTAMP)
        with self.assertRaises(AssertionError):
            panoptes_resource_set.resource_set_creation_timestamp = 0

        panoptes_resource_2 = PanoptesResource(resource_site=u'test', resource_class=u'test',
                                               resource_subclass=u'test',
                                               resource_type=u'test', resource_id=u'test2',
                                               resource_endpoint=u'test',
                                               resource_plugin=u'test',
                                               resource_ttl=RESOURCE_MANAGER_RESOURCE_EXPIRE,
                                               resource_creation_timestamp=_TIMESTAMP)
        panoptes_resource_set.add(panoptes_resource)
        panoptes_resource_set.add(panoptes_resource_2)
        self.assertEqual(len(panoptes_resource_set.get_resources_by_site()[u'test']), 2)
        self.assertEqual(panoptes_resource_set.resource_set_schema_version, u"0.1")

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

    @patch(u'redis.StrictRedis', panoptes_mock_redis_strict_client)
    def test_panoptes_resource_store(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                           key_value_store_class_list=[PanoptesTestKeyValueStore])
        with self.assertRaises(Exception):
            PanoptesResourceStore(panoptes_context)

        mock_kv_store = Mock(return_value=PanoptesTestKeyValueStore(panoptes_context))
        with patch(u'yahoo_panoptes.framework.resources.PanoptesContext.get_kv_store', mock_kv_store):
            panoptes_resource_store = PanoptesResourceStore(panoptes_context)
            panoptes_resource_store.add_resource(u"test_plugin_signature", self.__panoptes_resource)
            resource_key = u"plugin|test|site|test|class|test|subclass|test|type|test|id|test|endpoint|test"
            resource_value = panoptes_resource_store.get_resource(resource_key)
            self.assertEqual(self.__panoptes_resource, resource_value)

            panoptes_resource_2 = PanoptesResource(resource_site=u'test', resource_class=u'test',
                                                   resource_subclass=u'test',
                                                   resource_type=u'test', resource_id=u'test2',
                                                   resource_endpoint=u'test',
                                                   resource_plugin=u'test',
                                                   resource_ttl=RESOURCE_MANAGER_RESOURCE_EXPIRE,
                                                   resource_creation_timestamp=_TIMESTAMP)
            panoptes_resource_store.add_resource(u"test_plugin_signature", panoptes_resource_2)
            self.assertIn(self.__panoptes_resource, panoptes_resource_store.get_resources())
            self.assertIn(panoptes_resource_2, panoptes_resource_store.get_resources())

            panoptes_resource_store.delete_resource(u"test_plugin_signature", panoptes_resource_2)
            self.assertNotIn(panoptes_resource_2, panoptes_resource_store.get_resources())

            panoptes_resource_3 = PanoptesResource(resource_site=u'test', resource_class=u'test',
                                                   resource_subclass=u'test',
                                                   resource_type=u'test', resource_id=u'test3',
                                                   resource_endpoint=u'test',
                                                   resource_plugin=u'test3',
                                                   resource_ttl=RESOURCE_MANAGER_RESOURCE_EXPIRE,
                                                   resource_creation_timestamp=_TIMESTAMP)
            panoptes_resource_store.add_resource(u"test_plugin_signature", panoptes_resource_3)
            self.assertIn(panoptes_resource_3, panoptes_resource_store.get_resources(site=u'test',
                                                                                     plugin_name=u'test3'))
            self.assertNotIn(self.__panoptes_resource,
                             panoptes_resource_store.get_resources(site=u'test', plugin_name=u'test3'))

            # Test key not found
            mock_find_keys = Mock(
                return_value=[u'dummy',
                              u'plugin|test|site|test|class|test|subclass|test|type|test|id|test|endpoint|test'])
            with patch(u'yahoo_panoptes.framework.resources.PanoptesKeyValueStore.find_keys',
                       mock_find_keys):
                self.assertEqual(1, len(panoptes_resource_store.get_resources()))

            # Test resource store methods raise correct errors
            mock_get = Mock(side_effect=Exception)
            with patch(u'yahoo_panoptes.framework.resources.PanoptesKeyValueStore.get', mock_get):
                with self.assertRaises(PanoptesResourceError):
                    panoptes_resource_store.get_resource(u'test3')

            # Test bad input
            with self.assertRaises(AssertionError):
                panoptes_resource_store.get_resource(u"")
            with self.assertRaises(AssertionError):
                panoptes_resource_store.get_resource(1)

            with self.assertRaises(AssertionError):
                panoptes_resource_store.add_resource(u"", panoptes_resource_2)
            with self.assertRaises(AssertionError):
                panoptes_resource_store.add_resource(u"test_plugin_signature", None)
            with self.assertRaises(AssertionError):
                panoptes_resource_store.add_resource(u"test_plugin_signature", PanoptesResourceStore(panoptes_context))

            with self.assertRaises(AssertionError):
                panoptes_resource_store.delete_resource(u"", panoptes_resource_2)
            with self.assertRaises(AssertionError):
                panoptes_resource_store.delete_resource(u"test_plugin_signature", None)
            with self.assertRaises(AssertionError):
                panoptes_resource_store.delete_resource(u"test_plugin_signature",
                                                        PanoptesResourceStore(panoptes_context))

            with self.assertRaises(AssertionError):
                panoptes_resource_store.get_resources(u"", u"test_plugin_name")
            with self.assertRaises(AssertionError):
                panoptes_resource_store.get_resources(u"test_site", u"")
            with self.assertRaises(AssertionError):
                panoptes_resource_store.get_resources(1, u"test_plugin_name")
            with self.assertRaises(AssertionError):
                panoptes_resource_store.get_resources(u"test_site", 1)

            # Test non-existent key
            with self.assertRaises(PanoptesResourceError):
                panoptes_resource_store.get_resource(u'tes')

            mock_set = Mock(side_effect=Exception)
            with patch(u'yahoo_panoptes.framework.resources.PanoptesKeyValueStore.set', mock_set):
                with self.assertRaises(PanoptesResourceError):
                    panoptes_resource_store.add_resource(u"test_plugin_signature", panoptes_resource_2)

            mock_delete = Mock(side_effect=Exception)
            with patch(u'yahoo_panoptes.framework.resources.PanoptesKeyValueStore.delete', mock_delete):
                with self.assertRaises(PanoptesResourceError):
                    panoptes_resource_store.delete_resource(u"test_plugin_signature", panoptes_resource_2)

            with self.assertRaises(PanoptesResourceError):
                panoptes_resource_store._deserialize_resource(u"tes", u"null")

            with self.assertRaises(PanoptesResourceError):
                panoptes_resource_store._deserialize_resource(resource_key, u"null")

    @patch(u'redis.StrictRedis', panoptes_mock_redis_strict_client)
    def test_resource_dsl_parsing(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file)

        test_query = u'resource_class = "network" AND resource_subclass = "load-balancer"'

        test_result = (
            u'SELECT resources.*, group_concat(key,"|"), group_concat(value,"|") ' +
            u'FROM resources ' +
            u'LEFT JOIN resource_metadata ON resources.id = resource_metadata.id ' +
            u'WHERE (resources.resource_class = "network" ' +
            u'AND resources.resource_subclass = "load-balancer") ' +
            u'GROUP BY resource_metadata.id ' +
            u'ORDER BY resource_metadata.id'
        )

        panoptes_resource_dsl = PanoptesResourceDSL(test_query, panoptes_context)
        self.assertEqual(panoptes_resource_dsl.sql, test_result)

        # This very long query tests all code paths with the DSL parser:
        test_query = u'resource_class = "network" AND resource_subclass = "load-balancer" OR \
                resource_metadata.os_version LIKE "4%" AND resource_site NOT IN ("test_site") \
                AND resource_endpoint IN ("test1","test2") AND resource_type != "a10" OR ' \
                     u'resource_metadata.make NOT LIKE \
                "A10%" AND resource_metadata.model NOT IN ("test1", "test2")'

        test_result = (
            u'SELECT resources.*,group_concat(key,"|"),group_concat(value,"|") FROM (SELECT resource_metadata.id ' +
            u'FROM resources,resource_metadata WHERE (resources.resource_class = "network" ' +
            u'AND resources.resource_subclass = "load-balancer" ' +
            u'AND resources.resource_site NOT IN ("test_site") ' +
            u'AND resources.resource_endpoint IN ("test1","test2") ' +
            u'AND resources.resource_type != "a10" ' +
            u'AND ((resource_metadata.key = "os_version" ' +
            u'AND resource_metadata.value LIKE "4%")) ' +
            u'AND resource_metadata.id = resources.id) ' +
            u'UNION SELECT resource_metadata.id ' +
            u'FROM resources,resource_metadata WHERE (resource_metadata.key = "make" ' +
            u'AND resource_metadata.value NOT LIKE "A10%") ' +
            u'AND resource_metadata.id = resources.id ' +
            u'INTERSECT SELECT resource_metadata.id ' +
            u'FROM resources,resource_metadata WHERE (resource_metadata.key = "model" ' +
            u'AND resource_metadata.value NOT IN ("test1","test2")) ' +
            u'AND resource_metadata.id = resources.id ' +
            u'GROUP BY resource_metadata.id ' +
            u'ORDER BY resource_metadata.id) AS filtered_resources, ' +
            u'resources, resource_metadata WHERE resources.id = filtered_resources.id ' +
            u'AND resource_metadata.id = filtered_resources.id GROUP BY resource_metadata.id')

        panoptes_resource_dsl = PanoptesResourceDSL(test_query, panoptes_context)
        self.assertEqual(panoptes_resource_dsl.sql, test_result)

        panoptes_resource_dsl = PanoptesResourceDSL(u'resource_site = "local"', panoptes_context)
        self.assertIsInstance(panoptes_resource_dsl.tokens, ParseResults)

        with self.assertRaises(AssertionError):
            PanoptesResourceDSL(None, panoptes_context)
        with self.assertRaises(AssertionError):
            PanoptesResourceDSL(u'', None)
        with self.assertRaises(AssertionError):
            PanoptesResourceDSL(u'', panoptes_context)
        with self.assertRaises(ParseException):
            PanoptesResourceDSL(u'resources_site = local', panoptes_context)


class TestPanoptesResourceEncoder(unittest.TestCase):
    def setUp(self):
        self.__panoptes_resource = PanoptesResource(resource_site=u'test', resource_class=u'test',
                                                    resource_subclass=u'test',
                                                    resource_type=u'test', resource_id=u'test',
                                                    resource_endpoint=u'test',
                                                    resource_plugin=u'test',
                                                    resource_creation_timestamp=_TIMESTAMP,
                                                    resource_ttl=RESOURCE_MANAGER_RESOURCE_EXPIRE)

    def test_panoptes_resource_encoder(self):
        resource_encoder = PanoptesResourceEncoder()
        self.assertEqual(resource_encoder.default(set()), list(set()))
        self.assertEqual(resource_encoder.default(self.__panoptes_resource),
                         self.__panoptes_resource.__dict__[u'_PanoptesResource__data'])
        with self.assertRaises(TypeError):
            resource_encoder.default(dict())


class TestPanoptesResourceCache(unittest.TestCase):
    def setUp(self):
        self.__panoptes_resource = PanoptesResource(resource_site=u'test', resource_class=u'test',
                                                    resource_subclass=u'test',
                                                    resource_type=u'test', resource_id=u'test',
                                                    resource_endpoint=u'test',
                                                    resource_plugin=u'test',
                                                    resource_creation_timestamp=_TIMESTAMP,
                                                    resource_ttl=RESOURCE_MANAGER_RESOURCE_EXPIRE)
        self.my_dir, self.panoptes_test_conf_file = _get_test_conf_file()

    @patch(u'redis.StrictRedis', panoptes_mock_redis_strict_client)
    def test_resource_cache(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                           key_value_store_class_list=[PanoptesTestKeyValueStore])

        #  Test PanoptesResourceCache methods when setup_resource_cache not yet called
        panoptes_resource_cache = PanoptesResourceCache(panoptes_context)
        test_query = u'resource_class = "network"'
        with self.assertRaises(PanoptesResourceCacheException):
            panoptes_resource_cache.get_resources(test_query)
        panoptes_resource_cache.close_resource_cache()

        panoptes_resource = self.__panoptes_resource
        panoptes_resource.add_metadata(u"metadata_key1", u"test")
        panoptes_resource.add_metadata(u"metadata_key2", u"test")

        kv = panoptes_context.get_kv_store(PanoptesTestKeyValueStore)
        serialized_key, serialized_value = PanoptesResourceStore._serialize_resource(panoptes_resource)
        kv.set(serialized_key, serialized_value)

        mock_kv_store = Mock(return_value=kv)

        with patch(u'yahoo_panoptes.framework.resources.PanoptesContext.get_kv_store', mock_kv_store):
            # Test errors when setting up resource cache
            mock_resource_store = Mock(side_effect=Exception)
            with patch(u'yahoo_panoptes.framework.resources.PanoptesResourceStore', mock_resource_store):
                with self.assertRaises(PanoptesResourceCacheException):
                    panoptes_resource_cache.setup_resource_cache()

            mock_connect = Mock(side_effect=Exception)
            with patch(u'yahoo_panoptes.framework.resources.sqlite3.connect', mock_connect):
                with self.assertRaises(PanoptesResourceCacheException):
                    panoptes_resource_cache.setup_resource_cache()

            mock_get_resources = Mock(side_effect=Exception)
            with patch(u'yahoo_panoptes.framework.resources.PanoptesResourceStore.get_resources', mock_get_resources):
                with self.assertRaises(PanoptesResourceCacheException):
                    panoptes_resource_cache.setup_resource_cache()

            # Test basic operations
            panoptes_resource_cache.setup_resource_cache()
            self.assertIsInstance(panoptes_resource_cache.get_resources(u'resource_class = "network"'),
                                  PanoptesResourceSet)
            self.assertEqual(len(panoptes_resource_cache.get_resources(u'resource_class = "network"')), 0)
            self.assertIn(panoptes_resource, panoptes_resource_cache.get_resources(u'resource_class = "test"'))
            self.assertEqual(len(panoptes_resource_cache._cached_resources), 2)

            panoptes_resource_cache.close_resource_cache()

            # Mock PanoptesResourceStore.get_resources to return Resources that otherwise couldn't be constructed:
            mock_resources = PanoptesResourceSet()
            mock_resources.add(panoptes_resource)
            bad_panoptes_resource = PanoptesResource(resource_site=u'test', resource_class=u'test',
                                                     resource_subclass=u'test',
                                                     resource_type=u'test', resource_id=u'test2',
                                                     resource_endpoint=u'test',
                                                     resource_plugin=u'test',
                                                     resource_creation_timestamp=_TIMESTAMP,
                                                     resource_ttl=RESOURCE_MANAGER_RESOURCE_EXPIRE)
            bad_panoptes_resource.__dict__[u'_PanoptesResource__data'][u'resource_metadata'][u'*'] = u"test"
            bad_panoptes_resource.__dict__[u'_PanoptesResource__data'][u'resource_metadata'][u'**'] = u"test"
            mock_resources.add(bad_panoptes_resource)
            mock_get_resources = Mock(return_value=mock_resources)
            with patch(u'yahoo_panoptes.framework.resources.PanoptesResourceStore.get_resources', mock_get_resources):
                panoptes_resource_cache.setup_resource_cache()
                self.assertEqual(len(panoptes_resource_cache.get_resources(u'resource_class = "test"')), 1)


class TestPanoptesContext(unittest.TestCase):
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = _get_test_conf_file()

    def test_context_config_file(self):

        # Test invalid inputs for config_file
        with self.assertRaises(AssertionError):
            PanoptesContext(u'')

        with self.assertRaises(AssertionError):
            PanoptesContext(1)

        with self.assertRaises(PanoptesContextError):
            PanoptesContext(config_file=u'non.existent.config.file')

        # Test that the default config file is loaded if no config file is present in the arguments or environment
        with patch(u'yahoo_panoptes.framework.const.DEFAULT_CONFIG_FILE_PATH', self.panoptes_test_conf_file):
            panoptes_context = PanoptesContext()
            self.assertEqual(panoptes_context.config_object.redis_urls[0].url, u'redis://:password@localhost:6379/0')
            del panoptes_context

        # Test that the config file from environment is loaded, if present
        os.environ[const.CONFIG_FILE_ENVIRONMENT_VARIABLE] = self.panoptes_test_conf_file
        panoptes_context = PanoptesContext()
        self.assertEqual(panoptes_context.config_object.redis_urls[0].url, u'redis://:password@localhost:6379/0')
        del panoptes_context

        # Test config file with redis sentinel
        redis_sentinel_config_file = os.path.join(self.my_dir, u'config_files/test_panoptes_config_redis_sentinel.ini')
        panoptes_context = PanoptesContext(config_file=redis_sentinel_config_file)
        sentinels = panoptes_context.config_object.redis_urls_by_group[u'celery'][0].sentinels
        self.assertEqual(
            sentinels,
            [
                Sentinel(host=u'localhost', port=26379, password=u'password'),
                Sentinel(host=u'otherhost', port=26379, password=u'password')
            ]
        )
        self.assertEqual(panoptes_context.config_object.redis_urls_by_group[u'celery'][0].master_name,
                         u'panoptes_default_1')
        self.assertEqual(panoptes_context.config_object.redis_urls_by_group[u'celery'][0].db, 0)
        self.assertEqual(panoptes_context.config_object.redis_urls_by_group[u'celery'][0].master_password,
                         u'password_for_master_1')
        self.assertEqual(
            str(panoptes_context.config_object.redis_urls_by_group[u'celery'][0]),
            u'sentinel://:**@localhost:26379,sentinel://:**@otherhost:26379'
        )

        self.assertEqual(panoptes_context.config_object.redis_urls_by_group[u'celery'][1].master_name,
                         u'panoptes_default_2')
        self.assertEqual(panoptes_context.config_object.redis_urls_by_group[u'celery'][1].db, 0)
        self.assertEqual(panoptes_context.config_object.redis_urls_by_group[u'celery'][1].master_password,
                         u'password_2')
        self.assertEqual(
            str(panoptes_context.config_object.redis_urls_by_group[u'celery'][1]),
            u'sentinel://:**@localhost:26379,sentinel://:**@otherhost:26379'
        )
        del panoptes_context

        # Test bad config configuration files
        for f in glob.glob(os.path.join(self.my_dir, u'config_files/test_panoptes_config_bad_*.ini')):
            with self.assertRaises(PanoptesContextError):
                PanoptesContext(f)

    @patch(u'redis.StrictRedis', panoptes_mock_redis_strict_client)
    def test_context(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file)
        self.assertIsInstance(panoptes_context, PanoptesContext)
        self.assertEqual(panoptes_context.config_object.redis_urls[0].url, u'redis://:password@localhost:6379/0')
        self.assertEqual(str(panoptes_context.config_object.redis_urls[0]), u'redis://:**@localhost:6379/0')
        self.assertEqual(panoptes_context.config_object.zookeeper_servers, set([u'localhost:2181']))
        self.assertEqual(panoptes_context.config_object.kafka_brokers, set([u'localhost:9092']))
        self.assertIsInstance(panoptes_context.config_dict, dict)
        self.assertIsInstance(panoptes_context.logger, _loggerClass)
        self.assertIsInstance(panoptes_context.redis_pool, MockRedis)
        with self.assertRaises(AttributeError):
            panoptes_context.kafka_client
        with self.assertRaises(AttributeError):
            panoptes_context.zookeeper_client
        del panoptes_context

    @patch(u'redis.StrictRedis', panoptes_mock_redis_strict_client_bad_connection)
    def test_context_redis_bad_connection(self):
        with self.assertRaises(ConnectionError):
            PanoptesContext(self.panoptes_test_conf_file)

    @patch(u'logging.getLogger')
    def test_context_bad_logger(self, mock_logger):

        mock_logger.side_effect = Exception(u'Could Not Create Logger')

        with self.assertRaises(PanoptesContextError):
            PanoptesContext(self.panoptes_test_conf_file)

    @patch(u'redis.StrictRedis', panoptes_mock_redis_strict_client)
    def test_context_key_value_store(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                           key_value_store_class_list=[PanoptesTestKeyValueStore])
        self.assertIsInstance(panoptes_context, PanoptesContext)
        kv = panoptes_context.get_kv_store(PanoptesTestKeyValueStore)
        self.assertIsInstance(kv, PanoptesTestKeyValueStore)
        self.assertTrue(kv.set(u'test', u'test'))
        self.assertEqual(kv.get(u'test'), u'test')
        self.assertEqual(kv.get(u'non.existent.key'), None)

        with self.assertRaises(AssertionError):
            kv.set(None, None)
        with self.assertRaises(AssertionError):
            kv.set(u'test', None)
        with self.assertRaises(AssertionError):
            kv.set(None, u'test')
        with self.assertRaises(AssertionError):
            kv.set(1, u'test')
        with self.assertRaises(AssertionError):
            kv.set(u'test', 1)

        with self.assertRaises(PanoptesContextError):
            PanoptesContext(self.panoptes_test_conf_file, key_value_store_class_list=[None])
        with self.assertRaises(PanoptesContextError):
            PanoptesContext(self.panoptes_test_conf_file, key_value_store_class_list=[u'test'])
        with self.assertRaises(PanoptesContextError):
            PanoptesContext(self.panoptes_test_conf_file, key_value_store_class_list=[PanoptesMockRedis])

        panoptes_context = PanoptesContext(self.panoptes_test_conf_file)

        with self.assertRaises(PanoptesContextError):
            panoptes_context.get_kv_store(PanoptesTestKeyValueStore)

    @patch(u'redis.StrictRedis', panoptes_mock_redis_strict_client_timeout)
    def test_context_key_value_store_timeout(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                           key_value_store_class_list=[PanoptesTestKeyValueStore])
        kv = panoptes_context.get_kv_store(PanoptesTestKeyValueStore)
        with self.assertRaises(TimeoutError):
            kv.set(u'test', u'test')
        with self.assertRaises(TimeoutError):
            kv.get(u'test')

    @patch(u'redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch(u'kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_context_message_bus(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file, create_zookeeper_client=True)
        self.assertIsInstance(panoptes_context, PanoptesContext)
        self.assertIsInstance(panoptes_context.zookeeper_client, FakeClient)

    @patch(u'redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch(u'kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    @patch(u'yahoo_panoptes.framework.context.PanoptesContext._get_message_producer', MockKafkaConsumer)
    @patch(u'yahoo_panoptes.framework.context.PanoptesContext._get_kafka_client', MockKafkaConsumer)
    def test_context_del_methods(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                           key_value_store_class_list=[PanoptesTestKeyValueStore],
                                           create_message_producer=True, async_message_producer=False,
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
        with patch(u'yahoo_panoptes.framework.context.logging.getLogger', mock_get_logger):
            with self.assertRaises(PanoptesContextError):
                PanoptesContext(self.panoptes_test_conf_file)

    @patch(u'redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch(u'kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_message_producer(self):
        mock_kafka_client = MagicMock(return_value=MockKafkaClient(kafka_brokers={u'localhost:9092'}))
        with patch(u'yahoo_panoptes.framework.context.KafkaClient', mock_kafka_client):
            panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                               create_message_producer=True, async_message_producer=False)

            self.assertIsNotNone(panoptes_context.message_producer)

            #  Test error in message queue producer
            mock_panoptes_message_queue_producer = Mock(side_effect=Exception)
            with patch(u'yahoo_panoptes.framework.context.PanoptesMessageQueueProducer',
                       mock_panoptes_message_queue_producer):
                with self.assertRaises(PanoptesContextError):
                    PanoptesContext(self.panoptes_test_conf_file,
                                    create_message_producer=True, async_message_producer=True)

            with patch(u'kafka.KeyedProducer', MockPanoptesKeyedProducer):

                message_producer = PanoptesMessageQueueProducer(panoptes_context=panoptes_context)

                with self.assertRaises(AssertionError):
                    message_producer.send_messages(u'', u'test_key', u'{}')

                with self.assertRaises(AssertionError):
                    message_producer.send_messages(u'panoptes-metrics', u'', u'{}')

                with self.assertRaises(AssertionError):
                    message_producer.send_messages(u'panoptes-metrics', u'key', u'')

                # Make sure no error is thrown
                message_producer.send_messages(u'panoptes-metrics', u'key', u'{}')
                message_producer.send_messages(u'panoptes-metrics', u'key', u'{}', u'p_key')

    @patch(u'redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch(u'kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_get_kafka_client(self):
        mock_kafka_client = MockKafkaClient(kafka_brokers={u'localhost:9092'})
        mock_kafka_client_init = Mock(return_value=mock_kafka_client)
        with patch(u'yahoo_panoptes.framework.context.KafkaClient', mock_kafka_client_init):
            panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                               create_message_producer=True, async_message_producer=False)
            self.assertEqual(panoptes_context.kafka_client, mock_kafka_client)

    def test_get_panoptes_logger(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file)
        assert isinstance(panoptes_context._get_panoptes_logger(), logging.Logger)

        #  Test error raised when instantiating logger fails
        mock_get_child = Mock(side_effect=Exception)
        with patch(u'yahoo_panoptes.framework.context.PanoptesContext._PanoptesContext__rootLogger.getChild',
                   mock_get_child):
            with self.assertRaises(PanoptesContextError):
                PanoptesContext(self.panoptes_test_conf_file)

    @patch(u"tests.test_framework.PanoptesTestKeyValueStore.__init__")
    def test_get_kv_store_error(self, mock_init):
        mock_init.side_effect = Exception
        with self.assertRaises(PanoptesContextError):
            PanoptesContext(self.panoptes_test_conf_file,
                            key_value_store_class_list=[PanoptesTestKeyValueStore])

    @patch(u'redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch(u'kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_zookeeper_client(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                           create_zookeeper_client=True)
        assert isinstance(panoptes_context.zookeeper_client, FakeClient)

        mock_kazoo_client = Mock(side_effect=Exception)
        with patch(u'yahoo_panoptes.framework.context.kazoo.client.KazooClient', mock_kazoo_client):
            with self.assertRaises(PanoptesContextError):
                PanoptesContext(self.panoptes_test_conf_file,
                                create_zookeeper_client=True)

    @patch(u'redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch(u'redis.sentinel.Sentinel.discover_master', return_value=u'localhost:26379')
    def test_get_redis_connection(self, _):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file)
        # Test get_redis_connection
        with self.assertRaises(IndexError):
            panoptes_context.get_redis_connection(u"default", shard=1)
        self.assertIsNotNone(panoptes_context.get_redis_connection(u"dummy", shard=1))

        #  Test redis shard count error
        self.assertEqual(panoptes_context.get_redis_shard_count(u'dummy'), 1)
        with self.assertRaises(KeyError):
            panoptes_context.get_redis_shard_count(u'dummy', fallback_to_default=False)

        panoptes_sentinel_config = self.my_dir + u'/config_files/test_panoptes_config_redis_sentinel.ini'
        panoptes_context = PanoptesContext(panoptes_sentinel_config)

        with self.assertRaises(IndexError):
            panoptes_context._get_redis_connection(u"default", shard=1)

        panoptes_context._get_redis_connection(u"celery", shard=0)

    @patch(u'redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch(u'kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_get_lock(self):
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                           create_zookeeper_client=True)

        #  Test bad input
        with self.assertRaises(AssertionError):
            panoptes_context.get_lock(u'path/to/node', 1, 1, u"identifier")
        with self.assertRaises(AssertionError):
            panoptes_context.get_lock(u'/path/to/node', 0, 1, u"identifier")
        with self.assertRaises(AssertionError):
            panoptes_context.get_lock(u'/path/to/node', 1, -1, u"identifier")
        with self.assertRaises(AssertionError):
            panoptes_context.get_lock(u'/path/to/node', 1, 1)
        #  Test non-callable listener
        with self.assertRaises(AssertionError):
            panoptes_context.get_lock(u"/path/to/node", timeout=1, retries=1, identifier=u"test", listener=object())

        #  Test lock acquisition/release among multiple contenders
        lock = panoptes_context.get_lock(u"/path/to/node", timeout=1, retries=1, identifier=u"test")
        self.assertIsNotNone(lock)
        lock.release()

        lock2 = panoptes_context.get_lock(u"/path/to/node", timeout=1, retries=0, identifier=u"test")
        self.assertIsNotNone(lock2)

        lock3 = panoptes_context.get_lock(u"/path/to/node", timeout=1, retries=1, identifier=u"test")
        self.assertIsNone(lock3)
        lock2.release()

        #  Test adding a listener for the lock once acquired
        lock4 = panoptes_context.get_lock(u"/path/to/node", timeout=1, retries=1, identifier=u"test", listener=object)
        self.assertIsNotNone(lock4)

        mock_zookeeper_client = MockZookeeperClient()
        with patch(u'yahoo_panoptes.framework.context.PanoptesContext.zookeeper_client', mock_zookeeper_client):
            self.assertIsNone(panoptes_context.get_lock(u"/path/to/node", timeout=5, retries=1, identifier=u"test"))


class TestPanoptesConfiguration(unittest.TestCase):
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = _get_test_conf_file()

    def test_configuration(self):
        logger = getLogger(__name__)

        with self.assertRaises(AssertionError):
            PanoptesConfig(logger=logger)

        mock_config = Mock(side_effect=ConfigObjError)
        with patch(u'yahoo_panoptes.framework.configuration_manager.ConfigObj', mock_config):
            with self.assertRaises(ConfigObjError):
                PanoptesConfig(logger=logger, conf_file=self.panoptes_test_conf_file)

        test_config = PanoptesConfig(logger=logger, conf_file=self.panoptes_test_conf_file)
        _SNMP_DEFAULTS = {u'retries': 1, u'timeout': 5, u'community': u'public', u'proxy_port': 10161,
                          u'community_string_key': u'snmp_community_string', u'non_repeaters': 0,
                          u'max_repetitions': 25, u'connection_factory_class':
                              u'PanoptesSNMPConnectionFactory', u'port': 10161,
                          u'connection_factory_module': u'yahoo_panoptes.plugins.helpers.snmp_connections'}
        self.assertEqual(test_config.snmp_defaults, _SNMP_DEFAULTS)
        self.assertSetEqual(test_config.sites, {u'local'})

        #  Test exception is raised when plugin_type is not specified in config file
        mock_plugin_types = [u'dummy']
        with patch(u'yahoo_panoptes.framework.configuration_manager.const.PLUGIN_TYPES', mock_plugin_types):
            with self.assertRaises(Exception):
                PanoptesConfig(logger=logger, conf_file=self.panoptes_test_conf_file)


class TestPanoptesRedisConnectionConfiguration(unittest.TestCase):
    def test_redis_sentinel_init(self):

        sentinel = [u'sentinel://:password@localhost:26379', u'sentinel://:password_1@localhost:26379']

        panoptes_redis_connection_config = \
            PanoptesRedisSentinelConnectionConfiguration(sentinels=sentinel,
                                                         master_name=u'master',
                                                         db=u'test_db')

        assert repr(panoptes_redis_connection_config) == u'sentinel://:**@localhost:26379,sentinel' \
                                                         u'://:**@localhost:26379'


class TestBaseSNMP(unittest.TestCase):
    def test_SNMPTypeMixin(self):
        test_snmp_type_mixin = base.SNMPTypeMixin(1.0)
        self.assertEqual(u'float', str(test_snmp_type_mixin))

        with self.assertRaises(ValueError):
            base.SNMPTypeMixin(int(1))

        test_snmp_integer = base.SNMPInteger(1)
        self.assertEqual(u'integer', str(test_snmp_integer.name))

        test_snmp_integer = base.SNMPInteger32((2 ** 31) - 1)

        with self.assertRaises(ValueError):
            base.SNMPInteger32(2 ** 31)

        test_snmp_integer = base.SNMPGauge32(1)
        self.assertEqual(test_snmp_integer.metric_type, PanoptesMetricType.GAUGE)

    def test_oid(self):
        juniperMIB = base.oid(u'.1.3.6.1.4.1.2636')
        self.assertEqual(u'.1.3.6.1.4.1.2636', juniperMIB.oid)
        self.assertEqual(repr(juniperMIB), u'oid(".1.3.6.1.4.1.2636")')

        jnxMibs = juniperMIB + base.oid(u'3')
        self.assertEqual(repr(jnxMibs), u'oid(".1.3.6.1.4.1.2636.3")')

        bad_oid = base.oid(u'3')
        delattr(bad_oid, u'snmp_type')
        jnxMibs = juniperMIB + bad_oid
        self.assertIsNone(jnxMibs.snmp_type)


class TestPanoptesCelery(unittest.TestCase):
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = _get_test_conf_file()

    def test_panoptes_celery_config(self):
        celery_config = PanoptesCeleryConfig(u'test')
        self.assertEqual(celery_config.celery_accept_content, [u'application/json', u'json'])
        self.assertEqual(celery_config.worker_prefetch_multiplier, 1)
        self.assertEqual(celery_config.task_acks_late, True)

        self.assertEqual(celery_config.app_name, u'test')

        PanoptesCeleryValidators.valid_celery_config(celery_config)

    def test_panoptes_celery_instance(self):
        celery_config = PanoptesCeleryConfig(u'test')
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file)

        celery_instance = PanoptesCeleryInstance(panoptes_context, celery_config)
        self.assertIsInstance(celery_instance.celery, Celery)

        self.assertEqual(celery_instance.celery.conf[u'broker_url'], u'redis://:password@localhost:6379/0')
        del panoptes_context
        del celery_instance

        # Test config file with redis sentinel
        redis_sentinel_config_file = os.path.join(self.my_dir, u'config_files/test_panoptes_config_redis_sentinel.ini')
        panoptes_context = PanoptesContext(config_file=redis_sentinel_config_file)
        celery_instance = PanoptesCeleryInstance(panoptes_context, celery_config)

        sentinels = panoptes_context.config_object.redis_urls_by_group[u'celery'][0].sentinels
        self.assertEqual(
            sentinels,
            [
                Sentinel(host=u'localhost', port=26379, password=u'password'),
                Sentinel(host=u'otherhost', port=26379, password=u'password')
            ]
        )
        self.assertEqual(celery_instance.celery.conf[u'broker_url'],
                         u'sentinel://:password@localhost:26379;sentinel://:password@otherhost:26379')
        del panoptes_context

        # hit exception and check logs
        mockCelery = Mock(side_effect=PanoptesCeleryError())
        with patch(u'yahoo_panoptes.framework.celery_manager.Celery',
                   mockCelery):
            with self.assertRaises(AttributeError):
                panoptes_context = PanoptesContext(self.panoptes_test_conf_file)
                celery_instance = PanoptesCeleryInstance(panoptes_context, celery_config)

    @patch(u'time.time', mock_time)
    @patch(u'yahoo_panoptes.framework.resources.time', mock_time)
    def test_panoptes_celery_plugin_scheduler(self):

        celery_config = PanoptesCeleryConfig(u'test')
        panoptes_context = PanoptesContext(self.panoptes_test_conf_file)

        celery_instance = PanoptesCeleryInstance(panoptes_context, celery_config)
        celery_plugin_scheduler = PanoptesCeleryPluginScheduler(app=celery_instance.celery)

        new_schedule = dict()
        new_schedule[u'celery.backend_cleanup'] = {
            u'task': u'celery.backend_cleanup',
            u'schedule': crontab(u'0', u'4', u'*'),
            u'options': {u'expires': 12 * 3600}}
        new_schedule[u'test_task'] = {
            u'task': const.POLLING_PLUGIN_AGENT_MODULE_NAME,
            u'schedule': timedelta(seconds=60),
            u'args': (u"test_plugin", u"test"),
            u'last_run_at': datetime.utcfromtimestamp(DUMMY_TIME - 61),
            u'options': {
                u'expires': 60,
                u'time_limit': 120
            }
        }
        new_schedule[u'test_task_2'] = {
            u'task': const.POLLING_PLUGIN_AGENT_MODULE_NAME,
            u'schedule': timedelta(seconds=60),
            u'args': (u"test_plugin", u"test_2"),
            u'last_run_at': datetime.utcfromtimestamp(DUMMY_TIME - 1),
            u'options': {
                u'expires': 60,
                u'time_limit': 120
            }
        }

        celery_plugin_scheduler.update(celery_plugin_scheduler.logger, new_schedule)
        self.assertEqual(len(celery_plugin_scheduler.schedule), len(new_schedule))

        mock_producer = Mock()

        with patch(u'yahoo_panoptes.framework.celery_manager.PanoptesCeleryPluginScheduler.apply_entry',
                   return_value=None):
            with patch(u'yahoo_panoptes.framework.celery_manager.PanoptesCeleryPluginScheduler.producer',
                       mock_producer):
                self.assertIsNone(celery_plugin_scheduler._heap)
                self.assertEqual(celery_plugin_scheduler.tick(), 0)
                self.assertEqual(celery_plugin_scheduler.tick(), 0)
                assert celery_plugin_scheduler.tick() > 0


if __name__ == '__main__':
    unittest.main()
