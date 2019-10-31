"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
import time
import unittest
import json
import os

from mock import *

from yahoo_panoptes.framework.enrichment import PanoptesEnrichmentSet, PanoptesEnrichmentGroup, \
    PanoptesEnrichmentGroupSet, PanoptesEnrichmentSchemaValidator, PanoptesEnrichmentEncoder, \
    PanoptesEnrichmentMultiGroupSet
from yahoo_panoptes.framework.resources import PanoptesResource, PanoptesResourcesKeyValueStore
from yahoo_panoptes.enrichment.enrichment_plugin_agent import _store_enrichment_data, \
    PanoptesEnrichmentCacheKeyValueStore, enrichment_plugin_task, PanoptesEnrichmentTaskContext
from tests.test_framework import PanoptesMockRedis
from yahoo_panoptes.framework.context import PanoptesContext


mock_time = Mock()
mock_time.return_value = 1512629517.03121


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


class PanoptesEnrichmentInterfaceSchemaValidator(PanoptesEnrichmentSchemaValidator):
    schema = {
        'enrichment_label': {
            'type': 'dict',
            'schema': {
                         'speed': {'type': 'integer'},
                         'index': {'type': 'integer'},
                         'status': {'type': 'string'}
                      }
        }
    }

    def __init__(self):
        super(PanoptesEnrichmentInterfaceSchemaValidator, self).__init__()


class PanoptesEnrichmentNeighborSchemaValidator(PanoptesEnrichmentSchemaValidator):
    schema = {
        'enrichment_label': {
            'type': 'dict',
            'schema': {
                'vlan_id': {'type': 'integer', 'required': True},
                'property': {'type': 'string', 'required': True},
                'mac': {'type': 'string'}
            }
        }
    }

    def __init__(self):
        super(PanoptesEnrichmentNeighborSchemaValidator, self).__init__()


class TestEnrichmentFramework(unittest.TestCase):
    @patch(u'yahoo_panoptes.framework.resources.time', mock_time)
    def setUp(self):
        self.__panoptes_resource = PanoptesResource(resource_site=u'test', resource_class=u'test',
                                                    resource_subclass=u'test',
                                                    resource_type=u'test', resource_id=u'test',
                                                    resource_endpoint=u'test',
                                                    resource_plugin=u'test')
        self.__panoptes_resource.add_metadata(u'test', u'test')

    def test_enrichment_set(self):
        enrichment_set = PanoptesEnrichmentSet(u'int_001')
        enrichment_set.add(u'speed', 1000)
        enrichment_set.add(u'index', 0o01)
        enrichment_set.add(u'status', u'up')
        self.assertEquals(enrichment_set.key, u'int_001')
        self.assertDictEqual(enrichment_set.value, {u'status': u'up', u'index': 1, u'speed': 1000})
        self.assertEquals(len(enrichment_set), 3)

        enrichment_set1 = PanoptesEnrichmentSet(u'int_002', {u'status': u'down', u'index': 2, u'speed': 1000})
        self.assertEquals(enrichment_set1.key, u'int_002')
        self.assertDictEqual(enrichment_set1.value, {u'status': u'down', u'index': 2, u'speed': 1000})

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentSet(u'int_001', u'string')

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentSet(u'int_001', 100)

    def test_enrichment_schema_validator(self):
        validator = PanoptesEnrichmentInterfaceSchemaValidator()
        enrichment_set = PanoptesEnrichmentSet(u'int_001')
        enrichment_set.add(u'speed', 1000)
        enrichment_set.add(u'index', 0o01)
        enrichment_set.add(u'status', u'up')
        self.assertTrue(validator.validate(enrichment_set))

        enrichment_set.add(u'status', 0o1)
        self.assertFalse(validator.validate(enrichment_set))

    @patch(u'time.time', mock_time)
    def test_enrichment_group(self):
        interface_validation_object = PanoptesEnrichmentInterfaceSchemaValidator()
        neighbor_validation_object = PanoptesEnrichmentNeighborSchemaValidator()

        interface_data = \
            u'''{"data": [
            {"int_001": {"index": 1, "speed": 1000, "status": "up"}},
            {"int_002": {"index": 2, "speed": 1000, "status": "down"}}],
            "metadata": {"_enrichment_group_creation_timestamp": %f, "_enrichment_ttl": 300, "_execute_frequency": 60},
            "namespace": "interface"}''' % mock_time.return_value

        neighbor_data = \
            u'''{"data": [{"host_name": {"mac": "aa:bb:cc:dd:ee:ff", "property": "Test Property", "vlan_id": 501}}],
            "metadata": {"_enrichment_group_creation_timestamp": %f, "_enrichment_ttl": 600, "_execute_frequency": 120},
            "namespace": "neighbor"}''' % mock_time.return_value

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentGroup(1, interface_validation_object, 300, 60)

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentGroup(u'interface', u'non_validation_object', 300, 60)

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentGroup(u'interface', interface_validation_object, u'300', 60)

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentGroup(u'interface', interface_validation_object, 300, u'60')

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentGroup(u'interface', interface_validation_object, 0, 60)

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentGroup(u'interface', interface_validation_object, 300, 0)

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentGroup(u'interface', interface_validation_object, 300, 60).\
                add_enrichment_set(u'not_PanoptesEnrichmentSet_obj')

        enrichment_set1 = PanoptesEnrichmentSet(u'int_001')
        enrichment_set1.add(u'speed', 1000)
        enrichment_set1.add(u'index', 0o01)
        enrichment_set1.add(u'status', u'up')

        enrichment_set2 = PanoptesEnrichmentSet(u'int_002')
        enrichment_set2.add(u'speed', 1000)
        enrichment_set2.add(u'index', 0o02)
        enrichment_set2.add(u'status', u'down')

        enrichment_group1 = PanoptesEnrichmentGroup(u'interface', interface_validation_object, 300, 60)
        enrichment_group1.add_enrichment_set(enrichment_set1)
        enrichment_group1.add_enrichment_set(enrichment_set2)

        self.assertEqual(enrichment_group1.namespace, u'interface')
        self.assertEqual(enrichment_group1.enrichment_ttl, 300)
        self.assertEqual(enrichment_group1.execute_frequency, 60)
        self.assertEqual(enrichment_group1.enrichment_group_creation_timestamp, mock_time.return_value)
        self.assertEqual(ordered(json.loads(json.dumps(enrichment_group1.data, cls=PanoptesEnrichmentEncoder))),
                         ordered(json.loads(interface_data)[u'data']))
        self.assertEqual(ordered(json.loads(enrichment_group1.json())), ordered(json.loads(interface_data)))
        self.assertEquals(len(enrichment_group1), 2)

        enrichment_set3 = PanoptesEnrichmentSet(u'int_002')
        enrichment_set3.add(u'speed', 1000)
        enrichment_set3.add(u'index', 0o02)
        enrichment_set3.add(u'status', u'down')

        self.assertEqual(ordered(json.loads(enrichment_group1.json())), ordered(json.loads(interface_data)))
        self.assertEqual(ordered(enrichment_group1.metadata), ordered(json.loads(interface_data)[u'metadata']))
        self.assertEquals(len(enrichment_group1), 2)

        test_metadata = json.loads(interface_data)[u'metadata']
        test_metadata[u'metadata_key'] = u'metadata_value'

        enrichment_group1.upsert_metadata(u'metadata_key', u'metadata_value')
        self.assertEqual(ordered(enrichment_group1.metadata), ordered(test_metadata))
        enrichment_group1.upsert_metadata(u'ttl', 300)
        with self.assertRaises(ValueError):
            enrichment_group1.upsert_metadata(u'_enrichment_ttl', 300)
        with self.assertRaises(AssertionError):
            enrichment_group1.upsert_metadata(u'metadata', {})
        with self.assertRaises(AssertionError):
            enrichment_group1.upsert_metadata(u'metadata', [])

        enrichment_set4 = PanoptesEnrichmentSet(u'host_name')
        enrichment_set4.add(u'vlan_id', 501)
        enrichment_set4.add(u'property', u'Test Property')
        enrichment_set4.add(u'mac', u'aa:bb:cc:dd:ee:ff')

        enrichment_group2 = PanoptesEnrichmentGroup(u'neighbor', neighbor_validation_object, 600, 120)
        enrichment_group2.add_enrichment_set(enrichment_set4)

        self.assertEqual(ordered(json.loads(enrichment_group2.json())), ordered(json.loads(neighbor_data)))
        self.assertEquals(len(enrichment_group2), 1)

        enrichment_set5 = PanoptesEnrichmentSet(u'host_name01')
        enrichment_set5.add(u'vlan_id', 502)
        enrichment_set5.add(u'property', u'Netops01.US')

        enrichment_set6 = PanoptesEnrichmentSet(u'host_name02')
        enrichment_set6.add(u'vlan_id', 503)
        enrichment_set6.add(u'mac', u'aa:bb:cc:dd:ee:ff')

        enrichment_group3 = PanoptesEnrichmentGroup(u'neighbor', neighbor_validation_object, 600, 120)
        enrichment_group3.add_enrichment_set(enrichment_set5)
        with self.assertRaises(AssertionError):
            enrichment_group3.add_enrichment_set(enrichment_set6)

        interface_store_data = u'{"int_001": {"index": 1, "speed": 1000, "status": "up"}, ' \
                               u'"int_002": {"index": 2, "speed": 1000, "status": "down"}}'

        neighbor_store_data = u'{"host_name": {"mac": "aa:bb:cc:dd:ee:ff", ' \
                              u'"property": "Test Property", "vlan_id": 501}}'

        self.assertEquals(ordered(json.loads(enrichment_group1.serialize_data())),
                          ordered(json.loads(interface_store_data)))
        self.assertEquals(ordered(json.loads(enrichment_group2.serialize_data())),
                          ordered(json.loads(neighbor_store_data)))

        enrichment_group1.upsert_metadata(u'ttl', 300)
        with self.assertRaises(ValueError):
            enrichment_group1.upsert_metadata(u'_enrichment_ttl', 300)

        interface_data_serialized = u'''{{"data": {{"int_001": {{"index": 1, "speed": 1000, "status": "up"}},
        "int_002": {{"index": 2, "speed": 1000, "status": "down"}}}}, "metadata":
        {{"_enrichment_group_creation_timestamp": {:.5f}, "_enrichment_ttl": 300, "_execute_frequency": 60,
        "metadata_key": "metadata_value", "ttl": 300}}}}'''.format(mock_time.return_value)

        neighbor_data_serialized = u'''{{"data": {{"host_name": {{"mac": "aa:bb:cc:dd:ee:ff", "property":
        "Test Property","vlan_id": 501}}}}, "metadata": {{"_enrichment_group_creation_timestamp": {:.5f},
        "_enrichment_ttl": 600, "_execute_frequency": 120}}}}'''.format(mock_time.return_value)

        self.assertEquals(ordered(json.loads(enrichment_group1.serialize())),
                          ordered(json.loads(interface_data_serialized)))

        self.assertEquals(ordered(json.loads(enrichment_group2.serialize())),
                          ordered(json.loads(neighbor_data_serialized)))

    @patch(u'time.time', mock_time)
    def test_enrichment_group_set(self):
        interface_validation_object = PanoptesEnrichmentInterfaceSchemaValidator()
        neighbor_validation_object = PanoptesEnrichmentNeighborSchemaValidator()

        panoptes_resource = self.__panoptes_resource

        enrichment_data = \
            u'''{{"enrichment": [{{"metadata": {{"_enrichment_group_creation_timestamp": {:.5f}, "_enrichment_ttl": 600,
            "_execute_frequency": 120}}, "data": [{{"host_name":
            {{"mac": "aa:bb:cc:dd:ee:ff", "property": "Test Property", "vlan_id": 501}}}}],
            "namespace": "neighbor"}}, {{"metadata": {{"_enrichment_group_creation_timestamp": {:.5f},
            "_enrichment_ttl": 300,
            "_execute_frequency": 60}}, "data": [
            {{"int_001": {{"index": 1, "speed": 1000, "status": "up"}}}}, {{"int_002": {{"index": 2, "speed": 1000,
            "status": "down"}}}}], "namespace": "interface"}}],
            "enrichment_group_set_creation_timestamp": {:.5f}, "resource": {{"resource_class": "test",
            "resource_creation_timestamp": {:.5f}, "resource_endpoint": "test", "resource_id": "test",
            "resource_metadata": {{"_resource_ttl": "604800", "test": "test"}}, "resource_plugin": "test",
            "resource_site": "test",
            "resource_subclass": "test", "resource_type": "test"}}}}'''.format(mock_time.return_value,
                                                                               mock_time.return_value,
                                                                               mock_time.return_value,
                                                                               mock_time.return_value)

        enrichment_set1 = PanoptesEnrichmentSet(u'int_001')
        enrichment_set1.add(u'speed', 1000)
        enrichment_set1.add(u'index', 0o01)
        enrichment_set1.add(u'status', u'up')

        enrichment_set2 = PanoptesEnrichmentSet(u'int_002')
        enrichment_set2.add(u'speed', 1000)
        enrichment_set2.add(u'index', 0o02)
        enrichment_set2.add(u'status', u'down')

        enrichment_group1 = PanoptesEnrichmentGroup(u'interface', interface_validation_object, 300, 60)
        enrichment_group1.add_enrichment_set(enrichment_set1)
        enrichment_group1.add_enrichment_set(enrichment_set2)

        enrichment_set3 = PanoptesEnrichmentSet(u'host_name')
        enrichment_set3.add(u'vlan_id', 501)
        enrichment_set3.add(u'property', u'Test Property')
        enrichment_set3.add(u'mac', u'aa:bb:cc:dd:ee:ff')

        enrichment_group2 = PanoptesEnrichmentGroup(u'neighbor', neighbor_validation_object, 600, 120)
        enrichment_group2.add_enrichment_set(enrichment_set3)

        enrichment_group_set1 = PanoptesEnrichmentGroupSet(panoptes_resource)
        enrichment_group_set1.add_enrichment_group(enrichment_group1)
        enrichment_group_set1.add_enrichment_group(enrichment_group2)
        self.assertEquals(len(enrichment_group_set1), 2)

        group_set_repr = u"PanoptesEnrichmentGroupSet[resource:" \
                         u"plugin|test|site|test|class|test|subclass|test|type|test|id|test|endpoint|test," \
                         u"enrichment_group_set_creation_timestamp:{},PanoptesEnrichmentGroup[namespace:" \
                         u"interface,enrichment_ttl:300,execute_frequency:60,enrichment_group_creation_timestamp:{}," \
                         u"PanoptesEnrichmentSet[int_001[index:1,speed:1000,status:up]],PanoptesEnrichmentSet[" \
                         u"int_002[index:2,speed:1000,status:down]]],PanoptesEnrichmentGroup[namespace:neighbor," \
                         u"enrichment_ttl:600,execute_frequency:120,enrichment_group_creation_timestamp:{}," \
                         u"PanoptesEnrichmentSet[host_name[mac:aa:bb:cc:dd:ee:ff,property:" \
                         u"Test Property,vlan_id:501]]]]".format(mock_time.return_value,
                                                                 mock_time.return_value,
                                                                 mock_time.return_value)

        self.assertEquals(repr(enrichment_group_set1), group_set_repr)

        self.assertIsInstance(enrichment_group_set1.resource, PanoptesResource)
        self.assertEqual(enrichment_group_set1.enrichment_group_set_creation_timestamp, mock_time.return_value)

        self.assertEqual(
            ordered(json.loads(json.dumps(enrichment_group_set1.enrichment, cls=PanoptesEnrichmentEncoder))),
            ordered(json.loads(enrichment_data)[u'enrichment']))

        self.assertEqual(ordered(json.loads(enrichment_group_set1.json())[u'enrichment']),
                         ordered(json.loads(enrichment_data)[u'enrichment']))

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentGroupSet(u'bad_resource')

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentGroupSet(panoptes_resource).add_enrichment_group(u'non_PanoptesEnrichmentGroup_obj')

        enrichment_group_set2 = PanoptesEnrichmentGroupSet(panoptes_resource)
        enrichment_group3 = PanoptesEnrichmentGroup(u'interface', interface_validation_object, 300, 60)

        with self.assertRaises(AssertionError):
            enrichment_group_set2.add_enrichment_group(enrichment_group3)

        self.assertFalse(enrichment_group_set1 == enrichment_group1)
        self.assertFalse(enrichment_group_set1 == enrichment_group_set2)

    @patch(u'time.time', mock_time)
    @patch(u'yahoo_panoptes.framework.resources.time', mock_time)
    def test_multi_enrichment_group_set(self):
        interface_validation_object = PanoptesEnrichmentInterfaceSchemaValidator()
        neighbor_validation_object = PanoptesEnrichmentNeighborSchemaValidator()

        panoptes_resource = self.__panoptes_resource

        multi_enrichment_results_data = \
            {
                "group_sets": [
                    {
                        "enrichment": [
                            {
                                "data": [
                                    {
                                        "host_name": {
                                            "mac": "aa:bb:cc:dd:ee:ff",
                                            "property": "Test Property",
                                            "vlan_id": 501
                                        }
                                    },
                                    {
                                        "host_name01": {
                                            "mac": "aa:bb:cc:dd:ee:ff",
                                            "property": "Test Property",
                                            "vlan_id": 502
                                        }
                                    }
                                ],
                                "metadata": {
                                    "_enrichment_group_creation_timestamp": mock_time.return_value,
                                    "_enrichment_ttl": 600,
                                    "_execute_frequency": 120
                                },
                                "namespace": "neighbor"
                            },
                            {
                                "data": [
                                    {
                                        "int_001": {
                                            "index": 1,
                                            "speed": 1000,
                                            "status": "up"
                                        }
                                    }
                                ],
                                "metadata": {
                                    "_enrichment_group_creation_timestamp": mock_time.return_value,
                                    "_enrichment_ttl": 300,
                                    "_execute_frequency": 60
                                },
                                "namespace": "interface"
                            }
                        ],
                        "enrichment_group_set_creation_timestamp": mock_time.return_value,
                        "resource": {
                            "resource_class": "test_class",
                            "resource_creation_timestamp": mock_time.return_value,
                            "resource_endpoint": "test_endpoint01",
                            "resource_id": "test_resource_id01",
                            "resource_metadata": {
                                "_resource_ttl": "604800"
                            },
                            "resource_plugin": "test_plugin",
                            "resource_site": "test_site",
                            "resource_subclass": "test_subclass",
                            "resource_type": "test_type"
                        }
                    },
                    {
                        "enrichment": [
                            {
                                "data": [
                                    {
                                        "int_001": {
                                            "index": 1,
                                            "speed": 1000,
                                            "status": "up"
                                        }
                                    }
                                ],
                                "metadata": {
                                    "_enrichment_group_creation_timestamp": mock_time.return_value,
                                    "_enrichment_ttl": 300,
                                    "_execute_frequency": 60
                                },
                                "namespace": "interface"
                            },
                            {
                                "data": [
                                    {
                                        "host_name": {
                                            "mac": "aa:bb:cc:dd:ee:ff",
                                            "property": "Test Property",
                                            "vlan_id": 501
                                        }
                                    }
                                ],
                                "metadata": {
                                    "_enrichment_group_creation_timestamp": mock_time.return_value,
                                    "_enrichment_ttl": 600,
                                    "_execute_frequency": 120
                                },
                                "namespace": "neighbor"
                            }
                        ],
                        "enrichment_group_set_creation_timestamp": mock_time.return_value,
                        "resource": {
                            "resource_class": "test",
                            "resource_creation_timestamp": mock_time.return_value,
                            "resource_endpoint": "test",
                            "resource_id": "test",
                            "resource_metadata": {
                                "_resource_ttl": "604800",
                                "test": "test"
                            },
                            "resource_plugin": "test",
                            "resource_site": "test",
                            "resource_subclass": "test",
                            "resource_type": "test"
                        }
                    }
                ]
            }

        enrichment_set1 = PanoptesEnrichmentSet(u'int_001')
        enrichment_set1.add(u'speed', 1000)
        enrichment_set1.add(u'index', 0o01)
        enrichment_set1.add(u'status', u'up')

        enrichment_group1 = PanoptesEnrichmentGroup(u'interface', interface_validation_object, 300, 60)
        enrichment_group1.add_enrichment_set(enrichment_set1)

        enrichment_set3 = PanoptesEnrichmentSet(u'host_name')
        enrichment_set3.add(u'vlan_id', 501)
        enrichment_set3.add(u'property', u'Test Property')
        enrichment_set3.add(u'mac', u'aa:bb:cc:dd:ee:ff')

        enrichment_group2 = PanoptesEnrichmentGroup(u'neighbor', neighbor_validation_object, 600, 120)
        enrichment_group2.add_enrichment_set(enrichment_set3)

        enrichment_group_set1 = PanoptesEnrichmentGroupSet(panoptes_resource)
        enrichment_group_set1.add_enrichment_group(enrichment_group1)
        enrichment_group_set1.add_enrichment_group(enrichment_group2)

        panoptes_resource01 = PanoptesResource(resource_site=u'test_site',
                                               resource_class=u'test_class',
                                               resource_subclass=u'test_subclass',
                                               resource_type=u'test_type',
                                               resource_id=u'test_resource_id01',
                                               resource_endpoint=u'test_endpoint01',
                                               resource_plugin=u'test_plugin')

        panoptes_resource02 = PanoptesResource(resource_site=u'test_site',
                                               resource_class=u'test_class',
                                               resource_subclass=u'test_subclass',
                                               resource_type=u'test_type',
                                               resource_id=u'test_resource_id02',
                                               resource_endpoint=u'test_endpoint02',
                                               resource_plugin=u'test_plugin')

        enrichment_set4 = PanoptesEnrichmentSet(u'host_name01')
        enrichment_set4.add(u'vlan_id', 502)
        enrichment_set4.add(u'property', u'Test Property')
        enrichment_set4.add(u'mac', u'aa:bb:cc:dd:ee:ff')

        enrichment_group3 = PanoptesEnrichmentGroup(u'neighbor', neighbor_validation_object, 600, 120)
        enrichment_group3.add_enrichment_set(enrichment_set3)
        enrichment_group3.add_enrichment_set(enrichment_set4)

        enrichment_group_set2 = PanoptesEnrichmentGroupSet(panoptes_resource01)
        enrichment_group_set2.add_enrichment_group(enrichment_group1)
        enrichment_group_set2.add_enrichment_group(enrichment_group3)

        multi_enrichment_group_set = PanoptesEnrichmentMultiGroupSet()
        multi_enrichment_group_set.add_enrichment_group_set(enrichment_group_set1)
        multi_enrichment_group_set.add_enrichment_group_set(enrichment_group_set2)

        multi_enrichment_group_set_repr = u"PanoptesEnrichmentMultiGroupSet[PanoptesEnrichmentGroupSet[resource:" \
                                          u"plugin|test|site|test|class|test|subclass|test|type|test|id|test|endpoint" \
                                          u"|test,enrichment_group_set_creation_timestamp:{},PanoptesEnrichmentGroup" \
                                          u"[namespace:interface,enrichment_ttl:300,execute_frequency:60," \
                                          u"enrichment_group_creation_timestamp:{},PanoptesEnrichmentSet[" \
                                          u"int_001[index:1,speed:1000,status:up]]],PanoptesEnrichmentGroup[" \
                                          u"namespace:neighbor,enrichment_ttl:600,execute_frequency:120," \
                                          u"enrichment_group_creation_timestamp:{},PanoptesEnrichmentSet" \
                                          u"[host_name[mac:aa:bb:cc:dd:ee:ff,property:Test Property,vlan_id:501]" \
                                          u"]]],PanoptesEnrichmentGroupSet[resource:plugin|test_plugin|site|" \
                                          u"test_site|class|test_class|subclass|test_subclass|type|test_type|id" \
                                          u"|test_resource_id01|endpoint|test_endpoint01," \
                                          u"enrichment_group_set_creation_timestamp:{},PanoptesEnrichmentGroup" \
                                          u"[namespace:interface,enrichment_ttl:300,execute_frequency:60," \
                                          u"enrichment_group_creation_timestamp:{},PanoptesEnrichmentSet" \
                                          u"[int_001[index:1,speed:1000,status:up]]],PanoptesEnrichmentGroup" \
                                          u"[namespace:neighbor,enrichment_ttl:600,execute_frequency:120," \
                                          u"enrichment_group_creation_timestamp:{},PanoptesEnrichmentSet" \
                                          u"[host_name[mac:aa:bb:cc:dd:ee:ff,property:Test Property,vlan_id:501]]," \
                                          u"PanoptesEnrichmentSet[host_name01[mac:aa:bb:cc:dd:ee:ff," \
                                          u"property:Test Property,vlan_id:502]]]]]".format(mock_time.return_value,
                                                                                            mock_time.return_value,
                                                                                            mock_time.return_value,
                                                                                            mock_time.return_value,
                                                                                            mock_time.return_value,
                                                                                            mock_time.return_value)

        self.assertEquals(repr(multi_enrichment_group_set), multi_enrichment_group_set_repr)

        self.assertEquals(len(multi_enrichment_group_set.enrichment_group_sets), 2)

        self.assertEquals(ordered(json.loads(multi_enrichment_group_set.json())),
                          ordered(multi_enrichment_results_data))

        self.assertEquals(len(multi_enrichment_group_set), 2)

        multi_enrichment_group_set.add_enrichment_group_set(enrichment_group_set2)
        self.assertEquals(len(multi_enrichment_group_set), 2)

        enrichment_group_set3 = PanoptesEnrichmentGroupSet(panoptes_resource02)
        enrichment_group_set3.add_enrichment_group(enrichment_group1)
        multi_enrichment_group_set.add_enrichment_group_set(enrichment_group_set3)
        self.assertEquals(len(multi_enrichment_group_set), 3)

        with self.assertRaises(AssertionError):
            multi_enrichment_group_set.add_enrichment_group_set(u'non_enrichment_group')

        enrichment_group_set3 = PanoptesEnrichmentGroupSet(panoptes_resource01)
        with self.assertRaises(AssertionError):
            multi_enrichment_group_set.add_enrichment_group_set(enrichment_group_set3)


class TestPanoptesEnrichmentCacheStore(unittest.TestCase):
    @patch(u'redis.StrictRedis', PanoptesMockRedis)
    @patch(u'time.time', mock_time)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = _get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                                 key_value_store_class_list=[PanoptesEnrichmentCacheKeyValueStore,
                                                                             PanoptesResourcesKeyValueStore])

        self._enrichment_kv = self._panoptes_context.get_kv_store(PanoptesEnrichmentCacheKeyValueStore)

        self._panoptes_resource = PanoptesResource(resource_site=u'test_site',
                                                   resource_class=u'test_class',
                                                   resource_subclass=u'test_subclass',
                                                   resource_type=u'test_type',
                                                   resource_id=u'test_resource_id',
                                                   resource_endpoint=u'test_endpoint',
                                                   resource_plugin=u'test_plugin')

        interface_validation_object = PanoptesEnrichmentInterfaceSchemaValidator()
        neighbor_validation_object = PanoptesEnrichmentNeighborSchemaValidator()

        enrichment_set1 = PanoptesEnrichmentSet(u'int_001')
        enrichment_set1.add(u'speed', 1000)
        enrichment_set1.add(u'index', 0o01)
        enrichment_set1.add(u'status', u'up')

        enrichment_set2 = PanoptesEnrichmentSet(u'int_002')
        enrichment_set2.add(u'speed', 1000)
        enrichment_set2.add(u'index', 0o02)
        enrichment_set2.add(u'status', u'down')

        enrichment_group1 = PanoptesEnrichmentGroup(u'interface', interface_validation_object, 300, 60)
        enrichment_group1.add_enrichment_set(enrichment_set1)
        enrichment_group1.add_enrichment_set(enrichment_set2)

        enrichment_set3 = PanoptesEnrichmentSet(u'host_name')
        enrichment_set3.add(u'vlan_id', 501)
        enrichment_set3.add(u'property', u'Test Property')
        enrichment_set3.add(u'mac', u'aa:bb:cc:dd:ee:ff')

        enrichment_group2 = PanoptesEnrichmentGroup(u'neighbor', neighbor_validation_object, 600, 120)
        enrichment_group2.add_enrichment_set(enrichment_set3)

        self.enrichment_group_set1 = PanoptesEnrichmentGroupSet(self._panoptes_resource)
        self.enrichment_group_set1.add_enrichment_group(enrichment_group1)
        self.enrichment_group_set1.add_enrichment_group(enrichment_group2)

        self._panoptes_resource01 = PanoptesResource(resource_site=u'test_site',
                                                     resource_class=u'test_class',
                                                     resource_subclass=u'test_subclass',
                                                     resource_type=u'test_type',
                                                     resource_id=u'test_resource_id01',
                                                     resource_endpoint=u'test_endpoint01',
                                                     resource_plugin=u'test_plugin')

        enrichment_set4 = PanoptesEnrichmentSet(u'host_name01')
        enrichment_set4.add(u'vlan_id', 502)
        enrichment_set4.add(u'property', u'Test Property')
        enrichment_set4.add(u'mac', u'aa:bb:cc:dd:ee:ff')

        enrichment_group3 = PanoptesEnrichmentGroup(u'neighbor', neighbor_validation_object, 600, 120)
        enrichment_group3.add_enrichment_set(enrichment_set3)
        enrichment_group3.add_enrichment_set(enrichment_set4)

        self.enrichment_group_set2 = PanoptesEnrichmentGroupSet(self._panoptes_resource01)
        self.enrichment_group_set2.add_enrichment_group(enrichment_group1)
        self.enrichment_group_set2.add_enrichment_group(enrichment_group3)

        self._multi_enrichment_group_set = PanoptesEnrichmentMultiGroupSet()
        self._multi_enrichment_group_set.add_enrichment_group_set(self.enrichment_group_set1)
        self._multi_enrichment_group_set.add_enrichment_group_set(self.enrichment_group_set2)

    @patch(u'time.time', mock_time)
    def test_panoptes_enrichment_set(self):
        enrichment_set1 = PanoptesEnrichmentSet(u'int_001')
        enrichment_set1.add(u'speed', 1000)
        enrichment_set1.add(u'index', 0o01)
        enrichment_set1.add(u'status', u'up')
        self.assertEquals(enrichment_set1.json(),
                          u'{"int_001": {"index": 1, "speed": 1000, "status": "up"}}')
        self.assertEquals(repr(enrichment_set1),
                          u"PanoptesEnrichmentSet[int_001[index:1,speed:1000,status:up]]")

        enrichment_set2 = PanoptesEnrichmentSet(u'int_002')
        enrichment_set2.add(u'speed', 1000)
        enrichment_set2.add(u'index', 0o02)
        enrichment_set2.add(u'status', u'down')

        interface_validation_object = PanoptesEnrichmentInterfaceSchemaValidator()
        enrichment_group1 = PanoptesEnrichmentGroup(u'interface', interface_validation_object, 300, 60)

        self.assertFalse(enrichment_set1 == enrichment_group1)
        self.assertFalse(enrichment_set1 == enrichment_set2)

    @patch(u'time.time', mock_time)
    def test_panoptes_enrichment_group(self):
        interface_validation_object = PanoptesEnrichmentInterfaceSchemaValidator()
        enrichment_group1 = PanoptesEnrichmentGroup(u'interface', interface_validation_object, 300, 60)
        self.assertEquals(enrichment_group1.enrichment_schema, PanoptesEnrichmentInterfaceSchemaValidator.schema)

        self.assertEquals(repr(enrichment_group1), u"PanoptesEnrichmentGroup[namespace:interface,"
                                                   u"enrichment_ttl:300,execute_frequency:60,"
                                                   u"enrichment_group_creation_timestamp:{}]".format(
                                                    mock_time.return_value))

        enrichment_set1 = PanoptesEnrichmentSet(u'int_001')
        enrichment_set1.add(u'speed', 1000)
        enrichment_set1.add(u'index', 0o01)
        enrichment_set1.add(u'status', u'up')

        self.assertFalse(enrichment_group1 == enrichment_set1)

        enrichment_group2 = PanoptesEnrichmentGroup(u'interface', interface_validation_object, 300, 60)
        enrichment_group3 = PanoptesEnrichmentGroup(u'other_namespace', interface_validation_object, 300, 60)
        self.assertTrue(enrichment_group1 == enrichment_group2)
        self.assertFalse(enrichment_group1 == enrichment_group3)

    @patch(u'time.time', mock_time)
    def test_store_enrichment_data_enrichment_group_set(self):
        interface_result_data = \
            {
                "data": {
                    "int_001": {
                        "index": 1,
                        "speed": 1000,
                        "status": "up"
                    },
                    "int_002": {
                        "index": 2,
                        "speed": 1000,
                        "status": "down"
                    }
                },
                "metadata": {
                    "_enrichment_group_creation_timestamp": mock_time.return_value,
                    "_enrichment_ttl": 300,
                    "_execute_frequency": 60
                }
            }

        neighbor_result_data = \
            {
                "data": {
                    "host_name": {
                        "mac": "aa:bb:cc:dd:ee:ff",
                        "property": "Test Property",
                        "vlan_id": 501
                    }
                },
                "metadata": {
                    "_enrichment_group_creation_timestamp": mock_time.return_value,
                    "_enrichment_ttl": 600,
                    "_execute_frequency": 120
                }
            }

        _store_enrichment_data(self._panoptes_context, self.enrichment_group_set1, u'PanoptesPluginInfo')

        self.assertNotEquals(ordered(interface_result_data),
                             ordered(json.loads(self._enrichment_kv.get(u'test_resource_id:neighbor'))))

        self.assertEquals(ordered(interface_result_data),
                          ordered(json.loads(self._enrichment_kv.get(u'test_resource_id:interface'))))

        self.assertEquals(ordered(neighbor_result_data),
                          ordered(json.loads(self._enrichment_kv.get(u'test_resource_id:neighbor'))))

    def test_store_enrichment_data_enrichment_multi_group_set(self):

        enrichment_result_keys = [u'test_resource_id01:interface', u'test_resource_id01:neighbor',
                                  u'test_resource_id:interface', u'test_resource_id:neighbor']

        neighbor_result_data = \
            {u"data": {u"host_name": {u"mac": u"aa:bb:cc:dd:ee:ff", u"property": u"Test Property", u"vlan_id": 501},
                       u"host_name01": {u"mac": u"aa:bb:cc:dd:ee:ff", u"property": u"Test Property", u"vlan_id": 502}},
             u"metadata": {u"_enrichment_group_creation_timestamp": mock_time.return_value, u"_enrichment_ttl": 600,
                           u"_execute_frequency": 120}}

        _store_enrichment_data(self._panoptes_context, self._multi_enrichment_group_set, u'PanoptesPluginInfo')

        self.assertEquals(enrichment_result_keys, self._enrichment_kv.find_keys(u'*'))

        self.assertEquals(ordered(neighbor_result_data),
                          ordered(json.loads(self._enrichment_kv.get(u'test_resource_id01:neighbor'))))

        self.assertNotEquals(ordered(neighbor_result_data),
                             ordered(json.loads(self._enrichment_kv.get(u'test_resource_id01:interface'))))

    @patch(u'yahoo_panoptes.enrichment.enrichment_plugin_agent.PanoptesPluginWithEnrichmentRunner',
           create_auto_spec=True)
    @patch(u'yahoo_panoptes.framework.resources.PanoptesResourceStore.get_resource')
    @patch(u'yahoo_panoptes.enrichment.enrichment_plugin_agent.PanoptesEnrichmentTaskContext')
    def test_enrichment_plugin_task_is_executed(self, task_context, resource, enrichment_runner):

        task_context.return_value = self._panoptes_context
        resource.return_value = self._panoptes_resource

        # Test Exception is Thrown on failure to create PanoptesEnrichmentTaskContext
        task_context.side_effect = Exception()
        with self.assertRaises(SystemExit):
            enrichment_plugin_task(u'name', u'key')
        task_context.side_effect = None

        # Test Exception is Thrown on failure to create / run plugin
        enrichment_runner.side_effect = Exception()
        enrichment_plugin_task(u'name', u'key')
        enrichment_runner.execute_plugin.assert_not_called()
        enrichment_runner.side_effect = None

        # Test Enrichment Is Executed
        enrichment_plugin_task(u'name', u'key')
        enrichment_runner.assert_called()
        enrichment_runner().execute_plugin.assert_called_once()
