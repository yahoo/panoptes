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
from yahoo_panoptes.framework.resources import PanoptesResource
from yahoo_panoptes.enrichment.enrichment_plugin_agent import _store_enrichment_data, \
    PanoptesEnrichmentCacheKeyValueStore
from tests.test_framework import PanoptesMockRedis
from yahoo_panoptes.framework.context import PanoptesContext


mock_time = Mock()
mock_time.return_value = 1512629517.03121


def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj


def _get_test_conf_file():
    my_dir = os.path.dirname(os.path.realpath(__file__))
    panoptes_test_conf_file = os.path.join(my_dir, 'config_files/test_panoptes_config.ini')

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
    @patch('yahoo_panoptes.framework.resources.time', mock_time)
    def setUp(self):
        self.__panoptes_resource = PanoptesResource(resource_site='test', resource_class='test',
                                                    resource_subclass='test',
                                                    resource_type='test', resource_id='test', resource_endpoint='test',
                                                    resource_plugin='test')
        self.__panoptes_resource.add_metadata('test', 'test')

    def test_enrichment_set(self):
        enrichment_set = PanoptesEnrichmentSet('int_001')
        enrichment_set.add('speed', 1000)
        enrichment_set.add('index', 001)
        enrichment_set.add('status', 'up')
        self.assertEquals(enrichment_set.key, 'int_001')
        self.assertDictEqual(enrichment_set.value, {'status': 'up', 'index': 1, 'speed': 1000})
        self.assertEquals(len(enrichment_set), 3)

        enrichment_set1 = PanoptesEnrichmentSet('int_002', {'status': 'down', 'index': 2, 'speed': 1000})
        self.assertEquals(enrichment_set1.key, 'int_002')
        self.assertDictEqual(enrichment_set1.value, {'status': 'down', 'index': 2, 'speed': 1000})

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentSet('int_001', 'string')

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentSet('int_001', 100)

    def test_enrichment_schema_validator(self):
        validator = PanoptesEnrichmentInterfaceSchemaValidator()
        enrichment_set = PanoptesEnrichmentSet('int_001')
        enrichment_set.add('speed', 1000)
        enrichment_set.add('index', 001)
        enrichment_set.add('status', 'up')
        self.assertTrue(validator.validate(enrichment_set))

        enrichment_set.add('status', 01)
        self.assertFalse(validator.validate(enrichment_set))

    @patch('time.time', mock_time)
    def test_enrichment_group(self):
        interface_validation_object = PanoptesEnrichmentInterfaceSchemaValidator()
        neighbor_validation_object = PanoptesEnrichmentNeighborSchemaValidator()

        interface_data = \
            '''{"data": [
            {"int_001": {"index": 1, "speed": 1000, "status": "up"}},
            {"int_002": {"index": 2, "speed": 1000, "status": "down"}}],
            "metadata": {"_enrichment_group_creation_timestamp": %f, "_enrichment_ttl": 300, "_execute_frequency": 60},
            "namespace": "interface"}''' % mock_time.return_value

        neighbor_data = \
            '''{"data": [{"host_name": {"mac": "aa:bb:cc:dd:ee:ff", "property": "Test Property", "vlan_id": 501}}],
            "metadata": {"_enrichment_group_creation_timestamp": %f, "_enrichment_ttl": 600, "_execute_frequency": 120},
            "namespace": "neighbor"}''' % mock_time.return_value

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentGroup(1, interface_validation_object, 300, 60)

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentGroup('interface', 'non_validation_object', 300, 60)

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentGroup('interface', interface_validation_object, '300', 60)

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentGroup('interface', interface_validation_object, 300, '60')

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentGroup('interface', interface_validation_object, 0, 60)

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentGroup('interface', interface_validation_object, 300, 0)

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentGroup('interface', interface_validation_object, 300, 60).\
                add_enrichment_set('not_PanoptesEnrichmentSet_obj')

        enrichment_set1 = PanoptesEnrichmentSet('int_001')
        enrichment_set1.add('speed', 1000)
        enrichment_set1.add('index', 001)
        enrichment_set1.add('status', 'up')

        enrichment_set2 = PanoptesEnrichmentSet('int_002')
        enrichment_set2.add('speed', 1000)
        enrichment_set2.add('index', 002)
        enrichment_set2.add('status', 'down')

        enrichment_group1 = PanoptesEnrichmentGroup('interface', interface_validation_object, 300, 60)
        enrichment_group1.add_enrichment_set(enrichment_set1)
        enrichment_group1.add_enrichment_set(enrichment_set2)

        self.assertEqual(enrichment_group1.namespace, 'interface')
        self.assertEqual(enrichment_group1.enrichment_ttl, 300)
        self.assertEqual(enrichment_group1.execute_frequency, 60)
        self.assertEqual(enrichment_group1.enrichment_group_creation_timestamp, mock_time.return_value)
        self.assertEqual(ordered(json.loads(json.dumps(enrichment_group1.data, cls=PanoptesEnrichmentEncoder))),
                         ordered(json.loads(interface_data)['data']))
        self.assertEqual(ordered(json.loads(enrichment_group1.json())), ordered(json.loads(interface_data)))
        self.assertEquals(len(enrichment_group1), 2)

        enrichment_set3 = PanoptesEnrichmentSet('int_002')
        enrichment_set3.add('speed', 1000)
        enrichment_set3.add('index', 002)
        enrichment_set3.add('status', 'down')

        self.assertEqual(ordered(json.loads(enrichment_group1.json())), ordered(json.loads(interface_data)))
        self.assertEqual(ordered(enrichment_group1.metadata), ordered(json.loads(interface_data)['metadata']))
        self.assertEquals(len(enrichment_group1), 2)

        test_metadata = json.loads(interface_data)['metadata']
        test_metadata['metadata_key'] = 'metadata_value'

        enrichment_group1.upsert_metadata('metadata_key', 'metadata_value')
        self.assertEqual(ordered(enrichment_group1.metadata), ordered(test_metadata))
        enrichment_group1.upsert_metadata('ttl', 300)
        with self.assertRaises(ValueError):
            enrichment_group1.upsert_metadata('_enrichment_ttl', 300)
        with self.assertRaises(AssertionError):
            enrichment_group1.upsert_metadata('metadata', {})
        with self.assertRaises(AssertionError):
            enrichment_group1.upsert_metadata('metadata', [])

        enrichment_set4 = PanoptesEnrichmentSet('host_name')
        enrichment_set4.add('vlan_id', 501)
        enrichment_set4.add('property', 'Test Property')
        enrichment_set4.add('mac', 'aa:bb:cc:dd:ee:ff')

        enrichment_group2 = PanoptesEnrichmentGroup('neighbor', neighbor_validation_object, 600, 120)
        enrichment_group2.add_enrichment_set(enrichment_set4)

        self.assertEqual(ordered(json.loads(enrichment_group2.json())), ordered(json.loads(neighbor_data)))
        self.assertEquals(len(enrichment_group2), 1)

        enrichment_set5 = PanoptesEnrichmentSet('host_name01')
        enrichment_set5.add('vlan_id', 502)
        enrichment_set5.add('property', 'Netops01.US')

        enrichment_set6 = PanoptesEnrichmentSet('host_name02')
        enrichment_set6.add('vlan_id', 503)
        enrichment_set6.add('mac', 'aa:bb:cc:dd:ee:ff')

        enrichment_group3 = PanoptesEnrichmentGroup('neighbor', neighbor_validation_object, 600, 120)
        enrichment_group3.add_enrichment_set(enrichment_set5)
        with self.assertRaises(AssertionError):
            enrichment_group3.add_enrichment_set(enrichment_set6)

        interface_store_data = '{"int_001": {"index": 1, "speed": 1000, "status": "up"}, ' \
                               '"int_002": {"index": 2, "speed": 1000, "status": "down"}}'

        neighbor_store_data = '{"host_name": {"mac": "aa:bb:cc:dd:ee:ff", "property": "Test Property", "vlan_id": 501}}'

        self.assertEquals(ordered(json.loads(enrichment_group1.serialize_data())),
                          ordered(json.loads(interface_store_data)))
        self.assertEquals(ordered(json.loads(enrichment_group2.serialize_data())),
                          ordered(json.loads(neighbor_store_data)))

        enrichment_group1.upsert_metadata('ttl', 300)
        with self.assertRaises(ValueError):
            enrichment_group1.upsert_metadata('_enrichment_ttl', 300)

        interface_data_serialized = '''{{"data": {{"int_001": {{"index": 1, "speed": 1000, "status": "up"}},
        "int_002": {{"index": 2, "speed": 1000, "status": "down"}}}}, "metadata":
        {{"_enrichment_group_creation_timestamp": {:.5f}, "_enrichment_ttl": 300, "_execute_frequency": 60,
        "metadata_key": "metadata_value", "ttl": 300}}}}'''.format(mock_time.return_value)

        neighbor_data_serialized = '''{{"data": {{"host_name": {{"mac": "aa:bb:cc:dd:ee:ff", "property": "Test Property",
        "vlan_id": 501}}}}, "metadata": {{"_enrichment_group_creation_timestamp": {:.5f},
        "_enrichment_ttl": 600, "_execute_frequency": 120}}}}'''.format(mock_time.return_value)

        self.assertEquals(ordered(json.loads(enrichment_group1.serialize())),
                          ordered(json.loads(interface_data_serialized)))

        self.assertEquals(ordered(json.loads(enrichment_group2.serialize())),
                          ordered(json.loads(neighbor_data_serialized)))

    @patch('time.time', mock_time)
    def test_enrichment_group_set(self):
        interface_validation_object = PanoptesEnrichmentInterfaceSchemaValidator()
        neighbor_validation_object = PanoptesEnrichmentNeighborSchemaValidator()

        panoptes_resource = self.__panoptes_resource

        enrichment_data = \
            '''{{"enrichment": [{{"metadata": {{"_enrichment_group_creation_timestamp": {:.5f}, "_enrichment_ttl": 600,
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

        enrichment_set1 = PanoptesEnrichmentSet('int_001')
        enrichment_set1.add('speed', 1000)
        enrichment_set1.add('index', 001)
        enrichment_set1.add('status', 'up')

        enrichment_set2 = PanoptesEnrichmentSet('int_002')
        enrichment_set2.add('speed', 1000)
        enrichment_set2.add('index', 002)
        enrichment_set2.add('status', 'down')

        enrichment_group1 = PanoptesEnrichmentGroup('interface', interface_validation_object, 300, 60)
        enrichment_group1.add_enrichment_set(enrichment_set1)
        enrichment_group1.add_enrichment_set(enrichment_set2)

        enrichment_set3 = PanoptesEnrichmentSet('host_name')
        enrichment_set3.add('vlan_id', 501)
        enrichment_set3.add('property', 'Test Property')
        enrichment_set3.add('mac', 'aa:bb:cc:dd:ee:ff')

        enrichment_group2 = PanoptesEnrichmentGroup('neighbor', neighbor_validation_object, 600, 120)
        enrichment_group2.add_enrichment_set(enrichment_set3)

        enrichment_group_set1 = PanoptesEnrichmentGroupSet(panoptes_resource)
        enrichment_group_set1.add_enrichment_group(enrichment_group1)
        enrichment_group_set1.add_enrichment_group(enrichment_group2)
        self.assertEquals(len(enrichment_group_set1), 2)
        group_set_repr = "PanoptesEnrichmentGroupSet({{'enrichment_group_set_creation_timestamp': {:.5f}, " \
                         "'resource': plugin|test|site|test|class|test|subclass|test" \
                         "|type|test|id|test|endpoint|test, " \
                         "'enrichment': set([PanoptesEnrichmentGroup({{'data': set([PanoptesEnrichmentSet({{" \
                         "'int_001': {{'status': 'up', 'index': 1, 'speed': 1000}}}}), " \
                         "PanoptesEnrichmentSet({{'int_002': {{'status': 'down', 'index': 2, 'speed': 1000}}}})]), " \
                         "'namespace': 'interface', 'metadata': " \
                         "{{'_enrichment_group_creation_timestamp': {:.5f}, '_enrichment_ttl': 300, " \
                         "'_execute_frequency': 60}}}}), PanoptesEnrichmentGroup({{" \
                         "'data': set([PanoptesEnrichmentSet(" \
                         "{{'host_name': {{" \
                         "'mac': 'aa:bb:cc:dd:ee:ff', 'property': 'Test Property', 'vlan_id': 501}}}})]), " \
                         "'namespace': 'neighbor', 'metadata': {{'_enrichment_group_creation_timestamp': " \
                         "{:.5f}, '_enrichment_ttl': 600, " \
                         "'_execute_frequency': 120}}}})])}})".format(mock_time.return_value,
                                                                      mock_time.return_value,
                                                                      mock_time.return_value)
        self.assertEquals(repr(enrichment_group_set1), group_set_repr)

        self.assertIsInstance(enrichment_group_set1.resource, PanoptesResource)
        self.assertEqual(enrichment_group_set1.enrichment_group_set_creation_timestamp, mock_time.return_value)

        self.assertEqual(
            ordered(json.loads(json.dumps(enrichment_group_set1.enrichment, cls=PanoptesEnrichmentEncoder))),
            ordered(json.loads(enrichment_data)['enrichment']))

        self.assertEqual(ordered(json.loads(enrichment_group_set1.json())['enrichment']),
                         ordered(json.loads(enrichment_data)['enrichment']))

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentGroupSet('bad_resource')

        with self.assertRaises(AssertionError):
            PanoptesEnrichmentGroupSet(panoptes_resource).add_enrichment_group('non_PanoptesEnrichmentGroup_obj')

        enrichment_group_set2 = PanoptesEnrichmentGroupSet(panoptes_resource)
        enrichment_group3 = PanoptesEnrichmentGroup('interface', interface_validation_object, 300, 60)

        with self.assertRaises(AssertionError):
            enrichment_group_set2.add_enrichment_group(enrichment_group3)

        self.assertFalse(enrichment_group_set1 == enrichment_group1)
        self.assertFalse(enrichment_group_set1 == enrichment_group_set2)

    @patch('time.time', mock_time)
    @patch('yahoo_panoptes.framework.resources.time', mock_time)
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

        enrichment_set1 = PanoptesEnrichmentSet('int_001')
        enrichment_set1.add('speed', 1000)
        enrichment_set1.add('index', 001)
        enrichment_set1.add('status', 'up')

        enrichment_group1 = PanoptesEnrichmentGroup('interface', interface_validation_object, 300, 60)
        enrichment_group1.add_enrichment_set(enrichment_set1)

        enrichment_set3 = PanoptesEnrichmentSet('host_name')
        enrichment_set3.add('vlan_id', 501)
        enrichment_set3.add('property', 'Test Property')
        enrichment_set3.add('mac', 'aa:bb:cc:dd:ee:ff')

        enrichment_group2 = PanoptesEnrichmentGroup('neighbor', neighbor_validation_object, 600, 120)
        enrichment_group2.add_enrichment_set(enrichment_set3)

        enrichment_group_set1 = PanoptesEnrichmentGroupSet(panoptes_resource)
        enrichment_group_set1.add_enrichment_group(enrichment_group1)
        enrichment_group_set1.add_enrichment_group(enrichment_group2)

        panoptes_resource01 = PanoptesResource(resource_site='test_site',
                                               resource_class='test_class',
                                               resource_subclass='test_subclass',
                                               resource_type='test_type',
                                               resource_id='test_resource_id01',
                                               resource_endpoint='test_endpoint01',
                                               resource_plugin='test_plugin')

        panoptes_resource02 = PanoptesResource(resource_site='test_site',
                                               resource_class='test_class',
                                               resource_subclass='test_subclass',
                                               resource_type='test_type',
                                               resource_id='test_resource_id02',
                                               resource_endpoint='test_endpoint02',
                                               resource_plugin='test_plugin')

        enrichment_set4 = PanoptesEnrichmentSet('host_name01')
        enrichment_set4.add('vlan_id', 502)
        enrichment_set4.add('property', 'Test Property')
        enrichment_set4.add('mac', 'aa:bb:cc:dd:ee:ff')

        enrichment_group3 = PanoptesEnrichmentGroup('neighbor', neighbor_validation_object, 600, 120)
        enrichment_group3.add_enrichment_set(enrichment_set3)
        enrichment_group3.add_enrichment_set(enrichment_set4)

        enrichment_group_set2 = PanoptesEnrichmentGroupSet(panoptes_resource01)
        enrichment_group_set2.add_enrichment_group(enrichment_group1)
        enrichment_group_set2.add_enrichment_group(enrichment_group3)

        multi_enrichment_group_set = PanoptesEnrichmentMultiGroupSet()
        multi_enrichment_group_set.add_enrichment_group_set(enrichment_group_set1)
        multi_enrichment_group_set.add_enrichment_group_set(enrichment_group_set2)

        multi_enrichment_group_set_repr = \
            "PanoptesEnrichmentMultiGroupSet({{'group_sets': set([PanoptesEnrichmentGroupSet({{" \
            "'enrichment_group_set_creation_timestamp': {:.5f}, 'resource': plugin|test_plugin|" \
            "site|test_site|class|test_class|subclass|test_subclass|type|test_type|id|test_resource_id01|" \
            "endpoint|test_endpoint01, 'enrichment': set([PanoptesEnrichmentGroup({{'data': set([" \
            "PanoptesEnrichmentSet({{'int_001': {{'status': 'up', 'index': 1, 'speed': 1000}}}})]), " \
            "'namespace': 'interface', 'metadata': {{'_enrichment_group_creation_timestamp': {:.5f}, " \
            "'_enrichment_ttl': 300, '_execute_frequency': 60}}}}), PanoptesEnrichmentGroup({{" \
            "'data': set([PanoptesEnrichmentSet({{'host_name': {{" \
            "'mac': 'aa:bb:cc:dd:ee:ff', 'property': 'Test Property', " \
            "'vlan_id': 501}}}}), PanoptesEnrichmentSet({{'host_name01': {{'mac': 'aa:bb:cc:dd:ee:ff', " \
            "'property': 'Test Property', 'vlan_id': 502}}}})]), 'namespace': 'neighbor', 'metadata': {{" \
            "'_enrichment_group_creation_timestamp': {:.5f}, '_enrichment_ttl': 600, " \
            "'_execute_frequency': 120}}}})])}}), PanoptesEnrichmentGroupSet({{" \
            "'enrichment_group_set_creation_timestamp': {:.5f}, 'resource': plugin|test|site|test|" \
            "class|test|subclass|test|type|test|id|test|endpoint|test, 'enrichment': set([PanoptesEnrichmentGroup({{" \
            "'data': set([PanoptesEnrichmentSet({{'int_001': {{'status': 'up', 'index': 1, 'speed': 1000}}}})]), " \
            "'namespace': 'interface', 'metadata': {{'_enrichment_group_creation_timestamp': {:.5f}, " \
            "'_enrichment_ttl': 300, '_execute_frequency': 60}}}}), PanoptesEnrichmentGroup({{'data': set([" \
            "PanoptesEnrichmentSet({{'host_name': {{'mac': 'aa:bb:cc:dd:ee:ff', 'property': 'Test Property', " \
            "'vlan_id': 501}}}})]), 'namespace': 'neighbor', 'metadata': {{'_enrichment_group_creation_timestamp': " \
            "{:.5f}, '_enrichment_ttl': 600, '_execute_frequency': 120}}}})])}})])}})".format(
                mock_time.return_value,
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
            multi_enrichment_group_set.add_enrichment_group_set('non_enrichment_group')

        enrichment_group_set3 = PanoptesEnrichmentGroupSet(panoptes_resource01)
        with self.assertRaises(AssertionError):
            multi_enrichment_group_set.add_enrichment_group_set(enrichment_group_set3)


class TestPanoptesEnrichmentCacheStore(unittest.TestCase):
    @patch('redis.StrictRedis', PanoptesMockRedis)
    @patch('time.time', mock_time)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = _get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                                 key_value_store_class_list=[PanoptesEnrichmentCacheKeyValueStore])

        self._enrichment_kv = self._panoptes_context.get_kv_store(PanoptesEnrichmentCacheKeyValueStore)

        self._panoptes_resource = PanoptesResource(resource_site='test_site',
                                                   resource_class='test_class',
                                                   resource_subclass='test_subclass',
                                                   resource_type='test_type',
                                                   resource_id='test_resource_id',
                                                   resource_endpoint='test_endpoint',
                                                   resource_plugin='test_plugin')

        interface_validation_object = PanoptesEnrichmentInterfaceSchemaValidator()
        neighbor_validation_object = PanoptesEnrichmentNeighborSchemaValidator()

        enrichment_set1 = PanoptesEnrichmentSet('int_001')
        enrichment_set1.add('speed', 1000)
        enrichment_set1.add('index', 001)
        enrichment_set1.add('status', 'up')

        enrichment_set2 = PanoptesEnrichmentSet('int_002')
        enrichment_set2.add('speed', 1000)
        enrichment_set2.add('index', 002)
        enrichment_set2.add('status', 'down')

        enrichment_group1 = PanoptesEnrichmentGroup('interface', interface_validation_object, 300, 60)
        enrichment_group1.add_enrichment_set(enrichment_set1)
        enrichment_group1.add_enrichment_set(enrichment_set2)

        enrichment_set3 = PanoptesEnrichmentSet('host_name')
        enrichment_set3.add('vlan_id', 501)
        enrichment_set3.add('property', 'Test Property')
        enrichment_set3.add('mac', 'aa:bb:cc:dd:ee:ff')

        enrichment_group2 = PanoptesEnrichmentGroup('neighbor', neighbor_validation_object, 600, 120)
        enrichment_group2.add_enrichment_set(enrichment_set3)

        self.enrichment_group_set1 = PanoptesEnrichmentGroupSet(self._panoptes_resource)
        self.enrichment_group_set1.add_enrichment_group(enrichment_group1)
        self.enrichment_group_set1.add_enrichment_group(enrichment_group2)

        self._panoptes_resource01 = PanoptesResource(resource_site='test_site',
                                                     resource_class='test_class',
                                                     resource_subclass='test_subclass',
                                                     resource_type='test_type',
                                                     resource_id='test_resource_id01',
                                                     resource_endpoint='test_endpoint01',
                                                     resource_plugin='test_plugin')

        enrichment_set4 = PanoptesEnrichmentSet('host_name01')
        enrichment_set4.add('vlan_id', 502)
        enrichment_set4.add('property', 'Test Property')
        enrichment_set4.add('mac', 'aa:bb:cc:dd:ee:ff')

        enrichment_group3 = PanoptesEnrichmentGroup('neighbor', neighbor_validation_object, 600, 120)
        enrichment_group3.add_enrichment_set(enrichment_set3)
        enrichment_group3.add_enrichment_set(enrichment_set4)

        self.enrichment_group_set2 = PanoptesEnrichmentGroupSet(self._panoptes_resource01)
        self.enrichment_group_set2.add_enrichment_group(enrichment_group1)
        self.enrichment_group_set2.add_enrichment_group(enrichment_group3)

        self._multi_enrichment_group_set = PanoptesEnrichmentMultiGroupSet()
        self._multi_enrichment_group_set.add_enrichment_group_set(self.enrichment_group_set1)
        self._multi_enrichment_group_set.add_enrichment_group_set(self.enrichment_group_set2)

    @patch('time.time', mock_time)
    def test_panoptes_enrichment_set(self):
        enrichment_set1 = PanoptesEnrichmentSet('int_001')
        enrichment_set1.add('speed', 1000)
        enrichment_set1.add('index', 001)
        enrichment_set1.add('status', 'up')
        self.assertEquals(enrichment_set1.json(),
                          '{"int_001": {"index": 1, "speed": 1000, "status": "up"}}')
        self.assertEquals(repr(enrichment_set1),
                          "PanoptesEnrichmentSet({'int_001': {'status': 'up', 'index': 1, 'speed': 1000}})")

        enrichment_set2 = PanoptesEnrichmentSet('int_002')
        enrichment_set2.add('speed', 1000)
        enrichment_set2.add('index', 002)
        enrichment_set2.add('status', 'down')

        interface_validation_object = PanoptesEnrichmentInterfaceSchemaValidator()
        enrichment_group1 = PanoptesEnrichmentGroup('interface', interface_validation_object, 300, 60)

        self.assertFalse(enrichment_set1 == enrichment_group1)
        self.assertFalse(enrichment_set1 == enrichment_set2)

    @patch('time.time', mock_time)
    def test_panoptes_enrichment_group(self):
        interface_validation_object = PanoptesEnrichmentInterfaceSchemaValidator()
        enrichment_group1 = PanoptesEnrichmentGroup('interface', interface_validation_object, 300, 60)
        self.assertEquals(enrichment_group1.enrichment_schema, PanoptesEnrichmentInterfaceSchemaValidator.schema)
        self.assertEquals(repr(enrichment_group1), "PanoptesEnrichmentGroup({{'data': set([]), "
                                                   "'namespace': 'interface', "
                                                   "'metadata': {{"
                                                   "'_enrichment_group_creation_timestamp': {:.5f}, "
                                                   "'_enrichment_ttl': 300, "
                                                   "'_execute_frequency': 60}}}})".format(mock_time.return_value))

        enrichment_set1 = PanoptesEnrichmentSet('int_001')
        enrichment_set1.add('speed', 1000)
        enrichment_set1.add('index', 001)
        enrichment_set1.add('status', 'up')

        self.assertFalse(enrichment_group1 == enrichment_set1)

        enrichment_group2 = PanoptesEnrichmentGroup('interface', interface_validation_object, 300, 60)
        enrichment_group3 = PanoptesEnrichmentGroup('other_namespace', interface_validation_object, 300, 60)
        self.assertTrue(enrichment_group1 == enrichment_group2)
        self.assertFalse(enrichment_group1 == enrichment_group3)

    @patch('time.time', mock_time)
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

        _store_enrichment_data(self._panoptes_context, self.enrichment_group_set1, 'PanoptesPluginInfo')

        self.assertNotEquals(ordered(interface_result_data),
                             ordered(json.loads(self._enrichment_kv.get('test_resource_id:neighbor'))))

        self.assertEquals(ordered(interface_result_data),
                          ordered(json.loads(self._enrichment_kv.get('test_resource_id:interface'))))

        self.assertEquals(ordered(neighbor_result_data),
                          ordered(json.loads(self._enrichment_kv.get('test_resource_id:neighbor'))))

    def test_store_enrichment_data_enrichment_multi_group_set(self):

        enrichment_result_keys = ['test_resource_id01:interface', 'test_resource_id01:neighbor',
                                  'test_resource_id:interface', 'test_resource_id:neighbor']

        neighbor_result_data = \
            {"data": {"host_name": {"mac": "aa:bb:cc:dd:ee:ff", "property": "Test Property", "vlan_id": 501},
                      "host_name01": {"mac": "aa:bb:cc:dd:ee:ff", "property": "Test Property", "vlan_id": 502}},
             "metadata": {"_enrichment_group_creation_timestamp": mock_time.return_value, "_enrichment_ttl": 600,
                          "_execute_frequency": 120}}

        _store_enrichment_data(self._panoptes_context, self._multi_enrichment_group_set, 'PanoptesPluginInfo')

        self.assertEquals(enrichment_result_keys, self._enrichment_kv.find_keys('*'))

        self.assertEquals(ordered(neighbor_result_data),
                          ordered(json.loads(self._enrichment_kv.get('test_resource_id01:neighbor'))))

        self.assertNotEquals(ordered(neighbor_result_data),
                             ordered(json.loads(self._enrichment_kv.get('test_resource_id01:interface'))))
