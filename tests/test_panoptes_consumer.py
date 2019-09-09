"""
Copyright 2019, Verizon Media
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
import unittest
import json
from mock import patch
from helpers import get_test_conf_file

from test_framework import panoptes_mock_redis_strict_client
from yahoo_panoptes.framework.utilities.consumer import PanoptesConsumer, PanoptesResourcesConsumer, \
    CONSUMER_TYPE_NAMES, PanoptesConsumerRecordValidator, PanoptesConsumerTypes
from yahoo_panoptes.framework.resources import PanoptesContext, PanoptesResource, PanoptesResourceSet

from mock_kafka_consumer import MockKafkaConsumer


def panoptes_consumer_callback():
    return True


class TestValidators(unittest.TestCase):

    def setUp(self):

        self._panoptes_metric = {
            u'metrics_group_interval': 60,
            u'resource': {
                u'resource_site': u'test_site',
                u'resource_id': u'test_id',
                u'resource_class': u'network',
                u'resource_plugin': u'test_plugin',
                u'resource_creation_timestamp': 1567823517.46,
                u'resource_subclass': u'test_subclass',
                u'resource_endpoint': u'test_endpoint',
                u'resource_metadata': {
                    u'test_metadata_key': u'test_metadata_value',
                    u'_resource_ttl': "604800"
                },
                u'resource_type': u'test_type'
            },
            u'dimensions': [
                {
                    u'dimension_name': u'cpu_name',
                    u'dimension_value': u'test_cpu_name_value'
                },
                {
                    u'dimension_name': u'cpu_no',
                    u'dimension_value': u'test_cpu_no_value'
                },
                {
                    u'dimension_name': u'cpu_type',
                    u'dimension_value': u'test_cpu_type_value'
                }
            ],
            u'metrics_group_type': u'cpu',
            u'metrics': [
                {
                    u'metric_creation_timestamp': 1567823946.72,
                    u'metric_type': u'gauge',
                    u'metric_name': u'cpu_utilization',
                    u'metric_value': 0
                }
            ],
            u'metrics_group_creation_timestamp': 1567823946.72,
            u'metrics_group_schema_version': u'0.2'
        }

        self._panoptes_resource = PanoptesResource(resource_site='test', resource_class='test',
                                                   resource_subclass='test', resource_type='test',
                                                   resource_id='test', resource_endpoint='test',
                                                   resource_plugin='test')

        self._panoptes_resource.add_metadata('test', 'test')

        self._panoptes_resource_set = PanoptesResourceSet()
        self._panoptes_resource_set.add(self._panoptes_resource)

    def test_default_record(self):
        self.assertTrue(PanoptesConsumerRecordValidator.validate(PanoptesConsumerTypes.METRICS, self._panoptes_metric))
        self.assertTrue(PanoptesConsumerRecordValidator.validate(PanoptesConsumerTypes.PROCESSED,
                                                                 self._panoptes_metric))
        self.assertTrue(PanoptesConsumerRecordValidator.validate(PanoptesConsumerTypes.RESOURCES,
                                                                 json.loads(self._panoptes_resource_set.json)))
        self.assertFalse(PanoptesConsumerRecordValidator.validate(5, {}))

    def test_resource_throws(self):

        argument_overrides = [
            ('resource_site', 1),
            ('resource_id', None),
            ('resource_class', 1.5),
            ('resource_plugin', {}),
            ('resource_creation_timestamp', '1567823517'),
            ('resource_subclass', []),
            ('resource_endpoint', 123456.789)
        ]

        for (override_key, override_value) in argument_overrides:
            schema_arguments = self._panoptes_metric.copy()
            schema_arguments['resource'][override_key] = override_value
            self.assertEquals(
                PanoptesConsumerRecordValidator.validate_metrics(schema_arguments),
                False
            )


class TestConsumer(unittest.TestCase):

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kafka.KafkaConsumer', MockKafkaConsumer)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file)

        self._panoptes_consumer_arguments = {
            'panoptes_context': self._panoptes_context,
            'consumer_type': 0,
            'topics': ['panoptes-metrics'],
            'client_id': 1337,
            'group': 'panoptes-consumer-group',
            'keys': ['interface-metrics'],
            'poll_timeout': 200,
            'callback': panoptes_consumer_callback,
            'validate': False
        }

    def test_bad_parameters(self):

        argument_overrides = [
            ('panoptes_context', None),
            ('consumer_type', len(CONSUMER_TYPE_NAMES) + 1),
            ('consumer_type', -1),
            ('topics', []),
            ('client_id', ''),
            ('keys', ['', '']),
            ('keys', []),
            ('poll_timeout', '1500'),
            ('callback', {}),
            ('callback', []),
            ('validate', 'True')
        ]

        for (override_key, override_value) in argument_overrides:
            consumer_arguments = self._panoptes_consumer_arguments.copy()
            consumer_arguments[override_key] = override_value

            with self.assertRaises(AssertionError):
                PanoptesConsumer(panoptes_context=consumer_arguments['panoptes_context'],
                                 consumer_type=consumer_arguments['consumer_type'],
                                 topics=consumer_arguments['topics'],
                                 client_id=consumer_arguments['client_id'],
                                 group=consumer_arguments['group'],
                                 keys=consumer_arguments['keys'],
                                 poll_timeout=consumer_arguments['poll_timeout'],
                                 callback=consumer_arguments['callback'],
                                 validate=consumer_arguments['validate'])

    def test_properties(self):

        panoptes_consumer = PanoptesConsumer(panoptes_context=self._panoptes_context,
                                             consumer_type=0,
                                             topics=['panoptes-metrics'],
                                             client_id='1337',
                                             group='panoptes-consumer-group',
                                             keys=['class:subclass:type'],
                                             poll_timeout=200,
                                             callback=panoptes_consumer_callback,
                                             validate=True)

        self.assertEqual(repr(panoptes_consumer.panoptes_context), repr(self._panoptes_context))
        self.assertEqual(panoptes_consumer.client_id, '1337')
        self.assertEqual(panoptes_consumer.group, 'panoptes-consumer-group')
        self.assertEqual(panoptes_consumer.poll_timeout, 200 * 1000)
        self.assertEqual(panoptes_consumer.consumer_type, 0)
        self.assertEqual(panoptes_consumer.keys, ['class:subclass:type'])

    @patch('kafka.KafkaConsumer', MockKafkaConsumer)
    @patch('yahoo_panoptes.framework.utilities.consumer.PanoptesConsumer.asked_to_stop')
    def test_panoptes_consumer(self, asked_to_stop):

        asked_to_stop.side_effect = [False, True]

        reference = {}

        def consumer_callback(key, object, ref=reference):

            ref['key'] = key
            ref['object'] = object

        panoptes_consumer = PanoptesConsumer(panoptes_context=self._panoptes_context,
                                             consumer_type=0,
                                             topics=['panoptes-metrics'],
                                             client_id='1337',
                                             group='panoptes-consumer-group',
                                             keys=['class:subclass:type'],
                                             poll_timeout=200,
                                             callback=consumer_callback,
                                             validate=True)

        self.assertEqual(panoptes_consumer._asked_to_stop, False)

        panoptes_consumer.start_consumer()
        panoptes_consumer.stop_consumer()

        self.assertEqual(panoptes_consumer._asked_to_stop, True)
        self.assertEqual(reference['key'], 'class:subclass:type')

        self.assertDictEqual(reference['object'], {
            u'metrics_group_interval': 60,
            u'resource': {
                u'resource_site': u'test_site',
                u'resource_class': u'network',
                u'resource_id': u'test_id',
                u'resource_plugin': u'test_plugin',
                u'resource_creation_timestamp': 1567823517.46,
                u'resource_subclass': u'test_subclass',
                u'resource_endpoint': u'test_endpoint',
                u'resource_metadata': {
                    u'test_metadata_key': u'test_metadata_value',
                    u'_resource_ttl': u'604800'
                }, u'resource_type': u'test_type'
            },
            u'dimensions': [
                {u'dimension_name': u'cpu_name', u'dimension_value': u'test_cpu_name_value'},
                {u'dimension_name': u'cpu_no', u'dimension_value': u'test_cpu_no_value'},
                {u'dimension_name': u'cpu_type', u'dimension_value': u'test_cpu_type_value'}
            ],
            u'metrics_group_type': u'cpu',
            u'metrics': [
                {
                    u'metric_creation_timestamp': 1567823946.72,
                    u'metric_type': u'gauge',
                    u'metric_name': u'cpu_utilization',
                    u'metric_value': 0
                }
            ],
            u'metrics_group_creation_timestamp': 1567823946.72,
            u'metrics_group_schema_version': u'0.2'
        })

    def test_resource_consumer(self):

        resource_consumer = PanoptesResourcesConsumer(panoptes_context=self._panoptes_context,
                                                      client_id='1337',
                                                      group='panoptes-consumer-group',
                                                      keys=['class:subclass:type'],
                                                      poll_timeout=200,
                                                      callback=panoptes_consumer_callback,
                                                      validate=True)

        self.assertEqual(resource_consumer._topics, ['local-resources'])
