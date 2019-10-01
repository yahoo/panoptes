"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""

import os
import json
from unittest import TestCase
from mock import patch, PropertyMock
from configobj import ConfigObj
from testfixtures import LogCapture

from yahoo_panoptes.polling.polling_plugin_agent import _process_metrics_group_set, \
    PanoptesMetricsKeyValueStore, PanoptesPollingPluginAgentKeyValueStore, \
    PanoptesPollingPluginKeyValueStore, PanoptesPollingAgentContext, start_polling_plugin_agent

from yahoo_panoptes.framework.metrics import PanoptesMetricsGroupSet,\
    PanoptesMetricsGroup, PanoptesMetricDimension, PanoptesMetric, PanoptesMetricType
from yahoo_panoptes.framework.resources import PanoptesResource
from yahoo_panoptes.framework.resources import PanoptesContext
from yahoo_panoptes.framework import const

from tests.test_framework import panoptes_mock_redis_strict_client
from tests.mock_panoptes_producer import MockPanoptesMessageProducer
from tests.mock_redis import PanoptesMockRedis
from tests.helpers import get_test_conf_file

path = os.path.dirname(os.path.realpath(__file__))
pwd = os.path.dirname(os.path.abspath(__file__))

plugin_results_file = '{}/metric_group_sets/interface_plugin_results.json'.format(pwd)


_, global_panoptes_test_conf_file = get_test_conf_file()


class MockPanoptesConfigObject(object):
    """
    A mock plugin agent
    """
    pass


class TestPollingPluginAgent(TestCase):

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file)

    def prepare_panoptes_metrics_group_set(self, file_path=None):

        panoptes_metric_group_set = PanoptesMetricsGroupSet()

        path_to_metrics_file = plugin_results_file if file_path is None else file_path

        with open(path_to_metrics_file) as results_file:
            panoptes_json_data = json.load(results_file)

            for panoptes_data_object in panoptes_json_data:

                resource = panoptes_data_object['resource']

                panoptes_resource = PanoptesResource(
                    resource_site=resource['resource_site'],
                    resource_class=resource['resource_class'],
                    resource_subclass=resource['resource_subclass'],
                    resource_type=resource['resource_type'],
                    resource_id=resource['resource_id'],
                    resource_endpoint=resource['resource_endpoint'],
                    resource_plugin=resource['resource_plugin'],
                    resource_creation_timestamp=0)

                panoptes_metric_group = PanoptesMetricsGroup(
                    resource=panoptes_resource,
                    group_type=panoptes_data_object['metrics_group_type'],
                    interval=panoptes_data_object['metrics_group_interval']
                )

                for dimension in panoptes_data_object['dimensions']:
                    panoptes_metric_group.add_dimension(
                        PanoptesMetricDimension(
                            name=dimension['dimension_name'],
                            value=dimension['dimension_value']
                        )
                    )

                for metric in panoptes_data_object['metrics']:
                    panoptes_metric_group.add_metric(
                        PanoptesMetric(
                            metric_name=metric['metric_name'],
                            metric_value=metric['metric_value'],
                            metric_type=PanoptesMetricType().GAUGE if metric['metric_type'] == 'gauge'
                                                                   else PanoptesMetricType().COUNTER,
                            metric_creation_timestamp=metric['metric_creation_timestamp']
                        )
                    )

                panoptes_metric_group_set.add(panoptes_metric_group)

        return panoptes_metric_group_set

    @patch('yahoo_panoptes.framework.context.PanoptesContext.get_kv_store')
    @patch('yahoo_panoptes.framework.context.PanoptesContext._get_message_producer')
    @patch('yahoo_panoptes.framework.context.PanoptesContext.message_producer', new_callable=PropertyMock)
    def test_transform_converts_correct_counters(self, message_producer, message_producer_property, kv_store):

        mock_panoptes_plugin = MockPanoptesConfigObject()
        mock_panoptes_plugin.config = ConfigObj(pwd + '/config_files/test_panoptes_polling_plugin_conf.ini')
        kv_store.return_value = PanoptesMockRedis()

        mock_message_producer = MockPanoptesMessageProducer()

        message_producer.return_value = mock_message_producer
        message_producer_property.return_value = message_producer_property

        panoptes_context = PanoptesContext(config_file=os.path.join(path, 'config_files/test_panoptes_config.ini'))

        for i in range(1, 4):
            panoptes_metrics = self.prepare_panoptes_metrics_group_set(
                '{}/metric_group_sets/interface_plugin_counter_{}.json'.format(pwd, i))
            _process_metrics_group_set(context=panoptes_context, results=panoptes_metrics, plugin=mock_panoptes_plugin)

        published_kafka_messages = panoptes_context.message_producer._kafka_producer

        expected_results = [
            {
                'counter|test_system_uptime': 0,
                'counter|extra_test_metric': 0
            },
            {
                'counter|test_system_uptime': 60,
                'gauge|test_system_uptime': 0,
                'counter|extra_test_metric': 120,
                'gauge|extra_test_metric': 1
            },
            {
                'counter|test_system_uptime': 120,
                'gauge|test_system_uptime': 0,
                'counter|extra_test_metric': 240,
                'gauge|extra_test_metric': 1
            }
        ]

        for i, kafka_message in enumerate(published_kafka_messages):
            parsed_json = json.loads(kafka_message['message'])['metrics']
            for panoptes_metric in parsed_json:

                key = "|".join([panoptes_metric['metric_type'], panoptes_metric['metric_name']])

                self.assertEquals(
                    expected_results[i].get(key, None),
                    panoptes_metric['metric_value']
                )

    @patch('yahoo_panoptes.framework.context.PanoptesContext._get_message_producer')
    @patch('yahoo_panoptes.framework.context.PanoptesContext.message_producer', new_callable=PropertyMock)
    def test_kafka_produces_to_the_correct_topics(self, message_producer, message_producer_property):
        spec_paths = [
            'config_files/test_panoptes_config.ini',             # Produces to site topics but not Global
            'config_files/test_panoptes_config_kafka_true.ini'   # Sends enrichment results to both site & global topics
        ]

        expected_results = [
            {'test_site-processed': 1},
            {'panoptes-metrics': 1, 'test_site-processed': 1}
        ]

        all_tests_pass = True

        for i in range(len(spec_paths)):
            mock_panoptes_plugin = MockPanoptesConfigObject()
            mock_panoptes_plugin.config = ConfigObj(pwd + '/config_files/test_panoptes_polling_plugin_conf.ini')

            mock_message_producer = MockPanoptesMessageProducer()

            message_producer.return_value = mock_message_producer
            message_producer_property.return_value = message_producer_property

            panoptes_context = PanoptesContext(config_file=os.path.join(path, spec_paths[i]))

            metrics_group_set = self.prepare_panoptes_metrics_group_set()

            _process_metrics_group_set(context=panoptes_context, results=metrics_group_set, plugin=mock_panoptes_plugin)

            published_kafka_messages = panoptes_context.message_producer._kafka_producer

            actual_result = {}

            for message in published_kafka_messages:
                if message['topic'] in actual_result:
                    actual_result[message['topic']] += 1
                else:
                    actual_result[message['topic']] = 1

            all_tests_pass &= (actual_result == expected_results[i])

        self.assertTrue(all_tests_pass)

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    def test_kv_store(self):

        # Test namespace is mirrored correctly
        self.assertEqual(
            PanoptesMetricsKeyValueStore(self._panoptes_context).namespace,
            const.METRICS_KEY_VALUE_NAMESPACE)

        self.assertEqual(
            PanoptesPollingPluginAgentKeyValueStore(self._panoptes_context).namespace,
            const.POLLING_PLUGIN_AGENT_KEY_VALUE_NAMESPACE)

        self.assertEqual(
            PanoptesPollingPluginKeyValueStore(self._panoptes_context).namespace,
            const.PLUGINS_KEY_VALUE_NAMESPACE)

        # Test for Collision
        namespace_keys = {
            const.METRICS_KEY_VALUE_NAMESPACE,
            const.POLLING_PLUGIN_AGENT_KEY_VALUE_NAMESPACE,
            const.PLUGINS_KEY_VALUE_NAMESPACE
        }

        self.assertEqual(len(namespace_keys), 3)

    @patch('yahoo_panoptes.framework.const.DEFAULT_CONFIG_FILE_PATH', global_panoptes_test_conf_file)
    def test_polling_agent_context(self):

        panoptes_polling_context = PanoptesPollingAgentContext()

        self.assertEqual(panoptes_polling_context.kv_stores, {})

        with self.assertRaises(AttributeError):
            panoptes_polling_context.zookeeper_client

        with self.assertRaises(AttributeError):
            panoptes_polling_context.kafka_client

    @patch('yahoo_panoptes.framework.const.DEFAULT_CONFIG_FILE_PATH', global_panoptes_test_conf_file)
    @patch('yahoo_panoptes.polling.polling_plugin_agent.PanoptesPollingAgentContext')
    def test_start_polling_agent_exits(self, context):

        context.side_effect = Exception()

        with self.assertRaises(SystemExit):
            start_polling_plugin_agent()

    @patch('yahoo_panoptes.framework.const.DEFAULT_CONFIG_FILE_PATH', global_panoptes_test_conf_file)
    def test_start_polling_agent(self):

        # Assert Nothing Throws
        with LogCapture() as l:
            start_polling_plugin_agent()

        with patch('yahoo_panoptes.polling.polling_plugin_agent.PanoptesCeleryInstance') as c:
            c.side_effect = Exception('Fatal Error')
            with self.assertRaises(SystemExit):
                start_polling_plugin_agent()



