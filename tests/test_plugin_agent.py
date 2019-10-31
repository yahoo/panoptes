"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""

from builtins import range
from builtins import object
import os
import json
import re
from unittest import TestCase
from mock import patch, PropertyMock, Mock
from configobj import ConfigObj

from yahoo_panoptes.polling.polling_plugin_agent import PanoptesMetricsKeyValueStore, \
    PanoptesPollingPluginAgentKeyValueStore, PanoptesPollingPluginKeyValueStore, \
    PanoptesPollingAgentContext, start_polling_plugin_agent, polling_plugin_task, \
    _process_metrics_group_set, shutdown_signal_handler as polling_agent_shutdown, \
    PanoptesPollingTaskContext

from yahoo_panoptes.discovery.discovery_plugin_agent import PanoptesDiscoveryPluginAgentKeyValueStore, \
    PanoptesDiscoveryPluginKeyValueStore, PanoptesDiscoveryAgentContext, start_discovery_plugin_agent, \
    __send_resource_set as discovery_callback_function, shutdown_signal_handler as discovery_agent_shutdown, \
    discovery_plugin_task

from yahoo_panoptes.enrichment.enrichment_plugin_agent import PanoptesEnrichmentPluginAgentKeyValueStore, \
    PanoptesEnrichmentPluginKeyValueStore, start_enrichment_plugin_agent

from yahoo_panoptes.framework.metrics import PanoptesMetricsGroupSet,\
    PanoptesMetricsGroup, PanoptesMetricDimension, PanoptesMetric, PanoptesMetricType
from yahoo_panoptes.framework.resources import PanoptesResource, PanoptesResourceSet, PanoptesContext
from yahoo_panoptes.framework import const

from tests.test_framework import panoptes_mock_redis_strict_client
from tests.mock_panoptes_producer import MockPanoptesMessageProducer
from tests.mock_redis import PanoptesMockRedis
from tests.helpers import get_test_conf_file

path = os.path.dirname(os.path.realpath(__file__))
pwd = os.path.dirname(os.path.abspath(__file__))

plugin_results_file = u'{}/metric_group_sets/interface_plugin_results.json'.format(pwd)


_, global_panoptes_test_conf_file = get_test_conf_file()


class MockPanoptesObject(object):
    """
    A mock plugin agent
    """
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class PluginAgent(TestCase):

    @patch(u'redis.StrictRedis', panoptes_mock_redis_strict_client)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file)

    def prepare_panoptes_metrics_group_set(self, file_path=None):

        panoptes_metric_group_set = PanoptesMetricsGroupSet()

        path_to_metrics_file = plugin_results_file if file_path is None else file_path

        with open(path_to_metrics_file) as results_file:
            panoptes_json_data = json.load(results_file)

            for panoptes_data_object in panoptes_json_data:

                resource = panoptes_data_object[u'resource']

                panoptes_resource = PanoptesResource(
                    resource_site=resource[u'resource_site'],
                    resource_class=resource[u'resource_class'],
                    resource_subclass=resource[u'resource_subclass'],
                    resource_type=resource[u'resource_type'],
                    resource_id=resource[u'resource_id'],
                    resource_endpoint=resource[u'resource_endpoint'],
                    resource_plugin=resource[u'resource_plugin'],
                    resource_creation_timestamp=0)

                panoptes_metric_group = PanoptesMetricsGroup(
                    resource=panoptes_resource,
                    group_type=panoptes_data_object[u'metrics_group_type'],
                    interval=panoptes_data_object[u'metrics_group_interval']
                )

                for dimension in panoptes_data_object[u'dimensions']:
                    panoptes_metric_group.add_dimension(
                        PanoptesMetricDimension(
                            name=dimension[u'dimension_name'],
                            value=dimension[u'dimension_value']
                        )
                    )

                for metric in panoptes_data_object[u'metrics']:
                    panoptes_metric_group.add_metric(
                        PanoptesMetric(
                            metric_name=metric[u'metric_name'],
                            metric_value=metric[u'metric_value'],
                            metric_type=PanoptesMetricType().GAUGE if metric[u'metric_type'] == u'gauge'
                            else PanoptesMetricType().COUNTER,
                            metric_creation_timestamp=metric[u'metric_creation_timestamp']
                        )
                    )

                panoptes_metric_group_set.add(panoptes_metric_group)

        return panoptes_metric_group_set

    @patch(u'redis.StrictRedis', panoptes_mock_redis_strict_client)
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

        self.assertEqual(
            PanoptesDiscoveryPluginAgentKeyValueStore(self._panoptes_context).namespace,
            const.DISCOVERY_PLUGIN_AGENT_KEY_VALUE_NAMESPACE
        )

        self.assertEqual(
            PanoptesDiscoveryPluginKeyValueStore(self._panoptes_context).namespace,
            const.PLUGINS_KEY_VALUE_NAMESPACE
        )

        self.assertEqual(
            PanoptesEnrichmentPluginAgentKeyValueStore(self._panoptes_context).namespace,
            const.ENRICHMENT_PLUGIN_AGENT_KEY_VALUE_NAMESPACE
        )

        self.assertEqual(
            PanoptesEnrichmentPluginKeyValueStore(self._panoptes_context).namespace,
            const.PLUGINS_KEY_VALUE_NAMESPACE
        )

        # Test for Collision
        namespace_keys = {
            const.METRICS_KEY_VALUE_NAMESPACE,
            const.POLLING_PLUGIN_AGENT_KEY_VALUE_NAMESPACE,
            const.DISCOVERY_PLUGIN_AGENT_KEY_VALUE_NAMESPACE,
            const.ENRICHMENT_PLUGIN_AGENT_KEY_VALUE_NAMESPACE,
            const.PLUGINS_KEY_VALUE_NAMESPACE
        }

        self.assertEqual(len(namespace_keys), 5)


class TestDiscoveryPluginAgent(PluginAgent):

    @patch(u'yahoo_panoptes.framework.const.DEFAULT_CONFIG_FILE_PATH', global_panoptes_test_conf_file)
    def test_discovery_agent_context(self):

        discovery_agent_context = PanoptesDiscoveryAgentContext()

        self.assertEqual(discovery_agent_context.kv_stores, {})

        with self.assertRaises(AttributeError):
            discovery_agent_context.zookeeper_client

        with self.assertRaises(AttributeError):
            discovery_agent_context.kafka_client

    @patch(u'yahoo_panoptes.discovery.discovery_plugin_agent.PanoptesDiscoveryTaskContext')
    def test_discovery_agent_exits(self, task_context):

        task_context.side_effect = Exception()
        with self.assertRaises(SystemExit):
            discovery_plugin_task(u'Test')

    @patch(u'yahoo_panoptes.framework.const.DEFAULT_CONFIG_FILE_PATH', global_panoptes_test_conf_file)
    def test_start_discovery_plugin_agent(self):

        # Assert Nothing Throws
        start_discovery_plugin_agent()

        with patch(u'yahoo_panoptes.discovery.discovery_plugin_agent.PanoptesDiscoveryAgentContext') as c:
            c.side_effect = Exception()
            with self.assertRaises(SystemExit):
                start_discovery_plugin_agent()

        with patch(u'yahoo_panoptes.discovery.discovery_plugin_agent.PanoptesCeleryInstance') as c:
            c.side_effect = Exception(u'Fatal Error')
            with self.assertRaises(SystemExit):
                start_discovery_plugin_agent()

    @patch(u'yahoo_panoptes.discovery.discovery_plugin_agent.panoptes_context')
    def test_polling_shutdown_signal_handler(self, panoptes_context):

        # Test Shutdown Notifies Kafka Producer
        panoptes_context.return_value = self._panoptes_context

        discovery_agent_shutdown({})
        panoptes_context.message_producer.stop.assert_called_once()

        # Test Exception
        panoptes_context.message_producer.stop.side_effect = Exception()
        discovery_agent_shutdown({})

        panoptes_context.logger.error.assert_called_once_with(u'Could not shutdown message producer: ')


class TestEnrichmentPluginAgent(PluginAgent):

    @patch(u'yahoo_panoptes.framework.const.DEFAULT_CONFIG_FILE_PATH', global_panoptes_test_conf_file)
    def test_start_enrichment_plugin_agent(self):

        # Assert Nothing Throws
        start_enrichment_plugin_agent()

        with patch(u'yahoo_panoptes.enrichment.enrichment_plugin_agent.PanoptesEnrichmentAgentContext') as c:
            c.side_effect = Exception()
            with self.assertRaises(SystemExit):
                start_enrichment_plugin_agent()

        with patch(u'yahoo_panoptes.enrichment.enrichment_plugin_agent.PanoptesCeleryInstance') as c:
            c.side_effect = Exception()
            with self.assertRaises(SystemExit):
                start_enrichment_plugin_agent()


class TestPollingPluginAgent(PluginAgent):

    @patch(u'yahoo_panoptes.framework.context.PanoptesContext.get_kv_store')
    @patch(u'yahoo_panoptes.framework.context.PanoptesContext._get_message_producer')
    @patch(u'yahoo_panoptes.framework.context.PanoptesContext.message_producer', new_callable=PropertyMock)
    def test_polling_transform_converts_correct_counters(self, message_producer, message_producer_property, kv_store):

        mock_panoptes_plugin = MockPanoptesObject()
        mock_panoptes_plugin.config = ConfigObj(pwd + u'/config_files/test_panoptes_polling_plugin_conf.ini')
        kv_store.return_value = PanoptesMockRedis()

        mock_message_producer = MockPanoptesMessageProducer()

        message_producer.return_value = mock_message_producer
        message_producer_property.return_value = message_producer_property

        panoptes_context = PanoptesContext(config_file=os.path.join(path, u'config_files/test_panoptes_config.ini'))

        for i in range(1, 9):
            panoptes_metrics = self.prepare_panoptes_metrics_group_set(
                u'{}/metric_group_sets/interface_plugin_counter_{}.json'.format(pwd, i))
            _process_metrics_group_set(context=panoptes_context, results=panoptes_metrics, plugin=mock_panoptes_plugin)

        published_kafka_messages = panoptes_context.message_producer.messages

        expected_results = [
            {  # Test Conversion
                u'counter|test_system_uptime': 0,
                u'counter|extra_test_metric': 0
            },
            {  # Test Conversion
                u'counter|test_system_uptime': 60,
                u'gauge|test_system_uptime': 0,
                u'counter|extra_test_metric': 120,
                u'gauge|extra_test_metric': 1
            },
            {  # Test Conversion
                u'counter|test_system_uptime': 120,
                u'gauge|test_system_uptime': 0,
                u'counter|extra_test_metric': 240,
                u'gauge|extra_test_metric': 1
            },
            {  # Test time difference is negative
                u'counter|test_system_uptime': 120,
                u'gauge|test_system_uptime': 0,
                u'counter|extra_test_metric': 240,
                u'gauge|extra_test_metric': 1
            },
            {  # Test time difference is zero
                u'counter|test_system_uptime': 120,
                u'gauge|test_system_uptime': 0,
                u'counter|extra_test_metric': 240,
                u'gauge|extra_test_metric': 1
            },
            {  # Test Time difference is greater than TTL multiple
                u'counter|test_system_uptime': 500,
                u'gauge|test_system_uptime': 0,
                u'counter|extra_test_metric': 1000,
                u'gauge|extra_test_metric': 1
            },
            {
                # Confidence below the const.METRICS_CONFIDENCE_THRESHOLD
                # ( time_difference > (metrics_group.interval * const.METRICS_KV_STORE_TTL_MULTIPLE )
                u'counter|test_system_uptime': 500,
                u'gauge|test_system_uptime': 0,
                u'counter|extra_test_metric': 1000,
                u'gauge|extra_test_metric': 0
            },
            {
                # Skip Conversion due to the new counter value
                # being less than the previous value
                u'counter|test_system_uptime': 400,
                u'gauge|test_system_uptime': 1,
                u'counter|extra_test_metric': 900,
                u'gauge|extra_test_metric': 1
            }
        ]

        for i, kafka_message in enumerate(published_kafka_messages):
            parsed_json = json.loads(kafka_message[u'message'])[u'metrics']

            for panoptes_metric in parsed_json:
                key = u"|".join([panoptes_metric[u'metric_type'], panoptes_metric[u'metric_name']])

                self.assertEquals(
                    expected_results[i].get(key, None),
                    panoptes_metric[u'metric_value']
                )

    @patch(u'yahoo_panoptes.framework.context.PanoptesContext.get_kv_store')
    @patch(u'yahoo_panoptes.framework.context.PanoptesContext._get_message_producer')
    @patch(u'yahoo_panoptes.framework.context.PanoptesContext.message_producer', new_callable=PropertyMock)
    def test_polling_no_matching_transform_type(self, message_producer, message_producer_property, kv_store):

        mock_panoptes_plugin = MockPanoptesObject()
        mock_panoptes_plugin.config = ConfigObj(
            pwd + u'/config_files/test_panoptes_polling_plugin_conf_diff_transform_callback.ini')
        kv_store.return_value = PanoptesMockRedis()

        mock_message_producer = MockPanoptesMessageProducer()

        message_producer.return_value = mock_message_producer
        message_producer_property.return_value = message_producer_property

        panoptes_context = PanoptesContext(config_file=os.path.join(path, u'config_files/test_panoptes_config.ini'))

        for i in range(1, 9):
            panoptes_metrics = self.prepare_panoptes_metrics_group_set(
                u'{}/metric_group_sets/interface_plugin_counter_{}.json'.format(pwd, i))
            _process_metrics_group_set(context=panoptes_context, results=panoptes_metrics, plugin=mock_panoptes_plugin)

        self.assertEqual(
            panoptes_context.message_producer.messages,
            []
        )

    @patch(u'yahoo_panoptes.framework.context.PanoptesContext._get_message_producer')
    @patch(u'yahoo_panoptes.framework.context.PanoptesContext.message_producer', new_callable=PropertyMock)
    def test_polling_kafka_produces_to_the_correct_topics(self, message_producer, message_producer_property):
        spec_paths = [
            u'config_files/test_panoptes_config.ini',  # Produces to site topics but not Global
            u'config_files/test_panoptes_config_kafka_true.ini'  # Sends enrichment results to both site & global topics
        ]

        expected_results = [
            {u'test_site-processed': 1},
            {u'panoptes-metrics': 1, u'test_site-processed': 1}
        ]

        all_tests_pass = True

        for i in range(len(spec_paths)):
            mock_panoptes_plugin = MockPanoptesObject()
            mock_panoptes_plugin.config = ConfigObj(pwd + u'/config_files/test_panoptes_polling_plugin_conf.ini')

            mock_message_producer = MockPanoptesMessageProducer()

            message_producer.return_value = mock_message_producer
            message_producer_property.return_value = message_producer_property

            panoptes_context = PanoptesContext(config_file=os.path.join(path, spec_paths[i]))

            metrics_group_set = self.prepare_panoptes_metrics_group_set()

            _process_metrics_group_set(context=panoptes_context, results=metrics_group_set, plugin=mock_panoptes_plugin)

            published_kafka_messages = panoptes_context.message_producer.messages

            actual_result = {}

            for message in published_kafka_messages:
                if message[u'topic'] in actual_result:
                    actual_result[message[u'topic']] += 1
                else:
                    actual_result[message[u'topic']] = 1

            all_tests_pass &= (actual_result == expected_results[i])

        self.assertTrue(all_tests_pass)

    @patch(u'yahoo_panoptes.framework.const.DEFAULT_CONFIG_FILE_PATH', global_panoptes_test_conf_file)
    def test_polling_agent_context(self):

        panoptes_polling_context = PanoptesPollingAgentContext()

        self.assertEqual(panoptes_polling_context.kv_stores, {})

        with self.assertRaises(AttributeError):
            panoptes_polling_context.zookeeper_client

        with self.assertRaises(AttributeError):
            panoptes_polling_context.kafka_client

    @patch(u'yahoo_panoptes.framework.const.DEFAULT_CONFIG_FILE_PATH', global_panoptes_test_conf_file)
    def test_polling_agent_tasks_exit(self):

        with patch(u'yahoo_panoptes.polling.polling_plugin_agent.PanoptesPollingAgentContext') as context:
            context.side_effect = Exception()

            with self.assertRaises(SystemExit):
                start_polling_plugin_agent()

        with patch(u'yahoo_panoptes.polling.polling_plugin_agent.PanoptesPollingTaskContext') as context:
            context.side_effect = Exception()

            with self.assertRaises(SystemExit):
                polling_plugin_task(u'Test Polling Plugin', u'polling')

    @patch(u'yahoo_panoptes.framework.const.DEFAULT_CONFIG_FILE_PATH', global_panoptes_test_conf_file)
    def test_start_polling_agent(self):

        # Assert Nothing Throws
        start_polling_plugin_agent()

        with patch(u'yahoo_panoptes.polling.polling_plugin_agent.PanoptesCeleryInstance') as c:
            c.side_effect = Exception(u'Fatal Error')
            with self.assertRaises(SystemExit):
                start_polling_plugin_agent()

    @patch(u'yahoo_panoptes.polling.polling_plugin_agent.panoptes_polling_task_context')
    def test_polling_shutdown_signal_handler(self, task_context):

        # Test Shutdown Notifies Kafka Producer
        polling_agent_shutdown({})
        task_context.message_producer.stop.assert_called_once()

        # Test Exception
        task_context.message_producer.stop.side_effect = Exception()
        polling_agent_shutdown({})
        task_context.logger.error.assert_called_once_with(u'Could not shutdown message producer: Exception()')
