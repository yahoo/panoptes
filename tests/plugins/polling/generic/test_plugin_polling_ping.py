import unittest
import logging
import json
from mock import Mock, patch, create_autospec

from yahoo_panoptes.framework.utilities.helpers import ordered
from yahoo_panoptes.framework.context import PanoptesContext
from yahoo_panoptes.framework.resources import PanoptesResource
from yahoo_panoptes.framework.plugins.context import PanoptesPluginWithEnrichmentContext
from yahoo_panoptes.polling.polling_plugin import PanoptesPollingPluginConfigurationError
from yahoo_panoptes.plugins.polling.generic.plugin_polling_ping import PluginPollingPing

mock_time = Mock()
mock_time.return_value = 1512629517.03121

TEST_PING_RESPONSE_SUCCESS = u"ping statistics ---\n" \
                     u"10 packets transmitted, 10 received, 0% packet loss, time 1439ms\n" \
                     u"rtt min/avg/max/mdev = 0.040/0.120/0.162/0.057 ms"

TEST_PING_RESPONSE_FAILURE = u"ping statistics ---\n" \
                     u"10 packets transmitted, 0 received, 100% packet loss, time 10000ms\n" \
                     u"rtt min/avg/max/mdev = 0.0/0.0/0.0/0.0 ms"


TEST_PLUGIN_RESULT_EXCEPTION = {
    u"resource": {
        u"resource_site": u"test_site",
        u"resource_class": u"test_class",
        u"resource_subclass": u"test_subclass",
        u"resource_type": u"test_type",
        u"resource_id": u"test_id",
        u"resource_endpoint": u"test_endpoint",
        u"resource_metadata": {
            u"_resource_ttl": u"604800"
        },
        u"resource_creation_timestamp": 1512629517.03121,
        u"resource_plugin": u"test_plugin"},
    u"dimensions": [],
    u"metrics": [
        {
            u"metric_creation_timestamp": 1512629517.031,
            u"metric_type": u"gauge",
            u"metric_name": u"ping_status",
            u"metric_value": 7
        }
    ],
    u"metrics_group_type": u"ping",
    u"metrics_group_interval": 60,
    u"metrics_group_creation_timestamp": 1512629517.031,
    u"metrics_group_schema_version": u"0.2"
}

TEST_PLUGIN_RESULT_FAILURE = {
    u"resource": {
        u"resource_site": u"test_site",
        u"resource_class": u"test_class",
        u"resource_subclass": u"test_subclass",
        u"resource_type": u"test_type",
        u"resource_id": u"test_id",
        u"resource_endpoint": u"test_endpoint",
        u"resource_metadata": {
            u"_resource_ttl": u"604800"
        },
        u"resource_creation_timestamp": 1512629517.03121,
        u"resource_plugin": u"test_plugin"},
    u"dimensions": [],
    u"metrics": [
        {
            u"metric_creation_timestamp": 1512629517.031,
            u"metric_type": u"gauge",
            u"metric_name": u"ping_status",
            u"metric_value": 7
        },
        {
            u"metric_creation_timestamp": 1512629517.031,
            u"metric_type": u"gauge",
            u"metric_name": u"packet_loss_percent",
            u"metric_value": 100
        },
        {
            u"metric_creation_timestamp": 1512629517.031,
            u"metric_type": u"gauge",
            u"metric_name": u"round_trip_minimum",
            u"metric_value": 0
        },
        {
            u"metric_creation_timestamp": 1512629517.031,
            u"metric_type": u"gauge",
            u"metric_name": u"round_trip_average",
            u"metric_value": 0
        },
        {
            u"metric_creation_timestamp": 1512629517.031,
            u"metric_type": u"gauge",
            u"metric_name": u"round_trip_maximum",
            u"metric_value": 0
        },
        {
            u"metric_creation_timestamp": 1512629517.031,
            u"metric_type": u"gauge",
            u"metric_name": u"round_trip_standard_deviation",
            u"metric_value": 0
        }
    ],
    u"metrics_group_type": u"ping",
    u"metrics_group_interval": 60,
    u"metrics_group_creation_timestamp": 1512629517.031,
    u"metrics_group_schema_version": u"0.2"
}


TEST_PLUGIN_RESULT_SUCCESS = {
    u"resource": {
        u"resource_site": u"test_site",
        u"resource_class": u"test_class",
        u"resource_subclass": u"test_subclass",
        u"resource_type": u"test_type",
        u"resource_id": u"test_id",
        u"resource_endpoint": u"test_endpoint",
        u"resource_metadata": {
            u"_resource_ttl": u"604800"
        },
        u"resource_creation_timestamp": 1512629517.03121,
        u"resource_plugin": u"test_plugin"},
    u"dimensions": [],
    u"metrics": [
        {
            u"metric_creation_timestamp": 1512629517.031,
            u"metric_type": u"gauge",
            u"metric_name": u"ping_status",
            u"metric_value": 0
        },
        {
            u"metric_creation_timestamp": 1512629517.031,
            u"metric_type": u"gauge",
            u"metric_name": u"packet_loss_percent",
            u"metric_value": 0
        },
        {
            u"metric_creation_timestamp": 1512629517.031,
            u"metric_type": u"gauge",
            u"metric_name": u"round_trip_minimum",
            u"metric_value": 0.040
        },
        {
            u"metric_creation_timestamp": 1512629517.031,
            u"metric_type": u"gauge",
            u"metric_name": u"round_trip_average",
            u"metric_value": 0.120
        },
        {
            u"metric_creation_timestamp": 1512629517.031,
            u"metric_type": u"gauge",
            u"metric_name": u"round_trip_maximum",
            u"metric_value": 0.162
        },
        {
            u"metric_creation_timestamp": 1512629517.031,
            u"metric_type": u"gauge",
            u"metric_name": u"round_trip_standard_deviation",
            u"metric_value": 0.057
        }
    ],
    u"metrics_group_type": u"ping",
    u"metrics_group_interval": 60,
    u"metrics_group_creation_timestamp": 1512629517.031,
    u"metrics_group_schema_version": u"0.2"
}


class TestPluginPollingPing(unittest.TestCase):
    @patch(u'yahoo_panoptes.framework.resources.time', mock_time)
    def setUp(self):
        self._panoptes_resource = PanoptesResource(
                resource_site=u'test_site',
                resource_class=u'test_class',
                resource_subclass=u'test_subclass',
                resource_type=u'test_type',
                resource_id=u'test_id',
                resource_endpoint=u'test_endpoint',
                resource_plugin=u'test_plugin'
        )

        self._plugin_config = {
            u'Core': {
                u'name': u'Test Plugin',
                u'module': u'plugin_polling_ping'
            },
            u'main': {
                u'resource_filter': u'resource_class = u"network"',
                u'execute_frequency': 60,
            }
        }

        self._panoptes_context = create_autospec(PanoptesContext)

        self._panoptes_plugin_context = create_autospec(
                PanoptesPluginWithEnrichmentContext,
                instance=True, spec_set=True,
                data=self._panoptes_resource,
                config=self._plugin_config,
                logger=logging.getLogger(__name__)
        )

    @patch(u'yahoo_panoptes.framework.metrics.time', mock_time)
    @patch(u'yahoo_panoptes.framework.utilities.ping.subprocess.check_output',
           Mock(return_value=TEST_PING_RESPONSE_SUCCESS))
    def test_plugin_ping_success(self):
        results = PluginPollingPing().run(self._panoptes_plugin_context)
        self.assertEqual(ordered(json.loads(list(results)[0].json)), ordered(TEST_PLUGIN_RESULT_SUCCESS))

    @patch(u'yahoo_panoptes.framework.metrics.time', mock_time)
    @patch(u'yahoo_panoptes.framework.utilities.ping.subprocess.check_output',
           Mock(return_value=TEST_PING_RESPONSE_FAILURE))
    def test_plugin_ping_failure(self):
        results = PluginPollingPing().run(self._panoptes_plugin_context)

        self.assertEqual(ordered(json.loads(list(results)[0].json)), ordered(TEST_PLUGIN_RESULT_FAILURE))

    @patch(u'yahoo_panoptes.framework.metrics.time', mock_time)
    @patch(u'yahoo_panoptes.framework.utilities.ping.subprocess.check_output',
           Mock(side_effect=Exception))
    def test_plugin_ping_exception(self):
        results = PluginPollingPing().run(self._panoptes_plugin_context)
        self.assertEqual(ordered(json.loads(list(results)[0].json)), ordered(TEST_PLUGIN_RESULT_EXCEPTION))

    @patch(u'yahoo_panoptes.framework.metrics.time', mock_time)
    def test_plugin_ping_count_error(self):
        self._plugin_config = {
            u'Core': {
                u'name': u'Test Plugin',
                u'module': u'plugin_polling_ping'
            },
            u'main': {
                u'resource_filter': u'resource_class = u"network"',
                u'execute_frequency': 60,
                u'count': "string"
            }
        }

        self._panoptes_context = create_autospec(PanoptesContext)

        self._panoptes_plugin_context = create_autospec(
            PanoptesPluginWithEnrichmentContext,
            instance=True, spec_set=True,
            data=self._panoptes_resource,
            config=self._plugin_config,
            logger=logging.getLogger(__name__)
        )

        with self.assertRaises(PanoptesPollingPluginConfigurationError):
            results = PluginPollingPing().run(self._panoptes_plugin_context)

    @patch(u'yahoo_panoptes.framework.metrics.time', mock_time)
    def test_plugin_ping_timeout_error(self):
        self._plugin_config = {
            u'Core': {
                u'name': u'Test Plugin',
                u'module': u'plugin_polling_ping'
            },
            u'main': {
                u'resource_filter': u'resource_class = u"network"',
                u'execute_frequency': 60,
                u'timeout': "string"
            }
        }

        self._panoptes_context = create_autospec(PanoptesContext)

        self._panoptes_plugin_context = create_autospec(
            PanoptesPluginWithEnrichmentContext,
            instance=True, spec_set=True,
            data=self._panoptes_resource,
            config=self._plugin_config,
            logger=logging.getLogger(__name__)
        )

        with self.assertRaises(PanoptesPollingPluginConfigurationError):
            results = PluginPollingPing().run(self._panoptes_plugin_context)
