import json
import logging
import os
import unittest

import requests_mock
from influxdb import InfluxDBClient
from mock import patch, Mock

from tests.mock_panoptes_consumer import MockPanoptesConsumer, mock_get_client_id
from tests.mock_requests_responses import *
from tests.test_framework import PanoptesMockRedis
from yahoo_panoptes.consumers.influxdb.consumer import PanoptesInfluxDBConsumer
from yahoo_panoptes.framework.context import PanoptesContext, PanoptesContextError

MockPanoptesContextWithException = Mock(side_effect=PanoptesContextError())
MockRequestsSessionWithException = Mock(side_effect=Exception())


class MockPanoptesContext(PanoptesContext):
    @patch('redis.StrictRedis', PanoptesMockRedis)
    def __init__(self, config_file='tests/config_files/test_panoptes_config.ini'):
        super(MockPanoptesContext, self).__init__(
            key_value_store_class_list=[],
            create_zookeeper_client=False,
            config_file=config_file,
        )

    @property
    def logger(self):
        return logging.getLogger()


class MockRequestsSession(object):
    def __init__(self):
        self._bad_request_count = 0

    def Session(self):
        return self

    def post(self, **kwargs):
        data = json.loads(kwargs['data'])
        if (self._bad_request_count < 1) and ('memory_used' in data['metrics']):
            self._bad_request_count += 1
            return MockRequestsResponseBadRequest()
        elif 'packets_input' in data['metrics']:
            return MockRequestsResponseServerFailure()
        else:
            return MockRequestsResponseOK()


class MockInfluxDBClient(InfluxDBClient):
    def create_database(self, dbname):
        return True

    def get_list_database(self):
        return [{u'name': u'db1'}, {u'name': u'db2'}, {u'name': u'db3'}]

    def ping(self):
        return '1.6.2'


class MockBadPingInfluxDBClient(MockInfluxDBClient):
    def ping(self):
        pass


class TestPanoptesInfluxDBConsumer(unittest.TestCase):
    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesConsumer', MockPanoptesConsumer)
    @patch('redis.StrictRedis', PanoptesMockRedis)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesInfluxDBConsumerContext', MockPanoptesContext)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.get_client_id', mock_get_client_id)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesInfluxDBConnection', MockInfluxDBClient)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.DEFAULT_CONFIG_FILE',
           'tests/consumers/influxdb/conf/influxdb_consumer.ini')
    def test_panoptes_influxdb_consumer(self):
        """Default Test"""

        output_data_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output/influx_line00.data')

        output_data = open(output_data_file).read()

        MockPanoptesConsumer.files = [
            'consumers/influxdb/input/metrics_group00.json',
            'consumers/influxdb/input/metrics_group01.json',
            'consumers/influxdb/input/metrics_group02.json'
        ]

        # Testing the presented data matches expectations
        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST, "http://localhost:8086/write", status_code=204)
            PanoptesInfluxDBConsumer.factory()
            self.assertEquals(m.last_request.body, output_data)

        # Testing a bad ini file.
        with patch('yahoo_panoptes.consumers.influxdb.consumer.DEFAULT_CONFIG_FILE',
                   'tests/consumers/influxdb/conf/bad_influxdb_consumer.ini'):
            with self.assertRaises(SystemExit):
                PanoptesInfluxDBConsumer.factory()

    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesConsumer', MockPanoptesConsumer)
    @patch('redis.StrictRedis', PanoptesMockRedis)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesInfluxDBConsumerContext',
           MockPanoptesContextWithException)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.get_client_id', mock_get_client_id)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesInfluxDBConnection', MockInfluxDBClient)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.DEFAULT_CONFIG_FILE',
           'tests/consumers/influxdb/conf/influxdb_consumer.ini')
    def test_panoptes_influxdb_consumer02(self):
        """Test with bad PanoptesContext"""
        with self.assertRaises(SystemExit):
            PanoptesInfluxDBConsumer.factory()

    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesConsumer', MockPanoptesConsumer)
    @patch('redis.StrictRedis', PanoptesMockRedis)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesInfluxDBConsumerContext', MockPanoptesContext)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.get_client_id', mock_get_client_id)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesInfluxDBConnection', MockBadPingInfluxDBClient)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.DEFAULT_CONFIG_FILE',
           'tests/consumers/influxdb/conf/influxdb_consumer.ini')
    def test_failed_ping(self):
        """Tests a failed ping to the consumer"""
        with self.assertRaises(SystemExit):
            PanoptesInfluxDBConsumer.factory()

    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesConsumer', MockPanoptesConsumer)
    @patch('redis.StrictRedis', PanoptesMockRedis)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesInfluxDBConsumerContext', MockPanoptesContext)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.get_client_id', mock_get_client_id)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesInfluxDBConnection', MockInfluxDBClient)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.DEFAULT_CONFIG_FILE',
           'tests/consumers/influxdb/conf/influxdb_consumer.ini')
    @patch('yahoo_panoptes.consumers.influxdb.consumer.requests.Session')
    def test_panoptes_influxdb_one_by_one(self):
        """Test with bad PanoptesContext"""
        with self.assertRaises(SystemExit):
            PanoptesInfluxDBConsumer.factory()
