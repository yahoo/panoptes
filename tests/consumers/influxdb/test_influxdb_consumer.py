import logging
import unittest
import os
import requests_mock

from mock import patch, Mock


from yahoo_panoptes.consumers.influxdb.consumer import PanoptesInfluxDBConsumer
from yahoo_panoptes.framework.context import PanoptesContext, PanoptesContextError
from tests.test_framework import PanoptesMockRedis
from tests.mock_panoptes_consumer import MockPanoptesConsumer, mock_get_client_id
from influxdb import InfluxDBClient


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


MockPanoptesContextWithException = Mock(side_effect=PanoptesContextError())


class MockInfluxDBClient(InfluxDBClient):
    def create_database(self, dbname):
        return True

    def get_list_database(self):
        return [{u'name': u'db1'}, {u'name': u'db2'}, {u'name': u'db3'}]

    def ping(self):
        return '1.6.2'


class TestPanoptesInfluxDBConsumer(unittest.TestCase):
    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesConsumer', MockPanoptesConsumer)
    @patch('redis.StrictRedis', PanoptesMockRedis)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesInfluxDBConsumerContext', MockPanoptesContext)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.get_client_id', mock_get_client_id)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesInfluxDBConnection', MockInfluxDBClient)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.DEFAULT_CONFIG_FILE',
           'tests/consumers/influxdb/conf/influxdb_consumer.ini')
    def test_panoptes_influxdb_consumer(self):
        "Default Test"

        output_data_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output/influx_line00.data')

        output_data = open(output_data_file).read()

        MockPanoptesConsumer.files = [
            'consumers/influxdb/input/metrics_group00.json',
            'consumers/influxdb/input/metrics_group01.json',
            'consumers/influxdb/input/metrics_group02.json'
        ]

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST, "http://localhost:8086/write", status_code=204)

            PanoptesInfluxDBConsumer.factory()

            self.assertEquals(m.last_request.body, output_data)

    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesConsumer', MockPanoptesConsumer)
    @patch('redis.StrictRedis', PanoptesMockRedis)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesInfluxDBConsumerContext', MockPanoptesContextWithException)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.get_client_id', mock_get_client_id)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesInfluxDBConnection', MockInfluxDBClient)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.DEFAULT_CONFIG_FILE',
           'tests/consumers/influxdb/conf/influxdb_consumer.ini')
    def test_panoptes_influxdb_consumer02(self):
        """Test with bad PanoptesContext"""
        with self.assertRaises(SystemExit):
            PanoptesInfluxDBConsumer.factory()
