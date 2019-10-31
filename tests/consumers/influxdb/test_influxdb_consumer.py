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


class MockInfluxDBConnection(InfluxDBClient):
    def create_database(self, dbname):
        return True

    def get_list_database(self):
        return [{u'name': u'db1'}, {u'name': u'db2'}, {u'name': u'db3'}]

    def ping(self):
        return u'1.6.2'


class TestPanoptesInfluxDBConsumer(unittest.TestCase):
    def test_influxdb_consumer_parser_exception(self):
        mock_argument_parser = Mock()
        attributes = {u'parse_known_args.side_effect': Exception}
        mock_argument_parser.configure_mock(**attributes)
        with patch('yahoo_panoptes.consumers.influxdb.consumer.argparse.ArgumentParser', mock_argument_parser):
            with self.assertRaises(SystemExit):
                PanoptesInfluxDBConsumer.factory()
                unittest.main(exit=False)

    def test_influxdb_consumer_bad_configuration_file(self):
        with self.assertRaises(SystemExit):
            PanoptesInfluxDBConsumer('non.existent.config.file')
            unittest.main(exit=False)

    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesInfluxDBConnection', MockInfluxDBConnection)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.DEFAULT_CONFIG_FILE',
           'tests/consumers/influxdb/conf/influxdb_consumer.ini')
    def test_panoptes_influxdb_consumer_bad_context(self):
        """Test with bad PanoptesContext"""
        with self.assertRaises(SystemExit):
            PanoptesInfluxDBConsumer.factory()
            unittest.main(exit=False)

    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesConsumer', MockPanoptesConsumer)
    @patch('redis.StrictRedis', PanoptesMockRedis)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesInfluxDBConsumerContext', MockPanoptesContext)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.get_client_id', mock_get_client_id)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesInfluxDBConnection', MockInfluxDBConnection)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.DEFAULT_CONFIG_FILE',
           'tests/consumers/influxdb/conf/influxdb_consumer.ini')
    def test_panoptes_influxdb_connection_ping_exception(self):
        with patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesInfluxDBConnection.ping',
                   Mock(side_effect=Exception)):
            with self.assertRaises(SystemExit):
                PanoptesInfluxDBConsumer.factory()
                unittest.main(exit=False)

    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesConsumer', MockPanoptesConsumer)
    @patch('redis.StrictRedis', PanoptesMockRedis)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesInfluxDBConsumerContext', MockPanoptesContext)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.get_client_id', mock_get_client_id)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesInfluxDBConnection', MockInfluxDBConnection)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.DEFAULT_CONFIG_FILE',
           'tests/consumers/influxdb/conf/influxdb_consumer.ini')
    def test_panoptes_influxdb_connection_create_database_exception(self):
        with patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesInfluxDBConnection.create_database',
                   Mock(side_effect=Exception)):
            with self.assertRaises(SystemExit):
                PanoptesInfluxDBConsumer.factory()
                unittest.main(exit=False)

    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesConsumer', MockPanoptesConsumer)
    @patch('redis.StrictRedis', PanoptesMockRedis)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesInfluxDBConsumerContext', MockPanoptesContext)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.get_client_id', mock_get_client_id)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.PanoptesInfluxDBConnection', MockInfluxDBConnection)
    @patch('yahoo_panoptes.consumers.influxdb.consumer.DEFAULT_CONFIG_FILE',
           'tests/consumers/influxdb/conf/influxdb_consumer.ini')
    def test_panoptes_influxdb_consumer(self):
        """Test sending metrics through the InfluxDB client"""

        output_data_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), u'output/influx_line00.data')

        output_data = open(output_data_file).read()

        MockPanoptesConsumer.files = [
            u'consumers/influxdb/input/metrics_group00.json',
        ]

        with requests_mock.Mocker() as m:
            m.register_uri('POST', "http://127.0.0.1:8086/write", status_code=204)
            PanoptesInfluxDBConsumer.factory()
            self.assertEquals(m.last_request.body.decode("utf-8"), output_data)

        # Fail on first write to try _send_one_by_one
        with requests_mock.Mocker() as m:
            m.register_uri('POST', "http://127.0.0.1:8086/write", response_list=[
                {u'status_code': 400}, {u'status_code': 204}])
            PanoptesInfluxDBConsumer.factory()
            self.assertEquals(m.last_request.body.decode("utf-8"), output_data)

        # TODO: Test sending points in a single batch
