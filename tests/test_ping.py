"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""

import unittest
import logging
import requests
import subprocess
import json  # why isn't the call below recognized?

from mock import patch, Mock
from mock import create_autospec
from requests import Session

from tests.test_snmp_connections import panoptes_context, secret_store

from yahoo_panoptes.framework.utilities.ping import *
from yahoo_panoptes.framework.resources import PanoptesResource
from yahoo_panoptes.framework.plugins.context import PanoptesPluginWithEnrichmentContext

# For testing locally on OSX
TEST_PING_RESPONSE = "ping statistics ---\n" \
                     "3 packets transmitted, 3 received, 0% packet loss, time 1439ms\n" \
                     "rtt min/avg/max/mdev = 0.040/0.120/0.162/0.057 ms"


class MockConnection:
    def __init__(self):
        self._cert = ()

    @property
    def cert(self):
        return self.cert

    @cert.setter
    def cert(self, tuple):
        self._cert = tuple

    def post(self, *args, **kwargs):
        return MockHTTPReponse()


class MockHTTPReponse:
    def __init__(self):
        self._resp = {
            "host": "fw",
            "error": False,
            "error_message": None,
            "packets_transmitted": 10,
            "packets_received": 10,
            "packet_loss_pct": 0.0,
            "execution_time": 9.03,
            "round_trip_min": 0.485,
            "round_trip_avg": 0.504,
            "round_trip_max": 0.535,
            "round_trip_stddev": 0.027
        }

    def raise_for_status(self):
        pass

    def json(self):
        return self._resp


class TestPing(unittest.TestCase):

    def setUp(self):

        self._resource_dict = {
            'resource_plugin': 'test',
            'resource_site': 'test',
            'resource_class': 'test',
            'resource_subclass': 'test',
            'resource_type': 'test',
            'resource_id': 'localhost',
            'resource_endpoint': 'localhost',
            'resource_creation_timestamp': 0,
            'resource_metadata': {}
        }

    def test_code_output(self):
        # Mock the system calls
        subprocess.check_output = Mock(return_value=TEST_PING_RESPONSE)
        resource = PanoptesResource.resource_from_dict(self._resource_dict)
        test_ping = PanoptesPingConnectionFactory.get_ping_connection(resource=resource,
                                                                      context=None,
                                                                      count=3,
                                                                      timeout=100)
        test_d = {"round_trip_stddev": 0.057, "packets_received": 3, "execution_time": 1.44,
                  "round_trip_avg": 0.120, "packets_transmitted": 3, "packet_loss_pct": 0.0,
                  "round_trip_max": 0.162, "round_trip_min": 0.040}

        self.assertDictEqual(test_d, json.loads(test_ping.response))
        self.assertEqual(test_ping.packets_transmitted, 3)
        self.assertEqual(test_ping.packets_received, 3)
        self.assertEqual(test_ping.packet_loss_pct, 0.0)
        self.assertEqual(test_ping.execution_time, 1.44)  # add for testing prod
        self.assertEqual(test_ping.round_trip_min, 0.040)
        self.assertEqual(test_ping.round_trip_avg, 0.120)
        self.assertEqual(test_ping.round_trip_max, 0.162)
        self.assertEqual(test_ping.round_trip_stddev, 0.057)

        # Test bad parameters
        with self.assertRaises(AssertionError):
            PanoptesPingConnectionFactory.get_ping_connection(resource=resource,
                                                              context=None,
                                                              count=-1,
                                                              timeout=100)
        with self.assertRaises(AssertionError):
            PanoptesPingConnectionFactory.get_ping_connection(resource=resource,
                                                              context=None,
                                                              count='1',
                                                              timeout=100)
        with self.assertRaises(AssertionError):
            PanoptesPingConnectionFactory.get_ping_connection(resource=None,
                                                              context=None,
                                                              count=3,
                                                              timeout=100)
        with self.assertRaises(AssertionError):
            self._resource_dict['resource_endpoint'] = ''
            resource = PanoptesResource.resource_from_dict(self._resource_dict)
            PanoptesPingConnectionFactory.get_ping_connection(resource=resource,
                                                              context=None,
                                                              count=3,
                                                              timeout=100)
        with self.assertRaises(AssertionError):
            self._resource_dict['resource_endpoint'] = 'localhost'
            resource = PanoptesResource.resource_from_dict(self._resource_dict)
            PanoptesPingConnectionFactory.get_ping_connection(resource=resource,
                                                              context=None,
                                                              count=3,
                                                              timeout=0)
        with self.assertRaises(AssertionError):
            PanoptesPingConnectionFactory.get_ping_connection(resource=resource,
                                                              context=None,
                                                              count=3,
                                                              timeout=-1)

    def test_ping_timeout(self):
        e = subprocess.CalledProcessError(returncode=None, cmd=None, output=TEST_PING_RESPONSE)
        #  When I try: "TypeError: exceptions must be old-style classes or derived from BaseException, not Mock"
        subprocess.check_output = Mock(side_effect=e)
        test_d = {"round_trip_stddev": 0.057, "packets_received": 3, "execution_time": 1.44,
                  "round_trip_avg": 0.120, "packets_transmitted": 3, "packet_loss_pct": 0.0,
                  "round_trip_max": 0.162, "round_trip_min": 0.040}

        with self.assertRaises(PanoptesPingTimeoutException):
            resource = PanoptesResource.resource_from_dict(self._resource_dict)
            test_timeout_ping = PanoptesPingConnectionFactory.get_ping_connection(resource=resource,
                                                                                  context=None,
                                                                                  count=3,
                                                                                  timeout=100)
            self.assertDictEqual(test_d, json.loads(test_timeout_ping.response))
            self.assertEqual(test_timeout_ping.packets_transmitted, 3)
            self.assertEqual(test_timeout_ping.packets_received, 3)
            self.assertEqual(test_timeout_ping.execution_time, 1.44)
            self.assertEqual(test_timeout_ping.round_trip_min, 0.040)
            self.assertEqual(test_timeout_ping.round_trip_avg, 0.120)
            self.assertEqual(test_timeout_ping.round_trip_max, 0.162)
            self.assertEqual(test_timeout_ping.round_trip_stddev, 0.057)

    def test_ping_unknown_host(self):
        self._resource_dict['resource_endpoint'] = 'localhostx'
        resource = PanoptesResource.resource_from_dict(self._resource_dict)

        unknown_host_response = "ping: cannot resolve {}: Unknown host".format(self._resource_dict['resource_endpoint'])
        e = subprocess.CalledProcessError(returncode=None, cmd=None,
                                          output=unknown_host_response)  # Shouldn't I be mocking this?
        #  When I try: "TypeError: exceptions must be old-style classes or derived from BaseException, not Mock"
        subprocess.check_output = Mock(side_effect=e)
        test_fail_d = {"round_trip_stddev": None, "packets_received": None, "execution_time": None,
                       "round_trip_avg": None, "packets_transmitted": None, "packet_loss_pct": None,
                       "round_trip_max": None, "round_trip_min": None}

        with self.assertRaises(PanoptesPingException):
            test_unknown_host_ping = PanoptesPingConnectionFactory.get_ping_connection(resource=resource,
                                                                                       context=None,
                                                                                       count=3,
                                                                                       timeout=100)
            self.assertDictEqual(test_fail_d, test_unknown_host_ping.response)

    def test_steamroller_ping(self):
        plugin_conf = {
            'Core': {
                'name': 'Test Plugin',
                'module': 'test_plugin'
            },
            'main': {
                'execute_frequency': '60',
                'resource_filter': 'resource_class = "network"'
            }
        }

        self._resource_dict['resource_metadata']['ping_route'] = 'steamroller'
        self._resource_dict['resource_metadata']['snmp_proxy_hosts'] = 'proxyhost-5.doesntexist.com:4443'
        resource = PanoptesResource.resource_from_dict(self._resource_dict)

        plugin_context = create_autospec(
            PanoptesPluginWithEnrichmentContext, instance=True, spec_set=True,
            data=resource,
            config=plugin_conf,
            snmp=panoptes_context.config_object.snmp_defaults,
            x509=panoptes_context.config_object.x509_defaults,
            secrets=secret_store,
            logger=logging.getLogger(__name__)
        )

        with patch('requests.Session') as rs:
            rs.return_value = MockConnection()
            ping_connection = PanoptesPingConnectionFactory.get_ping_connection(resource, plugin_context)
            self.assertEqual(ping_connection.packets_transmitted, 10)
            self.assertEqual(ping_connection.packets_received, 10)

            self.assertEqual(ping_connection.packet_loss_pct, 0.0)
            self.assertEqual(ping_connection.execution_time, 9.03)
            self.assertEqual(ping_connection.round_trip_min, 0.485)
            self.assertEqual(ping_connection.round_trip_avg, 0.504)
            self.assertEqual(ping_connection.round_trip_max, 0.535)
            self.assertEqual(ping_connection.round_trip_stddev, 0.027)

        with patch.object(requests.Session, 'post') as rs:
            rs.side_effect = requests.exceptions.Timeout()
            with self.assertRaises(PanoptesPingTimeoutException):
                PanoptesPingConnectionFactory.get_ping_connection(resource, plugin_context)

        with patch.object(requests.Session, 'post') as rs:
            rs.side_effect = requests.exceptions.ConnectionError()
            with self.assertRaises(PanoptesPingConnectionError):
                PanoptesPingConnectionFactory.get_ping_connection(resource, plugin_context)

        with patch.object(requests.Session, 'post') as rs:
            rs.side_effect = requests.exceptions.RequestException()
            with self.assertRaises(PanoptesPingRequestException):
                PanoptesPingConnectionFactory.get_ping_connection(resource, plugin_context)
