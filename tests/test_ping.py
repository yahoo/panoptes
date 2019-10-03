"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""

from mock import patch, Mock
import unittest
import subprocess
import json  # why isn't the call below recognized?

from yahoo_panoptes.framework.utilities.ping import *
from yahoo_panoptes.framework.resources import PanoptesResource, PanoptesContext
from yahoo_panoptes.plugins.polling.utilities.polling_status import PanoptesPollingStatus, \
    DEVICE_METRICS_STATES
from yahoo_panoptes.framework.metrics import PanoptesMetricsGroup

from helpers import get_test_conf_file

# For testing locally on OSX
TEST_PING_RESPONSE = "ping statistics ---\n" \
                     "3 packets transmitted, 3 received, 0% packet loss, time 1439ms\n" \
                     "rtt min/avg/max/mdev = 0.040/0.120/0.162/0.057 ms"


class TestPing(unittest.TestCase):

    def test_code_output(self):
        # Mock the system calls
        subprocess.check_output = Mock(return_value=TEST_PING_RESPONSE)
        test_ping = PanoptesPing(count=3, timeout=100, hostname="localhost")
        test_d = {u"round_trip_stddev": 0.057, u"packets_received": 3, u"execution_time": 1.44,
                  u"round_trip_avg": 0.120, u"packets_transmitted": 3, u"packet_loss_pct": 0.0,
                  u"round_trip_max": 0.162, u"round_trip_min": 0.040}

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
            PanoptesPing(count=-1)
        with self.assertRaises(AssertionError):
            PanoptesPing(count="1")
        with self.assertRaises(AssertionError):
            PanoptesPing(hostname=None)
        with self.assertRaises(AssertionError):
            PanoptesPing(hostname='')
        with self.assertRaises(AssertionError):
            PanoptesPing(timeout=0)
        with self.assertRaises(AssertionError):
            PanoptesPing(timeout=-1)

    def test_ping_timeout(self):
        e = subprocess.CalledProcessError(returncode=None, cmd=None, output=TEST_PING_RESPONSE)
        #  When I try: "TypeError: exceptions must be old-style classes or derived from BaseException, not Mock"
        subprocess.check_output = Mock(side_effect=e)
        test_d = {u"round_trip_stddev": 0.057, u"packets_received": 3, u"execution_time": 1.44,
                  u"round_trip_avg": 0.120, u"packets_transmitted": 3, u"packet_loss_pct": 0.0,
                  u"round_trip_max": 0.162, u"round_trip_min": 0.040}

        with self.assertRaises(PanoptesPingTimeoutException):
            test_timeout_ping = PanoptesPing(count=3, timeout=100, hostname="localhost")
            self.assertDictEqual(test_d, json.loads(test_timeout_ping.response))
            self.assertEqual(test_timeout_ping.packets_transmitted, 3)
            self.assertEqual(test_timeout_ping.packets_received, 3)
            self.assertEqual(test_timeout_ping.execution_time, 1.44)
            self.assertEqual(test_timeout_ping.round_trip_min, 0.040)
            self.assertEqual(test_timeout_ping.round_trip_avg, 0.120)
            self.assertEqual(test_timeout_ping.round_trip_max, 0.162)
            self.assertEqual(test_timeout_ping.round_trip_stddev, 0.057)

    def test_ping_unknown_host(self):
        hostname = 'localhostx'
        unknown_host_response = "ping: cannot resolve " + hostname + ": Unknown host"
        e = subprocess.CalledProcessError(returncode=None, cmd=None,
                                          output=unknown_host_response)  # Shouldn't I be mocking this?
        #  When I try: "TypeError: exceptions must be old-style classes or derived from BaseException, not Mock"
        subprocess.check_output = Mock(side_effect=e)
        test_fail_d = {u"round_trip_stddev": None, u"packets_received": None, u"execution_time": None,
                       u"round_trip_avg": None, u"packets_transmitted": None, u"packet_loss_pct": None,
                       u"round_trip_max": None, u"round_trip_min": None}

        with self.assertRaises(PanoptesPingException):
            test_unknown_host_ping = PanoptesPing(hostname=hostname)
            self.assertDictEqual(test_fail_d, test_unknown_host_ping.response)


class TestPollingStatus(unittest.TestCase):

    def setUp(self):

        self.my_dir, self.panoptes_test_conf_file = get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file)
        self._panoptes_resource = PanoptesResource(resource_site='test',
                                                   resource_class='test',
                                                   resource_subclass='test',
                                                   resource_type='test',
                                                   resource_id='test',
                                                   resource_endpoint='test',
                                                   resource_plugin='test',
                                                   resource_creation_timestamp=0,
                                                   resource_ttl=180)
        self.polling_status = PanoptesPollingStatus(resource=self._panoptes_resource,
                                                    execute_frequency=180,
                                                    logger=self._panoptes_context.logger)

    def test_polling_status_initial_settings(self):

        self.assertEqual(self.polling_status.device_status, DEVICE_METRICS_STATES.SUCCESS)
        self.assertEqual(self.polling_status.device_name, 'test')
        self.assertEqual(self.polling_status.device_type, 'test:test:test')

    def test_polling_failure_1(self):

        ERROR_PING_RESPONSE = "ping statistics ---\n" \
                              "3 packets transmitted, 0 received, 100% packet loss, time 1439ms\n" \
                              "rtt min/avg/max/mdev = 0.000/0.00/0.0/0.00 ms"

        subprocess.check_output = Mock(return_value=ERROR_PING_RESPONSE)

        self.polling_status._device_status = DEVICE_METRICS_STATES.TIMEOUT

        device_status = self.polling_status.device_status_metrics_group

        self.assertEqual(
            json.loads(device_status.json)['metrics'][0]['metric_value'],
            DEVICE_METRICS_STATES.PING_FAILURE
        )

    def test_polling_state_failure(self):

        self.polling_status.handle_success('Interface')
        self.polling_status.handle_success('Test')

        self.assertEqual(self.polling_status._metric_statuses,
            {'Interface': 0, 'Test': 0})

        self.polling_status.handle_exception('Interface', DEVICE_METRICS_STATES.TIMEOUT)

        self.assertEqual(self.polling_status._metric_statuses,
            {'Interface': 4, 'Test': 0})

        self.assertEqual(self.polling_status.device_status, 4)

    def test_polling_state_internal_failure(self):
        self.polling_status.handle_exception('Interface', 10000)

        self.assertEqual(self.polling_status._device_status,
                         DEVICE_METRICS_STATES.INTERNAL_FAILURE)

    def test_polling_state_success(self):

        self.polling_status.handle_success('Interface')
        self.polling_status.handle_success('Test')

        self.assertEqual(self.polling_status._metric_statuses,
            {'Interface': 0, 'Test': 0})

        self.assertEqual(self.polling_status.device_status, 0)
