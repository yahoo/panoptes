"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""

from mock import patch, Mock
import unittest
import subprocess
import json  # why isn't the call below recognized?

from yahoo_panoptes.framework.utilities.ping import *

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
