"""
This module supplies an ICMP ping utility. Only supports Linux systems.
"""

import json
import re
import subprocess

from ..exceptions import PanoptesBaseException
from ..validators import PanoptesValidators

_VALID_PING_STATS = re.compile(r'.*ping statistics ---\n([0-9]*) packets transmitted, ([0-9]*) received, '
                               r'(\d*\.?\d*)% packet loss, time ([0-9]*)ms\nrtt min/avg/max/mdev = '
                               r'(\d*\.\d+)/(\d*\.\d+)/(\d*\.\d+)/(\d*\.\d+) ms')
"""
    regular expression object: Precompile regex pattern for collecting ping statistics
"""


class PanoptesPingException(PanoptesBaseException):
    """
        The class for all ping exceptions in the Panoptes system
    """
    pass


class PanoptesPingTimeoutException(PanoptesPingException):
    """
        The class for all ping timeout exceptions in the Panoptes system
    """
    pass


class PanoptesPing(object):
    """
        The class for pinging a device and returning its status

        Args:
            count (int): number of ping attempts to be made
            hostname (str): name of the host to ping
            timeout (int): time to wait before cancelling ping
    """

    def __init__(self, count=10, timeout=10, hostname="localhost"):
        assert PanoptesValidators.valid_nonzero_integer(count), 'count must be integer > 0'
        assert PanoptesValidators.valid_nonempty_string(hostname), 'hostname must be nonempty string'
        assert PanoptesValidators.valid_nonzero_integer(timeout), 'timeout must be integer > 0'

        self._response = dict()  # dictionary containing ping statistics
        self._response['packets_transmitted'] = None
        self._response['packets_received'] = None
        self._response['packet_loss_pct'] = None
        self._response['execution_time'] = None
        self._response['round_trip_min'] = None
        self._response['round_trip_avg'] = None
        self._response['round_trip_max'] = None
        self._response['round_trip_stddev'] = None

        try:
            resp = subprocess.check_output(
                ['/bin/ping', '-c', str(count), '-w', str(timeout), hostname],
                stderr=subprocess.STDOUT,
                universal_newlines=True  # return string not bytes
            )
            self._get_ping_stats(resp)
        except subprocess.CalledProcessError as e:
            self._get_ping_stats(e.output)
            if self._response['packets_transmitted'] is not None:
                raise PanoptesPingTimeoutException("Ping timed out with response: " + str(self._response))
            raise PanoptesPingException(e.output)
        except Exception as e:
            raise PanoptesPingException(e.message)

    def _get_ping_stats(self, resp):
        m = _VALID_PING_STATS.search(resp)
        if m:
            self._response['packets_transmitted'] = int(m.group(1))
            self._response['packets_received'] = int(m.group(2))
            self._response['packet_loss_pct'] = float(m.group(3))
            self._response['execution_time'] = round(float(m.group(4)) / 1000, 2)  # in seconds
            self._response['round_trip_min'] = float(m.group(5))
            self._response['round_trip_avg'] = float(m.group(6))
            self._response['round_trip_max'] = float(m.group(7))
            self._response['round_trip_stddev'] = float(m.group(8))

    @property
    def packets_transmitted(self):
        """whole number count of packets transmitted; should equal 'count'"""
        return self._response['packets_transmitted']

    @property
    def packets_received(self):
        """whole number count of packets received"""
        return self._response['packets_received']

    @property
    def packet_loss_pct(self):
        """percentage of packets lost"""
        return self._response['packet_loss_pct']

    @property
    def execution_time(self):
        """duration in seconds of time 'ping' takes to execute"""
        return self._response['execution_time']

    @property
    def round_trip_min(self):
        """minimum round-trip time"""
        return self._response['round_trip_min']

    @property
    def round_trip_avg(self):
        """average round-trip time"""
        return self._response['round_trip_avg']

    @property
    def round_trip_max(self):
        """maximum round-trip time"""
        return self._response['round_trip_max']

    @property
    def round_trip_stddev(self):
        """standard deviation of all round-trip times"""
        return self._response['round_trip_stddev']

    @property
    def response(self):
        """returns JSON of all ping statistics"""
        return json.dumps(self._response)
