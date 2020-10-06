"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module supplies an ICMP ping utility. Only supports Linux systems.
"""

import json
import re
import random
import requests
import subprocess

from yahoo_panoptes.framework import const
from yahoo_panoptes.framework.resources import PanoptesResource
from yahoo_panoptes.framework.exceptions import PanoptesBaseException
from yahoo_panoptes.framework.validators import PanoptesValidators
from yahoo_panoptes.plugins.helpers.snmp_connections import PanoptesSNMPConnectionFactory, \
    PanoptesSNMPSteamRollerAgentConnection

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


class PanoptesPingConnectionError(PanoptesPingException):
    """
    The class for all ping connection errors in the Panoptes system
    """
    pass


class PanoptesPingRequestException(PanoptesBaseException):
    """
    The class for all ping request exceptions in the Panoptes system
    """
    pass


class PanoptesPingConnectionFactory:

    @staticmethod
    def get_ping_connection(resource, context=None, count=10, timeout=10):
        """
        Returns the appropriate ping class.

        Ping routes by default are assumed to be 'direct'. I.E. The polling host will run ping.

        If the ping_route on the device is set to steamroller AND there is a snmp_proxy_host key set
        on the device. The ICMP request will be forwarded via steamroller and executed on the
        steamroller proxy box.

        Note: ping_route is a value that can be toggled in the discovery plugin.
        """
        assert isinstance(resource, PanoptesResource), 'resource must be an instance of PanoptesResource'

        if context and resource.resource_metadata.get('ping_route', 'default') == 'steamroller' and \
                'snmp_proxy_hosts' in list(resource.resource_metadata.keys()):
            return PanoptesPingSteamroller(context, resource, count, timeout)
        else:
            return PanoptesPingDirect(count, timeout, hostname=resource.resource_endpoint)


class PanoptesPing:
    """
    The container class that holds the resulting metrics from a device ping
    """

    def __init__(self):
        self._response = dict()  # dictionary containing ping statistics
        self._response['packets_transmitted'] = None
        self._response['packets_received'] = None
        self._response['packet_loss_pct'] = None
        self._response['execution_time'] = None
        self._response['round_trip_min'] = None
        self._response['round_trip_avg'] = None
        self._response['round_trip_max'] = None
        self._response['round_trip_stddev'] = None

    def setPingStatsAPIResponse(self, decoded_response):
        self._response['packets_transmitted'] = decoded_response['packets_transmitted']
        self._response['packets_received'] = decoded_response['packets_received']
        self._response['packet_loss_pct'] = decoded_response['packet_loss_pct']
        self._response['execution_time'] = decoded_response['execution_time']
        self._response['round_trip_min'] = decoded_response['round_trip_min']
        self._response['round_trip_avg'] = decoded_response['round_trip_avg']
        self._response['round_trip_max'] = decoded_response['round_trip_max']
        self._response['round_trip_stddev'] = decoded_response['round_trip_stddev']

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


class PanoptesPingSteamroller(PanoptesPing):
    """
    This class proxies a ping request to an external server running
    steamroller client to perform the ping.

    TODO: Open Source Steamroller Client
    """

    def __init__(self, context, resource, count, timeout):
        super(PanoptesPingSteamroller, self).__init__()

        self._plugin_context = context
        self._resource = resource
        self._count = count
        self._timeout = timeout

        self._get_ping_stats()

    def _get_proxy_host(self):
        proxy_hosts = self._resource.resource_metadata['snmp_proxy_hosts'].split(const.KV_STORE_DELIMITER)
        return proxy_hosts[random.randint(0, len(proxy_hosts) - 1)]

    def _get_ping_stats(self):

        x509_secure_connection, x509_key_file, x509_cert_file = \
            PanoptesSNMPConnectionFactory.parse_x509_config(self._plugin_context)
        connection = PanoptesSNMPSteamRollerAgentConnection._make_connection(x509_secure_connection,
                                                                             x509_key_file,
                                                                             x509_cert_file)

        request = dict()
        request['count'] = self._count
        request['timeout'] = self._timeout

        endpoint = 'https://{}/ping/{}'.format(self._get_proxy_host(), self._resource.resource_endpoint)

        try:
            response = connection.post(endpoint, json=request)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise PanoptesPingException('Error in getting response from SteamRoller SNMP Agent: {} -> {}'.
                                        format(str(e), response.text))
        except requests.exceptions.Timeout as e:
            raise PanoptesPingTimeoutException(str(e))
        except requests.exceptions.ConnectionError as e:
            raise PanoptesPingConnectionError(str(e))
        except requests.exceptions.RequestException as e:
            raise PanoptesPingRequestException(str(e))

        try:
            decoded_response = response.json()
        except ValueError as e:
            raise PanoptesPingException('Error Parsing Response From Steamroller -> {}'.format(str(e)))

        self.setPingStatsAPIResponse(decoded_response)


class PanoptesPingDirect(PanoptesPing):
    """
    The class for pinging a device and returning its status

    Args:
        count (int): number of ping attempts to be made
        hostname (str): name of the host to ping
        timeout (int): time to wait before cancelling ping
    """

    def __init__(self, count=10, timeout=10, hostname='localhost'):
        assert PanoptesValidators.valid_nonzero_integer(count), 'count must be integer > 0'
        assert PanoptesValidators.valid_nonempty_string(hostname), 'hostname must be nonempty string'
        assert PanoptesValidators.valid_nonzero_integer(timeout), 'timeout must be integer > 0'

        super(PanoptesPingDirect, self).__init__()

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
            raise PanoptesPingException(str(e))

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
