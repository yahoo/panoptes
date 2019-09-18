"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
import base64
import random
import uuid

import requests
from yahoo_panoptes.framework import const
from yahoo_panoptes.framework.resources import PanoptesResource
from yahoo_panoptes.framework.utilities.snmp.connection import *


class PanoptesSNMPSteamRollerAgentConnection(PanoptesSNMPConnection):

    def _create_request(self, method, oid, options):
        request = dict()
        request['guid'] = str(uuid.uuid4())
        request['requests'] = list()
        request['requests'].append(dict())
        request['requests'][0]['devices'] = list()
        request['requests'][0]['devices'].append(self._host)
        request['requests'][0]['authentication'] = {'type': 'community',
                                                    'params': {
                                                        'community': self._community
                                                    }}
        request['requests'][0]['timeout'] = self._timeout
        request['requests'][0]['phases'] = {
            oid: {
                'operation': method,
                'version': 2,
                'oids': list(),
                'options': options
            }
        }
        request['requests'][0]['phases'][oid]['oids'].append(oid)

        return request

    @staticmethod
    def _decode_value(type, value):
        if value is None:
            return None
        elif type == 'OCTETSTR':
            return base64.b64decode(value)
        else:
            return value

    def _deserialize_response(self, response, method, oid):
        varbinds = list()

        try:
            snmp_objects = response['responses'][self._host][oid]
        except KeyError:
            raise PanoptesSNMPException('Error parsing SNMP response')

        for i in range(len(snmp_objects)):
            response_dict = snmp_objects[i]

            if response_dict['result'] != 'success':
                try:
                    raise SNMP_ERRORS_MAP[response_dict['reason']]
                except KeyError as e:
                    raise PanoptesSNMPException('Error parsing SNMP response - missing key: {}'.format(e.message))

            try:
                response_type = response_dict['type']
                response_value = self._decode_value(response_type, response_dict['value'])

                varbinds.append(PanoptesSNMPVariable(queried_oid=oid,
                                                     oid=response_dict['oid'],
                                                     index=response_dict['index'],
                                                     value=response_value,
                                                     snmp_type=response_type)
                                )
            except KeyError as e:
                raise PanoptesSNMPException('Error parsing SNMP response - missing key: {}'.format(e.message))

        if method == 'get':
            return varbinds[0]
        else:
            return varbinds

    def _send_and_process_request(self, method, oid, **options):
        try:
            request = self._create_request(method, oid, options)
        except KeyError:
            return PanoptesSNMPException('Error creating JSON request')

        try:
            response = self._connection.post(self._proxy_url, json=request)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise PanoptesSNMPConnectionException('Error in getting response from SteamRoller SNMP Agent: {} -> {}'.
                                                  format(e.message, response.text))
        except requests.exceptions.Timeout as e:
            raise PanoptesSNMPTimeoutException(e.message)
        except requests.exceptions.ConnectionError as e:
            raise PanoptesSNMPConnectionException(e.message)
        except requests.exceptions.RequestException as e:
            raise PanoptesSNMPException(e.message)

        try:
            decoded_response = response.json()
        except ValueError as e:
            raise PanoptesSNMPConnectionException('Error in parsing response from SteamRoller SNMP Agent: {}'.format(e))

        return self._deserialize_response(decoded_response, method, oid)

    def __init__(self, host, port, timeout, retries, community, proxy_url, x509_secure_connection, x509_cert_file,
                 x509_key_file):
        """
        Starts a SNMP connection with the given parameters

        Args:
            host (str): The host to interact with SNMP
            port (int): The port on the host
            timeout (int): Non-zero seconds to wait for connection and replies
            retries (int): The number of times to retry a failed query
            community (str): The community string to use
            proxy_url (str): The SteamRoller SNMP Agent URL to send the request to
            x509_secure_connection (int): Whether connections should be secure
            x509_cert_file (string): absolute path and filename to the x509 certificate
            x509_key_file (string): absolute path and filename to the x509 key

        Returns:
            None
        """
        super(PanoptesSNMPSteamRollerAgentConnection, self).__init__(host, port, timeout, retries)
        assert PanoptesValidators.valid_nonempty_string(community), 'community_string must a non-empty string'
        assert PanoptesValidators.valid_nonempty_string(proxy_url), 'proxy_host must a non-empty string'

        self._community = community
        self._proxy_url = proxy_url
        self._connection = self._make_connection(x509_secure_connection, x509_cert_file, x509_key_file)

    def get(self, oid):
        return self._send_and_process_request(method='get', oid=oid)

    def bulk_walk(self, oid, non_repeaters=0, max_repetitions=10):
        return self._send_and_process_request(method='bulkwalk', oid=oid,
                                              non_repeaters=non_repeaters,
                                              max_repetitions=max_repetitions)

    @staticmethod
    def _make_connection(secure_connection, cert_file, key_file):
        """
        x509 local certificate use.
        If secure_connection is mandated, a bad key/cert will throw an AssertionError.  If optional, it'll use an
        insecure connection as a fallback.

        Args:
            secure_connection (int): Whether the connection should be secure 0 - No, 1 - Optional, 2 - Yes
            cert_file (string): Absolute path and filename to the supplied cert
            key_file (string: Absolute path and filename to the key file

        Returns:
            requests.Session
        """
        secure = False
        connection = requests.Session()

        is_valid_key_file = PanoptesValidators.valid_readable_file(key_file)
        is_valid_cert_file = PanoptesValidators.valid_readable_file(cert_file)
        if secure_connection == '2':
            # required
            assert is_valid_key_file, 'Check key file is readable - {}'.format(key_file)
            assert is_valid_cert_file, 'Check cert file is readable - {}'.format(cert_file)
            secure = True
        elif secure_connection == '1':
            # optional
            if is_valid_key_file and is_valid_cert_file:
                secure = True

        if secure:
            connection.cert = (cert_file, key_file)

        return connection


class PanoptesSNMPConnectionFactory(object):
    def __init__(self):
        pass

    @staticmethod
    def get_snmp_connection_raw(resource, snmp_community_string_key, community_suffix,
                                secrets, logger, x509_secure_connection, x509_cert_file, x509_key_file, timeout=None,
                                retries=None, port=None):
        host = resource.resource_endpoint

        try:
            logger.debug('Going to get fetch SNMP community string using key "%s" for site "%s"' % (
                snmp_community_string_key, resource.resource_site))
            community_string = secrets.get_by_site(snmp_community_string_key, resource.resource_site)
        except Exception as e:
            raise PanoptesSNMPException('Could not fetch SNMP community string for site % using key %s: %s' % (
                resource.resource_site, snmp_community_string_key, str(e)))

        if not community_string:
            raise PanoptesSNMPException(
                'SNMP community string is empty for site %s (used key %s)' % (resource.resource_site,
                                                                              snmp_community_string_key))

        if community_suffix:
            community_string = community_string + '@' + str(community_suffix)

        if 'snmp_proxy_hosts' in resource.resource_metadata.keys():
            # If the resource has associated SNMP Proxy Hosts, try a SteamRoller SNMP Agent connection
            proxy_hosts = resource.resource_metadata['snmp_proxy_hosts'].split(const.KV_STORE_DELIMITER)
            # Pick a random proxy host from the list of proxy hosts
            proxy_host = proxy_hosts[random.randint(0, len(proxy_hosts) - 1)]

            logger.info('Using Steamroller connection via "%s" to %s (x509=%s)' % (proxy_host, host,
                                                                                   x509_secure_connection))
            return PanoptesSNMPSteamRollerAgentConnection(host=host, port=port, timeout=timeout,
                                                          retries=retries,
                                                          x509_secure_connection=x509_secure_connection,
                                                          x509_key_file=x509_key_file, x509_cert_file=x509_cert_file,
                                                          community=community_string,
                                                          proxy_url='https://{}'.format(proxy_host))
        else:
            # Try a SNMP V2 connection
            logger.info('Using SNMPV2 connection for %s' % host)
            return PanoptesSNMPV2Connection(host=host, port=port, timeout=timeout, retries=retries,
                                            community=community_string)

    @staticmethod
    def get_snmp_connection(plugin_context, resource, timeout=None, retries=None, port=None,
                            x509_secure_connection=None, x509_key_file=None, x509_cert_file=None,
                            community_suffix=None):
        assert isinstance(plugin_context,
                          PanoptesPluginContext), 'plugin_context must be a class or subclass of PanoptesPluginContext'
        assert isinstance(resource, PanoptesResource), 'resource must be a class or subclass of PanoptesResource'

        logger = plugin_context.logger
        secrets = plugin_context.secrets

        default_snmp_config = plugin_context.snmp
        default_x509_config = plugin_context.x509

        # SNMP ----
        if timeout is None:
            timeout = default_snmp_config['timeout']

        if retries is None:
            retries = default_snmp_config['retries']

        if not port:
            port = default_snmp_config['port']

        # x509 ----
        if x509_secure_connection is None:
            x509_secure_connection = plugin_context.config['x509'].get('x509_secured_requests',
                                                                       default_x509_config['x509_secured_requests'])

        if not x509_key_file:
            key_location = plugin_context.config['x509'].get('x509_key_location',
                                                             default_x509_config['x509_key_location'])
            key_filename = plugin_context.config['x509'].get('x509_key_filename',
                                                             default_x509_config['x509_key_filename'])
            x509_key_file = '{}/{}'.format(key_location, key_filename)

        if not x509_cert_file:
            cert_location = plugin_context.config['x509'].get('x509_cert_location',
                                                              default_x509_config['x509_cert_location'])
            cert_filename = plugin_context.config['x509'].get('x509_cert_filename',
                                                              default_x509_config['x509_cert_filename'])
            x509_cert_file = '{}/{}'.format(cert_location, cert_filename)

        snmp_community_string_key = default_snmp_config['community_string_key']

        return PanoptesSNMPConnectionFactory.get_snmp_connection_raw(resource, snmp_community_string_key,
                                                                     community_suffix, secrets, logger,
                                                                     x509_secure_connection,
                                                                     x509_cert_file, x509_key_file, timeout,
                                                                     retries, port)
