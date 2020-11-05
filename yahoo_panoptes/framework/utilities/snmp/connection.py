"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""

from builtins import object
import yahoo_panoptes_snmp as easysnmp
from yahoo_panoptes_snmp.exceptions import *

from yahoo_panoptes.framework.utilities.snmp.exceptions import *
from yahoo_panoptes.framework.utilities.snmp.variable import PanoptesSNMPVariable
from yahoo_panoptes.framework.plugins.context import PanoptesPluginContext
from yahoo_panoptes.framework.validators import PanoptesValidators


exception_class_mapping = {EasySNMPError: PanoptesSNMPException,
                           EasySNMPConnectionError: PanoptesSNMPConnectionException,
                           EasySNMPTimeoutError: PanoptesSNMPTimeoutException,
                           EasySNMPUnknownObjectIDError: PanoptesSNMPUnknownObjectIDException,
                           EasySNMPNoSuchNameError: PanoptesSNMPNoSuchNameException,
                           EasySNMPNoSuchObjectError: PanoptesSNMPNoSuchObjectException,
                           EasySNMPNoSuchInstanceError: PanoptesSNMPNoSuchInstanceException,
                           EasySNMPUndeterminedTypeError: PanoptesSNMPUndeterminedTypeException}


SNMP_ERRORS_MAP = {
    u'ERROR': PanoptesSNMPException,
    u'CONNECTION': PanoptesSNMPConnectionException,
    u'TIMEOUT': PanoptesSNMPTimeoutException,
    u'NOSUCHOBJECT': PanoptesSNMPNoSuchObjectException,
    u'NOSUCHINSTANCE': PanoptesSNMPNoSuchInstanceException,
    u'NOSUCHNAME': PanoptesSNMPNoSuchNameException,
    u'UNKNOWNOBJECTID': PanoptesSNMPUnknownObjectIDException,
    u'UNDETERMINEDTYPE': PanoptesSNMPUndeterminedTypeException
}


class PanoptesSNMPConnection(object):
    def __init__(self, host, port, timeout, retries):
        """
        Initialize the device.

        Args:
            self: (todo): write your description
            host: (str): write your description
            port: (int): write your description
            timeout: (int): write your description
            retries: (todo): write your description
        """
        assert PanoptesValidators.valid_nonempty_string(host), u'host must a non-empty string'
        assert PanoptesValidators.valid_port(port), u'port must be an integer between 1 and 65535'
        assert PanoptesValidators.valid_nonzero_integer(timeout), u'timeout must be a integer greater than zero'
        assert PanoptesValidators.valid_positive_integer(retries), u'retries must a non-negative integer'

        self._host = host
        self._port = port
        self._timeout = timeout
        self._retries = retries
        self._easy_snmp_session = None

    @property
    def host(self):
        """
        Return the host.

        Args:
            self: (todo): write your description
        """
        return self._host

    @property
    def port(self):
        """
        : return : return :

        Args:
            self: (todo): write your description
        """
        return self._port

    @property
    def timeout(self):
        """
        The timeout.

        Args:
            self: (todo): write your description
        """
        return self._timeout

    @property
    def retries(self):
        """
        Returns the retries of retries.

        Args:
            self: (todo): write your description
        """
        return self._retries

    @staticmethod
    def _wrap_panoptes_snmp_exception(easy_snmp_exception):
        """
        Maps EasySNMP exceptions to PanoptesSNMPExceptions

        Args:
            easy_snmp_exception (Exception): The EasySNMP exception to map

        Returns:
            PanoptesSNMPException: An equivalent PanoptesSNMPException
        """
        try:
            if hasattr(easy_snmp_exception, 'message'):
                return exception_class_mapping[type(easy_snmp_exception)](easy_snmp_exception.message)
            return exception_class_mapping[type(easy_snmp_exception)](str(easy_snmp_exception))
        except KeyError:
            return PanoptesSNMPException(repr(easy_snmp_exception))

    def get(self, oid):
        """
        Takes a single numeric oid and returns a PanoptesSNMPVariable containing the queried oid, index, value and type

        Args:
            oid (str): A numeric oid

        Returns:
            PanoptesSNMPVariable: A PanoptesSNMPVariable containing the queried oid, index, value and type
        """
        assert PanoptesValidators.valid_numeric_snmp_oid(oid), u'oid must be numeric string with a leading period'
        assert self._easy_snmp_session is not None, u'PanoptesSNMPSession not initialized'

        try:
            varbind = self._easy_snmp_session.get(oids=oid)
            return PanoptesSNMPVariable(queried_oid=oid, oid=varbind.oid, index=varbind.oid_index, value=varbind.value,
                                        snmp_type=varbind.snmp_type)
        except Exception as e:
            raise self._wrap_panoptes_snmp_exception(e)

    def get_bulk(self, oid, non_repeaters=0, max_repetitions=10, base_oid=None):
        """
         Takes a single numeric oid and returns a list of PanoptesSNMPVariables which start with the oid

        Args:
            oid (str): A numeric oid
            non_repeaters (int): A non-negative integer
            max_repetitions (int):  A non-negative integer
            base_oid (str): An optional numeric oid which is used to set the oid in the returned PanoptesSNMPVariables

        Returns:
            list(): A list of PanoptesSNMPVariables starting with the provided oid
        """
        assert PanoptesValidators.valid_numeric_snmp_oid(oid), u'oid must be numeric string with a leading period'
        assert PanoptesValidators.valid_positive_integer(non_repeaters), u'non_repeaters must a positive integer'
        assert PanoptesValidators.valid_positive_integer(max_repetitions), u'max_repetitions must a positive integer'
        assert (base_oid is None) or PanoptesValidators.valid_numeric_snmp_oid(
                base_oid), u'oid must be numeric string with a leading period'
        assert self._easy_snmp_session is not None, u'PanoptesSNMPSession not initialized'
        try:
            varbinds = self._easy_snmp_session.get_bulk(oids=oid, non_repeaters=non_repeaters,
                                                        max_repetitions=max_repetitions)

            result = list()
            for varbind in varbinds:
                if varbind.oid.startswith(base_oid):
                    result.append(PanoptesSNMPVariable(queried_oid=base_oid if base_oid else oid, oid=varbind.oid,
                                                       index=varbind.oid_index, value=varbind.value,
                                                       snmp_type=varbind.snmp_type))
            return result
        except Exception as e:
            raise self._wrap_panoptes_snmp_exception(e)

    def bulk_walk(self, oid, non_repeaters=0, max_repetitions=10):
        """
        Takes a single numeric oid and fetches oids under it's subtree using GETBULKs

        Args:
            oid (str): A numeric oid
            non_repeaters (int): A non-negative integer
            max_repetitions (int):  A non-negative integer

        Returns:
            list(PanoptesSNMPVariable): A list of PanoptesSNMPVariables within the subtree of the provided oid
        """
        assert PanoptesValidators.valid_numeric_snmp_oid(oid), u'oid must be numeric string with a leading period'
        assert PanoptesValidators.valid_positive_integer(non_repeaters), u'non_repeaters must a positive integer'
        assert PanoptesValidators.valid_positive_integer(max_repetitions), u'max_repetitions must a positive integer'
        assert self._easy_snmp_session is not None, u'PanoptesSNMPSession not initialized'

        base_oid = oid
        varbinds = list()
        running = 1

        while running:
            results = self.get_bulk(oid=oid, non_repeaters=non_repeaters,
                                    max_repetitions=max_repetitions, base_oid=base_oid)
            varbinds.extend(results)
            if len(results) < max_repetitions:
                break
            oid = base_oid + u'.' + results[-1].index

        return varbinds


class PanoptesSNMPV2Connection(PanoptesSNMPConnection):
    def __init__(self, host, port, timeout, retries, community):
        """
        Starts a SNMP V2 connection with the given parameters

        Args:
            host (str): The host to interact with SNMP
            port (int): The port on the host
            timeout (int): Non-zero seconds to wait for connection and replies
            retries (int): The number of times to retry a failed query
            community (str): The community string to use

        Returns:
            None: The session is initiated and stored locally
        """

        super(PanoptesSNMPV2Connection, self).__init__(host, port, timeout, retries)
        assert PanoptesValidators.valid_nonempty_string(community), u'community_string must a non-empty string'

        self._community = community

        try:
            self._easy_snmp_session = easysnmp.Session(hostname=self._host, remote_port=self._port,
                                                       timeout=self._timeout, retries=self._retries,
                                                       version=2, use_numeric=True,
                                                       community=self._community)

        except Exception as e:
            raise self._wrap_panoptes_snmp_exception(e)

    @property
    def community(self):
        """
        Returns the community.

        Args:
            self: (todo): write your description
        """
        return self._community


class PanoptesSNMPV3TLSConnection(PanoptesSNMPConnection):
    def __init__(self, host, port, timeout, retries, context, our_identity, their_identity):
        """
        Starts a SNMP V3 connection with the given parameters - works only with TLS

        Args:
            host (str): The host to interact with SNMP
            port (int): The port on the host
            timeout (int): Non-zero seconds to wait for connection and replies
            retries (int): The number of times to retry a failed query
            context (str): A non-empty SNMP v3 context
            our_identity (str): The fingerprint or filename of the certificate to present to the host
            their_identity (str): The fingerprint or filename of the certificate to expect from the host

        Returns:
            None: The session is initiated and stored locally
        """
        super(PanoptesSNMPV3TLSConnection, self).__init__(host, port, timeout, retries)
        assert PanoptesValidators.valid_nonempty_string(context), u'context must be a non-empty string'
        assert PanoptesValidators.valid_nonempty_string(our_identity), u'our_identity must be a non-empty string'
        assert PanoptesValidators.valid_nonempty_string(their_identity), u'their_identity must be a non-empty string'

        self._context = context
        self._our_identity = our_identity
        self._their_identity = their_identity

        try:
            self._easy_snmp_session = easysnmp.Session(hostname=self._host, remote_port=self._port,
                                                       timeout=self._timeout, retries=self._retries,
                                                       version=3, use_numeric=True,
                                                       transport=u'tlstcp',
                                                       context=self._context,
                                                       our_identity=self._our_identity,
                                                       their_identity=self._their_identity)
        except Exception as e:
            raise self._wrap_panoptes_snmp_exception(e)


class PanoptesSNMPPluginConfiguration(object):
    """
    This class encapsulates all SNMP related configuration, including fetching SNMP community string from the secrets
    store as needed

    Args:
        plugin_context: The PanoptesPluginContext from which to derive all SNMP configuration
    """

    def __init__(self, plugin_context):
        """
        Initialize the snmp configuration.

        Args:
            self: (todo): write your description
            plugin_context: (todo): write your description
        """
        assert isinstance(plugin_context,
                          PanoptesPluginContext), u'plugin_context must be a class or subclass of ' \
                                                  u'PanoptesPluginContext'
        self._plugin_context = plugin_context
        self._plugin_snmp_configuration = plugin_context.config.get(u'snmp', {})
        self._plugin_x509_configuration = plugin_context.config.get(u'x509', {})
        self._default_snmp_configuration = plugin_context.snmp
        self._default_x509_configuration = plugin_context.x509

        self._connection_factory_module = None
        self._connection_factory_class = None
        self._community_string_key = None
        self._proxy_port = None
        self._port = None
        self._community = None
        self._community_string_key = None
        self._timeout = None
        self._retries = None
        self._non_repeaters = None
        self._max_repetitions = None

        self._x509_secure_connection = None
        self._x509_certificate_file = None
        self._x509_key_file = None

        self._parse_snmp_configuration()
        self._parse_x509_configuration()
        self._get_snmp_community_string()

    def _get_snmp_community_string(self):
        """
        This method sets the community string that should be used. It looks up the community string in the following
        order of preference

        1. A community string specified in the plugin configuration file
        2. A community string related to the site in which the resource resides from the 'secrets' store
        3. The default community string specified in the Panoptes configuration file

        Returns:
            None
        """
        logger = self._plugin_context.logger

        # Try and get community string from the plugin configuration
        self._community = self._plugin_snmp_configuration.get(u'community')

        if self._community:
            assert PanoptesValidators.valid_nonempty_string(
                self._community), u'SNMP community must be a non-empty string'
            return

        # Else lookup the site-specific community string
        site = self._plugin_context.data.resource_site

        try:
            logger.debug(u'Going to get fetch SNMP community string using key "{}" for site "{}"'.format(
                self.community_string_key, site))
            self._community = self._plugin_context.secrets.get_by_site(self.community_string_key, site)
        except Exception as e:
            raise PanoptesSNMPException(
                u'Could not fetch SNMP community string for site "{}" using key "{}": {}'.format(
                    site, self.community_string_key, repr(e)))

        if self._community:
            assert PanoptesValidators.valid_nonempty_string(
                self._community), u'SNMP community must be a non-empty string'
            return

        # Else return default community string
        self._community = self._default_snmp_configuration.get(u'community')

        assert PanoptesValidators.valid_nonempty_string(
            self._community), u'SNMP community must be a non-empty string'

    def _parse_snmp_configuration(self):
        """
        Parse snmp configuration.

        Args:
            self: (todo): write your description
        """
        self._connection_factory_module = self._plugin_snmp_configuration.get(u'connection_factory_module',
                                                                              self._default_snmp_configuration[
                                                                                  u'connection_factory_module'])
        assert PanoptesValidators.valid_nonempty_string(
            self._connection_factory_module), u'SNMP connection factory module must be a non-empty string'

        self._connection_factory_class = self._plugin_snmp_configuration.get(u'connection_factory_class',
                                                                             self._default_snmp_configuration[
                                                                                 u'connection_factory_class'])
        assert PanoptesValidators.valid_nonempty_string(
            self._connection_factory_class), u'SNMP connection factory class must be a non-empty string'

        self._community_string_key = self._plugin_snmp_configuration.get(u'community_string_key',
                                                                         self._default_snmp_configuration.get(
                                                                             u'community_string_key'))
        assert PanoptesValidators.valid_nonempty_string(
            self._community_string_key), u'SNMP community string key must be a non-empty string'

        self._port = int(
            self._plugin_snmp_configuration.get(u'port', self._default_snmp_configuration[u'port']))
        assert PanoptesValidators.valid_port(self._port), u'SNMP port must be a valid TCP/UDP port number'

        self._proxy_port = int(
            self._plugin_snmp_configuration.get(u'proxy_port', self._default_snmp_configuration[u'proxy_port']))
        assert PanoptesValidators.valid_port(self._proxy_port), u'SNMP proxy port must be a valid TCP/UDP port number'

        self._timeout = int(
            self._plugin_snmp_configuration.get(u'timeout', self._default_snmp_configuration[u'timeout']))
        assert PanoptesValidators.valid_nonzero_integer(
            self._timeout), u'SNMP timeout must be a positive integer'

        self._retries = int(
            self._plugin_snmp_configuration.get(u'retries', self._default_snmp_configuration[u'retries']))
        assert PanoptesValidators.valid_nonzero_integer(
            self._retries), u'SNMP retries must be a positive integer'

        self._non_repeaters = int(self._plugin_snmp_configuration.get(u'non_repeaters',
                                                                      self._default_snmp_configuration[
                                                                          u'non_repeaters']))
        assert PanoptesValidators.valid_positive_integer(
            self._non_repeaters), u'SNMP non-repeaters must be a positive integer'

        self._max_repetitions = int(
            self._plugin_snmp_configuration.get(u'max_repetitions',
                                                self._default_snmp_configuration[u'max_repetitions']))
        assert PanoptesValidators.valid_nonzero_integer(
            self._max_repetitions), u'SNMP max-repetitions must be a positive integer'

    def _parse_x509_configuration(self):
        """
        Parses the config spec to provide some details on the x509 communication.  This is effectively requests over
        x509 based on the value of _x509_secure_connection.

        0 - x509 communications
        1 - x509 where available (optional)
        2 - x509 mandatory (no connection made without x509)

        cert and key must be available for 1 & 2.
        """
        self._x509_secure_connection = int(self._plugin_x509_configuration.get(u'x509_secured_requests',
                                                                               self._default_x509_configuration[
                                                                                   u'x509_secured_requests']))
        assert PanoptesValidators.valid_positive_integer(self._x509_secure_connection),\
            u'x509 secure connection must be a positive integer'
        assert self._x509_secure_connection < 3, u'x509 secure connection cannot be greater than 2'

        cert_location = self._plugin_x509_configuration.get(u'x509_cert_location',
                                                            self._default_x509_configuration[u'x509_cert_location'])

        assert PanoptesValidators.valid_nonempty_string(cert_location), u'cert location must be a valid string'
        cert_filename = self._plugin_x509_configuration.get(u'x509_cert_filename',
                                                            self._default_x509_configuration[u'x509_cert_filename'])
        assert PanoptesValidators.valid_nonempty_string(cert_filename), u'cert filename must be a valid string'
        self._x509_certificate_file = u'{}/{}'.format(cert_location, cert_filename)

        key_location = self._plugin_x509_configuration.get(u'x509_key_location',
                                                           self._default_x509_configuration[u'x509_key_location'])
        assert PanoptesValidators.valid_nonempty_string(key_location), u'key location must be a valid string'
        key_filename = self._plugin_x509_configuration.get(u'x509_key_filename',
                                                           self._default_x509_configuration[u'x509_key_filename'])
        assert PanoptesValidators.valid_nonempty_string(key_filename), u'key filename must be a valid string'
        self._x509_key_file = u'{}/{}'.format(key_location, key_filename)

    @property
    def connection_factory_module(self):
        """
        Return a connection factory.

        Args:
            self: (todo): write your description
        """
        return self._connection_factory_module

    @property
    def connection_factory_class(self):
        """
        Return a new connection factory. connectionfactory.

        Args:
            self: (todo): write your description
        """
        return self._connection_factory_class

    @property
    def community_string_key(self):
        """
        Str : class : community_string for the community s community.

        Args:
            self: (todo): write your description
        """
        return self._community_string_key

    @property
    def community(self):
        """
        Returns the community.

        Args:
            self: (todo): write your description
        """
        return self._community

    @property
    def port(self):
        """
        : return : return :

        Args:
            self: (todo): write your description
        """
        return self._port

    @property
    def proxy_port(self):
        """
        Return the proxy port.

        Args:
            self: (todo): write your description
        """
        return self._proxy_port

    @property
    def timeout(self):
        """
        The timeout.

        Args:
            self: (todo): write your description
        """
        return self._timeout

    @property
    def retries(self):
        """
        Returns the retries of retries.

        Args:
            self: (todo): write your description
        """
        return self._retries

    @property
    def non_repeaters(self):
        """
        Returns an iterable non - zero list of non - empty_repeaters.

        Args:
            self: (todo): write your description
        """
        return self._non_repeaters

    @property
    def max_repetitions(self):
        """
        Returns the maximum number of maximum number of maximum maximum maximum maximum number.

        Args:
            self: (todo): write your description
        """
        return self._max_repetitions

    @property
    def x509_secure_connection(self):
        """
        Gets the x509 api client.

        Args:
            self: (todo): write your description
        """
        return self._x509_secure_connection

    @property
    def x509_cert_file(self):
        """
        The x509 certificate file : return : return :

        Args:
            self: (todo): write your description
        """
        return self._x509_certificate_file

    @property
    def x509_key_file(self):
        """
        The x509 key file : return : class :.

        Args:
            self: (todo): write your description
        """
        return self._x509_key_file
