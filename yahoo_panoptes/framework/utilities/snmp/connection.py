import yahoo_panoptes_snmp as easysnmp
from yahoo_panoptes_snmp.exceptions import *

from .exceptions import *
from .variable import PanoptesSNMPVariable
from ...plugins.context import PanoptesPluginContext
from ...validators import PanoptesValidators

exception_class_mapping = {EasySNMPError: PanoptesSNMPException,
                           EasySNMPConnectionError: PanoptesSNMPConnectionException,
                           EasySNMPTimeoutError: PanoptesSNMPTimeoutException,
                           EasySNMPUnknownObjectIDError: PanoptesSNMPUnknownObjectIDException,
                           EasySNMPNoSuchNameError: PanoptesSNMPNoSuchNameException,
                           EasySNMPNoSuchObjectError: PanoptesSNMPNoSuchObjectException,
                           EasySNMPNoSuchInstanceError: PanoptesSNMPNoSuchInstanceException,
                           EasySNMPUndeterminedTypeError: PanoptesSNMPUndeterminedTypeException}


SNMP_ERRORS_MAP = {
    'ERROR': PanoptesSNMPException,
    'CONNECTION': PanoptesSNMPConnectionException,
    'TIMEOUT': PanoptesSNMPTimeoutException,
    'NOSUCHOBJECT': PanoptesSNMPNoSuchObjectException,
    'NOSUCHINSTANCE': PanoptesSNMPNoSuchInstanceException,
    'NOSUCHNAME': PanoptesSNMPNoSuchNameException,
    'UNKNOWNOBJECTID': PanoptesSNMPUnknownObjectIDException,
    'UNDETERMINEDTYPE': PanoptesSNMPUndeterminedTypeException
}


class PanoptesSNMPConnection(object):
    def __init__(self, host, port, timeout, retries):
        assert PanoptesValidators.valid_nonempty_string(host), 'host must a non-empty string'
        assert PanoptesValidators.valid_port(port), 'port must be an integer between 1 and 65535'
        assert PanoptesValidators.valid_nonzero_integer(timeout), 'timeout must be a integer greater than zero'
        assert PanoptesValidators.valid_positive_integer(retries), 'retries must a non-negative integer'

        self._host = host
        self._port = port
        self._timeout = timeout
        self._retries = retries
        self._easy_snmp_session = None

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
            return exception_class_mapping[type(easy_snmp_exception)](easy_snmp_exception.message)
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
        assert PanoptesValidators.valid_numeric_snmp_oid(oid), 'oid must be numeric string with a leading period'
        assert self._easy_snmp_session is not None, 'PanoptesSNMPSession not initialized'

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
        assert PanoptesValidators.valid_numeric_snmp_oid(oid), 'oid must be numeric string with a leading period'
        assert PanoptesValidators.valid_positive_integer(non_repeaters), 'non_repeaters must a positive integer'
        assert PanoptesValidators.valid_positive_integer(max_repetitions), 'max_repetitions must a positive integer'
        assert (base_oid is None) or PanoptesValidators.valid_numeric_snmp_oid(
                base_oid), 'oid must be numeric string with a leading period'
        assert self._easy_snmp_session is not None, 'PanoptesSNMPSession not initialized'

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
        assert PanoptesValidators.valid_numeric_snmp_oid(oid), 'oid must be numeric string with a leading period'
        assert PanoptesValidators.valid_positive_integer(non_repeaters), 'non_repeaters must a positive integer'
        assert PanoptesValidators.valid_positive_integer(max_repetitions), 'max_repetitions must a positive integer'
        assert self._easy_snmp_session is not None, 'PanoptesSNMPSession not initialized'

        base_oid = oid
        varbinds = list()
        running = 1

        while running:
            results = self.get_bulk(oid=oid, non_repeaters=non_repeaters,
                                    max_repetitions=max_repetitions, base_oid=base_oid)
            varbinds.extend(results)
            if len(results) < max_repetitions:
                break
            oid = base_oid + '.' + results[-1].index

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
        assert PanoptesValidators.valid_nonempty_string(community), 'community_string must a non-empty string'

        self._community = community

        try:
            self._easy_snmp_session = easysnmp.Session(hostname=self._host, remote_port=self._port,
                                                       timeout=self._timeout, retries=self._retries,
                                                       version=2, use_numeric=True,
                                                       community=self._community)

        except Exception as e:
            raise self._wrap_panoptes_snmp_exception(e)


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
        assert PanoptesValidators.valid_nonempty_string(context), 'context must be a non-empty string'
        assert PanoptesValidators.valid_nonempty_string(our_identity), 'our_identity must be a non-empty string'
        assert PanoptesValidators.valid_nonempty_string(their_identity), 'their_identity must be a non-empty string'

        self._context = context
        self._our_identity = our_identity
        self._their_identity = their_identity

        try:
            self._easy_snmp_session = easysnmp.Session(hostname=self._host, remote_port=self._port,
                                                       timeout=self._timeout, retries=self._retries,
                                                       version=3, use_numeric=True,
                                                       transport='tlstcp',
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
        assert isinstance(plugin_context,
                          PanoptesPluginContext), 'plugin_context must be a class or subclass of ' \
                                                  'PanoptesPluginContext'

        self._plugin_context = plugin_context
        self._plugin_snmp_configuration = plugin_context.config.get('snmp', {})
        self._default_snmp_configuration = plugin_context.snmp

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

        self._parse_snmp_configuration()
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
        self._community = self._plugin_snmp_configuration.get('community')

        if self._community:
            assert PanoptesValidators.valid_nonempty_string(
                    self._community), 'SNMP community must be a non-empty string'
            return

        # Else lookup the site-specific community string
        site = self._plugin_context.data.resource_site

        try:
            logger.debug('Going to get fetch SNMP community string using key "{}" for site "{}"'.format(
                    self.community_string_key, site))
            self._community = self._plugin_context.secrets.get_by_site(self.community_string_key, site)
        except Exception as e:
            raise PanoptesSNMPException(
                    'Could not fetch SNMP community string for site "{}" using key "{}": {}'.format(
                            site, self.community_string_key, repr(e)))

        if self._community:
            assert PanoptesValidators.valid_nonempty_string(
                    self._community), 'SNMP community must be a non-empty string'
            return

        # Else return default community string
        self._community = self._default_snmp_configuration.get('community')

        assert PanoptesValidators.valid_nonempty_string(
                self._community), 'SNMP community must be a non-empty string'

    def _parse_snmp_configuration(self):
        self._connection_factory_module = self._plugin_snmp_configuration.get('connection_factory_module',
                                                                              self._default_snmp_configuration[
                                                                                  'connection_factory_module'])
        assert PanoptesValidators.valid_nonempty_string(
                self._connection_factory_module), 'SNMP connection factory module must be a non-empty string'

        self._connection_factory_class = self._plugin_snmp_configuration.get('connection_factory_class',
                                                                             self._default_snmp_configuration[
                                                                                 'connection_factory_class'])
        assert PanoptesValidators.valid_nonempty_string(
                self._connection_factory_class), 'SNMP connection factory class must be a non-empty string'

        self._community_string_key = self._plugin_snmp_configuration.get('community_string_key',
                                                                         self._default_snmp_configuration.get(
                                                                                 'community_string_key'))
        assert PanoptesValidators.valid_nonempty_string(
                self._community_string_key), 'SNMP community string key must be a non-empty string'

        self._port = int(
                self._plugin_snmp_configuration.get('port', self._default_snmp_configuration['port']))
        assert PanoptesValidators.valid_port(self._port), 'SNMP port must be a valid TCP/UDP port number'

        self._proxy_port = int(
                self._plugin_snmp_configuration.get('proxy_port', self._default_snmp_configuration['proxy_port']))
        assert PanoptesValidators.valid_port(self._proxy_port), 'SNMP proxy port must be a valid TCP/UDP port number'

        self._timeout = int(
                self._plugin_snmp_configuration.get('timeout', self._default_snmp_configuration['timeout']))
        assert PanoptesValidators.valid_nonzero_integer(
                self._timeout), 'SNMP timeout must be a positive integer'

        self._retries = int(
                self._plugin_snmp_configuration.get('retries', self._default_snmp_configuration['retries']))
        assert PanoptesValidators.valid_nonzero_integer(
                self._retries), 'SNMP retries must be a positive integer'

        self._non_repeaters = int(self._plugin_snmp_configuration.get('non_repeaters',
                                                                      self._default_snmp_configuration[
                                                                          'non_repeaters']))
        assert PanoptesValidators.valid_positive_integer(
                self._non_repeaters), 'SNMP non-repeaters must be a positive integer'

        self._max_repetitions = int(
                self._plugin_snmp_configuration.get('max_repetitions',
                                                    self._default_snmp_configuration['max_repetitions']))
        assert PanoptesValidators.valid_nonzero_integer(
                self._max_repetitions), 'SNMP max-repetitions must be a positive integer'

    @property
    def connection_factory_module(self):
        return self._connection_factory_module

    @property
    def connection_factory_class(self):
        return self._connection_factory_class

    @property
    def community_string_key(self):
        return self._community_string_key

    @property
    def community(self):
        return self._community

    @property
    def port(self):
        return self._port

    @property
    def proxy_port(self):
        return self._proxy_port

    @property
    def timeout(self):
        return self._timeout

    @property
    def retries(self):
        return self._retries

    @property
    def non_repeaters(self):
        return self._non_repeaters

    @property
    def max_repetitions(self):
        return self._max_repetitions


class PanoptesSNMPConnectionFactory(object):
    """
    This class returns an SNMP connection object. Currently only supports V2 connection objects
    """
    @staticmethod
    def get_snmp_connection(plugin_context, snmp_configuration, community_suffix=None):
        assert isinstance(plugin_context,
                          PanoptesPluginContext), 'plugin_context must be a class or subclass of ' \
                                                  'PanoptesPluginContext'
        assert isinstance(snmp_configuration,
                          PanoptesSNMPPluginConfiguration), 'plugin_context must be a class or subclass of ' \
                                                            'PanoptesSNMPPluginConfiguration'

        host = plugin_context.data.resource_endpoint
        community = snmp_configuration.community

        try:
            if community_suffix:
                community += '@' + str(community_suffix)

            return PanoptesSNMPV2Connection(host=host, port=snmp_configuration.port,
                                            timeout=snmp_configuration.timeout,
                                            retries=snmp_configuration.retries,
                                            community=community)
        except Exception as e:
            raise PanoptesSNMPConnectionException(
                    'Error while creating SNMP connection for device "{}": {}'.format(host, repr(e)))
