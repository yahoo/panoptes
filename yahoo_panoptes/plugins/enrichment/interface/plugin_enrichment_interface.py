from builtins import object
import time
import requests

from yahoo_panoptes.enrichment.enrichment_plugin import PanoptesEnrichmentPlugin, PanoptesEnrichmentPluginError
from yahoo_panoptes.framework.enrichment import PanoptesEnrichmentSet, PanoptesEnrichmentGroupSet
from yahoo_panoptes.framework.utilities.helpers import transform_octet_to_mac
from yahoo_panoptes.framework.utilities.snmp.mibs.ifTable import getIfTypeDesc, ifDescr, ifType, ifSpeed, \
    ifPhysAddress
from yahoo_panoptes.framework.utilities.snmp.mibs.ifXTable import ifAlias, ifName, ifHighSpeed
from yahoo_panoptes.plugins.helpers.snmp_connections import PanoptesSNMPConnectionFactory
from yahoo_panoptes.enrichment.schema.interface import PanoptesInterfaceEnrichmentGroup


class InterfaceEnrichment(object):
    _MISSING_VALUE_STRING = u'<not set>'
    _MISSING_METRIC_VALUE = -1

    def __init__(self, plugin_context, device_resource, interface_enrichment_oids):
        self._plugin_context = plugin_context
        self._logger = plugin_context.logger
        self._plugin_conf = plugin_context.config
        try:
            self._execute_frequency = int(self._plugin_conf[u'main'][u'execute_frequency'])
            self._enrichment_ttl = int(self._plugin_conf[u'main'][u'enrichment_ttl'])
            self._snmp_max_repetitions = int(self._plugin_conf[u'snmp'][u'max_repetitions'])
            self._snmp_timeout = int(self._plugin_conf[u'snmp'][u'timeout'])
            self._snmp_retries = int(self._plugin_conf[u'snmp'][u'retries'])
        except Exception as e:
            raise PanoptesEnrichmentPluginError(
                    u'Either required configurations not specified or not an integer: %s' % repr(e))

        self._device_resource = device_resource
        self._device_fqdn = device_resource.resource_endpoint
        self._interface_enrichment_group = PanoptesInterfaceEnrichmentGroup(enrichment_ttl=self._enrichment_ttl,
                                                                            execute_frequency=self._execute_frequency)
        self._interface_enrichment_group_set = PanoptesEnrichmentGroupSet(device_resource)
        self._interface_enrichment_oids = interface_enrichment_oids
        self._snmp_connection = None
        self._enrichments_map = dict()
        self._interface_table = dict()
        self._session = requests.Session()

    def _build_enrichments_map(self):
        self._enrichments_map = dict()
        for enrichment_oid in self._interface_enrichment_oids:
            for varbind in self._snmp_connection.bulk_walk(enrichment_oid, max_repetitions=self._snmp_max_repetitions):
                self._enrichments_map[varbind.oid + u'.' + varbind.index] = varbind.value

    def get_interface_name(self, index):
        # Different devices vendors return different data types for ifname
        # http://cric.grenoble.cnrs.fr/Administrateurs/Outils/MIBS/?oid=1.3.6.1.2.1.31.1.1.1.1 (DisplayString)
        # https://apps.juniper.net/mib-explorer/search.jsp#object=ifname&product=Junos%20OS&release=19.3R1
        # (OCTET STRING)
        ifname = self._enrichments_map.get(ifName + '.' + index, self._MISSING_VALUE_STRING)
        # TODO: Get snmpsim to return octet strings for some test cases
        if isinstance(ifname, bytes):
            return ifname.decode(u'ascii', u'ignore')  # pragma: no cover

        return ifname

    @property
    def device_fqdn(self):
        return self._device_fqdn

    def get_parent_interface_name(self, index):
        """
        Gets the parent interface name for the interface associated with the provided index

        Args:
            index (int): The index used to look up the associated interface in self._interface_table

        Returns:
            string: The name of the parent interface, or self._MISSING_VALUE_STRING if the interface has no parent.
                    For Cisco devices, this is everything to the left of the '.' in the interface name, if a '.' is
                    present.
        """
        return self._MISSING_VALUE_STRING

    def get_parent_interface_media_type(self, index):
        """
        Gets the parent interface media type for the interface associated with the provided index

        Args:
            index (int): The index used to look up the associated interface in self._interface_table

        Returns:
            string: The media_type of the parent interface, or self._MISSING_VALUE_STRING if the interface
            has no parent.
        """
        parent_index = self._get_parent_interface_index(index)
        if parent_index:
            return self._interface_table[parent_index][u'media_type']
        else:
            return self._MISSING_VALUE_STRING

    def get_parent_interface_port_speed(self, index):
        """
        Gets the parent interface port speed for the interface associated with the provided index

        Args:
            index (int): The index used to look up the associated interface in self._interface_table

        Returns:
            integer: The port speed of the parent interface, or self._MISSING_METRIC_VALUE if the interface
            has no parent.
        """
        parent_index = self._get_parent_interface_index(index)
        if parent_index:
            return self._interface_table[parent_index][u'port_speed']
        else:
            return self._MISSING_METRIC_VALUE

    def get_parent_interface_configured_speed(self, index):
        """
        Gets the parent interface configured speed for the interface associated with the provided index

        Args:
            index (int): The index used to look up the associated interface in self._interface_table

        Returns:
            integer: The configured speed of the parent interface, or self._MISSING_METRIC_VALUE if the interface
            has no parent.
        """
        parent_index = self._get_parent_interface_index(index)
        if parent_index:
            return self._interface_table[parent_index][u'configured_speed']
        else:
            return self._MISSING_METRIC_VALUE

    def get_description(self, index):
        description = self._enrichments_map.get(ifDescr + '.' + index, self._MISSING_VALUE_STRING)
        # TODO: Get snmpsim to return octet strings for some test cases
        if isinstance(description, bytes):
            return description.decode(u'ascii', u'ignore')  # pragma: no cover

        return description

    def get_media_type(self, index):
        type_index = self._enrichments_map.get(ifType + u'.' + index)
        return getIfTypeDesc(type_index) if type_index is not None else self._MISSING_VALUE_STRING

    def get_alias(self, index):
        alias = self._enrichments_map.get(ifAlias + '.' + index, self._MISSING_VALUE_STRING)
        # TODO: Get snmpsim to return octet strings for some test cases
        if isinstance(alias, bytes):
            alias = alias.decode(u'ascii', u'ignore')  # pragma: no cover

        return alias if len(alias) > 0 else self._MISSING_VALUE_STRING

    def get_if_speed(self, index):
        return int(self._enrichments_map.get(ifSpeed + u'.' + index, self._MISSING_METRIC_VALUE))

    def get_configured_speed(self, index):
        high_speed = self._enrichments_map.get(ifHighSpeed + u'.' + index)
        # TODO Should this actually be if 0 <= high_speed < 4294 (i.e. floor(2^32 / 10^6))?
        if high_speed in [u'0', u'1', None]:
            speed = self.get_if_speed(index)
            return int(speed) if speed != 0 else (int(high_speed) * 1000000 if high_speed is not None
                                                  else self._MISSING_METRIC_VALUE)
        else:
            return int(high_speed) * 1000000  # Mbps to bps

    def get_port_speed(self, index):
        return self.get_configured_speed(index)

    def get_physical_address(self, index):
        physical_address = self._enrichments_map.get(ifPhysAddress + u'.' + index, self._MISSING_VALUE_STRING)
        return transform_octet_to_mac(physical_address) if physical_address not in [None, ""] else \
            self._MISSING_VALUE_STRING

    @property
    def interface_table(self):
        return self._interface_table

    def _build_interface_table(self):
        for oid in list(self._enrichments_map.keys()):
            index = oid.split(u'.')[-1]
            self._interface_table[index] = dict()
            self._interface_table[index][u'interface_name'] = self.get_interface_name(index)
            self._interface_table[index][u'description'] = self.get_description(index)
            self._interface_table[index][u'media_type'] = self.get_media_type(index)
            self._interface_table[index][u'alias'] = self.get_alias(index)
            self._interface_table[index][u'configured_speed'] = self.get_configured_speed(index)
            self._interface_table[index][u'port_speed'] = self.get_port_speed(index)
            self._interface_table[index][u'physical_address'] = self.get_physical_address(index)

    def _get_parent_interface_index(self, index):
        parent_interface_name = self.get_parent_interface_name(index)
        return self._get_index_from_interface_name(parent_interface_name)

    def _get_index_from_interface_name(self, name):
        for index in list(self._interface_table.keys()):
            if name == self._interface_table[index][u'interface_name']:
                return index
        return None

    def _add_parent_interface_enrichments(self):
        for oid in list(self._interface_table.keys()):
            index = oid.split(u'.')[-1]
            self._interface_table[index][u'parent_interface_name'] = self.get_parent_interface_name(index)
            self._interface_table[index][u'parent_interface_media_type'] = self.get_parent_interface_media_type(index)
            self._interface_table[index][u'parent_interface_configured_speed'] = \
                self.get_parent_interface_configured_speed(index)
            self._interface_table[index][u'parent_interface_port_speed'] = self.get_parent_interface_port_speed(index)

    def get_enrichment(self):
        try:
            self._snmp_connection = PanoptesSNMPConnectionFactory.get_snmp_connection(
                plugin_context=self._plugin_context, resource=self._device_resource,
                timeout=self._snmp_timeout, retries=self._snmp_retries)
        except Exception as e:
            raise PanoptesEnrichmentPluginError(u'Error while creating snmp connection for the device {}: {}'.
                                                format(self.device_fqdn, repr(e)))

        self._build_enrichments_map()
        self._build_interface_table()
        self._add_parent_interface_enrichments()

        for index, enrichment_set in list(self.interface_table.items()):
            try:
                self._interface_enrichment_group.add_enrichment_set(PanoptesEnrichmentSet(str(index), enrichment_set))
            except Exception as e:
                self._logger.error(u'Error while adding enrichment set {} to enrichment group for the device {}: {}'.
                                   format(str(index), self.device_fqdn, repr(e)))

        self._interface_enrichment_group_set.add_enrichment_group(self._interface_enrichment_group)

        self._logger.debug(u'Interface enrichment for device {} PanoptesEnrichmentGroupSet {}'.
                           format(self.device_fqdn, self._interface_enrichment_group_set))

        return self._interface_enrichment_group_set


class PluginEnrichmentInterface(PanoptesEnrichmentPlugin):
    interface_enrichment_class = InterfaceEnrichment

    def run(self, context):
        """
        The main entry point to the plugin

        Args:
            context (PanoptesPluginContext): The Plugin Context passed by the Plugin Agent

        Returns:
            PanoptesEnrichmentGroupSet: A non-empty resource set

        Raises:
            PanoptesEnrichmentPluginError: This exception is raised if any part of the metrics process has errors
        """
        logger = context.logger

        start_time = time.time()

        device_resource = context.data

        logger.info(u'Going to poll resource "%s" for interface enrichment' % device_resource.resource_endpoint)

        interface_enrichment_oids = [ifType, ifDescr, ifName, ifAlias, ifHighSpeed, ifSpeed, ifPhysAddress]

        device_polling = self.interface_enrichment_class(plugin_context=context, device_resource=device_resource,
                                                         interface_enrichment_oids=interface_enrichment_oids)

        device_results = device_polling.get_enrichment()

        end_time = time.time()

        if device_results:
            logger.info(
                u'Done polling interface enrichment for resource "%s" in %.2f seconds, %s elements' % (
                    device_resource.resource_endpoint, end_time - start_time, len(device_results)))
        else:
            logger.warn(u'Error polling interface enrichment for resource %s' % device_resource.resource_endpoint)

        return device_results
