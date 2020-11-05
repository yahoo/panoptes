from yahoo_panoptes.enrichment.enrichment_plugin import PanoptesEnrichmentPlugin
from yahoo_panoptes.enrichment.schema.interface import PanoptesInterfaceEnrichmentGroup
from yahoo_panoptes.framework.enrichment import PanoptesEnrichmentSet, PanoptesEnrichmentGroupSet
from yahoo_panoptes.framework.plugins.base_snmp_plugin import PanoptesSNMPBaseEnrichmentPlugin
from yahoo_panoptes.framework.utilities.helpers import transform_octet_to_mac
from yahoo_panoptes.framework.utilities.snmp.mibs.ifTable import getIfTypeDesc, ifDescr, ifType, ifSpeed, ifPhysAddress
from yahoo_panoptes.framework.utilities.snmp.mibs.ifXTable import ifAlias, ifName, ifHighSpeed


class PluginEnrichmentInterface(PanoptesSNMPBaseEnrichmentPlugin, PanoptesEnrichmentPlugin):
    _MISSING_VALUE_STRING = u'<not set>'
    _MISSING_METRIC_VALUE = -1
    interface_enrichment_oids = [ifType, ifDescr, ifName, ifAlias, ifHighSpeed, ifSpeed, ifPhysAddress]

    def __init__(self):
        """
        Initialize the interface table.

        Args:
            self: (todo): write your description
        """
        super(PluginEnrichmentInterface, self).__init__()
        self._interface_enrichment_group = None
        self._interface_enrichment_group_set = None
        self._enrichments_map = dict()
        self._interface_table = dict()

    def _build_enrichments_map(self):
        """
        Build snmp snmp snmp configuration.

        Args:
            self: (todo): write your description
        """
        self._enrichments_map = dict()
        for enrichment_oid in self.interface_enrichment_oids:
            for varbind in self._snmp_connection.bulk_walk(enrichment_oid,
                                                           max_repetitions=self.snmp_configuration.max_repetitions):
                self._enrichments_map[varbind.oid + '.' + varbind.index] = varbind.value

    def get_interface_name(self, index):
        """
        Return the interface name of an interface.

        Args:
            self: (todo): write your description
            index: (int): write your description
        """
        # Different devices vendors return different data types for ifname
        # http://cric.grenoble.cnrs.fr/Administrateurs/Outils/MIBS/?oid=1.3.6.1.2.1.31.1.1.1.1 (DisplayString)
        # https://apps.juniper.net/mib-explorer/search.jsp#object=ifname&product=Junos%20OS&release=19.3R1 (OCTET STRING)
        ifname = self._enrichments_map.get(ifName + '.' + index, self._MISSING_VALUE_STRING)
        # TODO: Get snmpsim to return octet strings for some test cases
        if isinstance(ifname, bytes):
            return ifname.decode(u'ascii', u'ignore')  # pragma: no cover

        return ifname

    @property
    def host(self):
        """
        Return the host.

        Args:
            self: (todo): write your description
        """
        return self._host

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
        """
        Get the description of the given index.

        Args:
            self: (todo): write your description
            index: (int): write your description
        """
        description = self._enrichments_map.get(ifDescr + '.' + index, self._MISSING_VALUE_STRING)
        # TODO: Get snmpsim to return octet strings for some test cases
        if isinstance(description, bytes):
            return description.decode(u'ascii', u'ignore') # pragma: no cover

        return description

    def get_media_type(self, index):
        """
        Returns the media type for the given index.

        Args:
            self: (todo): write your description
            index: (int): write your description
        """
        type_index = self._enrichments_map.get(ifType + '.' + index)
        return getIfTypeDesc(type_index) if type_index is not None else self._MISSING_VALUE_STRING

    def get_alias(self, index):
        """
        Get an alias for the given index.

        Args:
            self: (todo): write your description
            index: (int): write your description
        """
        alias = self._enrichments_map.get(ifAlias + '.' + index, self._MISSING_VALUE_STRING)
        # TODO: Get snmpsim to return octet strings for some test cases
        if isinstance(alias, bytes):
            alias = alias.decode(u'ascii', u'ignore') # pragma: no cover

        return alias if len(alias) > 0 else self._MISSING_VALUE_STRING

    def get_if_speed(self, index):
        """
        Gets the speed for the specified index.

        Args:
            self: (todo): write your description
            index: (int): write your description
        """
        return int(self._enrichments_map.get(ifSpeed + '.' + index, self._MISSING_METRIC_VALUE))

    def get_configured_speed(self, index):
        """
        Gets the speed speed.

        Args:
            self: (todo): write your description
            index: (int): write your description
        """
        high_speed = self._enrichments_map.get(ifHighSpeed + '.' + index)
        if high_speed in [u'0', u'1', None]:
            speed = self.get_if_speed(index)
            return int(speed) if speed != 0 else (int(high_speed) * 1000000 if high_speed is not None
                                                  else self._MISSING_METRIC_VALUE)
        else:
            return int(high_speed) * 1000000  # Mbps to bps

    def get_port_speed(self, index):
        """
        Gets the speed of the specified index.

        Args:
            self: (todo): write your description
            index: (int): write your description
        """
        return self.get_configured_speed(index)

    def get_physical_address(self, index):
        """
        Returns physical physical physical physical physical physical physical address

        Args:
            self: (todo): write your description
            index: (int): write your description
        """
        physical_address = self._enrichments_map.get(ifPhysAddress + '.' + index, self._MISSING_VALUE_STRING)

        try:
            return transform_octet_to_mac(physical_address) if physical_address not in [None, u""] else \
                self._MISSING_VALUE_STRING
        except:
            return self._MISSING_VALUE_STRING

    @property
    def interface_table(self):
        """
        Return the interface table.

        Args:
            self: (todo): write your description
        """
        return self._interface_table

    def _build_interface_table(self):
        """
        Build the interface table.

        Args:
            self: (todo): write your description
        """
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
        """
        Return the interface index of the given interface

        Args:
            self: (todo): write your description
            index: (int): write your description
        """
        parent_interface_name = self.get_parent_interface_name(index)
        return self._get_index_from_interface_name(parent_interface_name)

    def _get_index_from_interface_name(self, name):
        """
        Returns the index of the interface name.

        Args:
            self: (todo): write your description
            name: (str): write your description
        """
        for index in list(self._interface_table.keys()):
            if name == self._interface_table[index][u'interface_name']:
                return index
        return None

    def _add_parent_interface_enrichments(self):
        """
        R add parent interface interfaces.

        Args:
            self: (todo): write your description
        """
        for oid in list(self._interface_table.keys()):
            index = oid.split(u'.')[-1]
            self._interface_table[index][u'parent_interface_name'] = self.get_parent_interface_name(index)
            self._interface_table[index][u'parent_interface_media_type'] = self.get_parent_interface_media_type(index)
            self._interface_table[index][u'parent_interface_configured_speed'] = \
                self.get_parent_interface_configured_speed(index)
            self._interface_table[index][u'parent_interface_port_speed'] = self.get_parent_interface_port_speed(index)

    def get_results(self):
        """
        Returns the results of this interface.

        Args:
            self: (todo): write your description
        """
        self._interface_enrichment_group = PanoptesInterfaceEnrichmentGroup(enrichment_ttl=self.enrichment_ttl,
                                                                            execute_frequency=self.execute_frequency)
        self._interface_enrichment_group_set = PanoptesEnrichmentGroupSet(self.resource)

        self._build_enrichments_map()
        self._build_interface_table()
        self._add_parent_interface_enrichments()

        for index, enrichment_set in list(self.interface_table.items()):
            try:
                self._interface_enrichment_group.add_enrichment_set(PanoptesEnrichmentSet(str(index), enrichment_set))
            except Exception as e:
                self._logger.error(u'Error while adding enrichment set {} to enrichment group for the device {}: {}'.
                                   format(str(index), self.host, repr(e)))

        self._interface_enrichment_group_set.add_enrichment_group(self._interface_enrichment_group)

        self._logger.debug(u'Interface enrichment for device {} PanoptesEnrichmentGroupSet'.
                           format(self.host))

        return self._interface_enrichment_group_set
