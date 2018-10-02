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
        super(PluginEnrichmentInterface, self).__init__()
        self._interface_enrichment_group = None
        self._interface_enrichment_group_set = None
        self._enrichments_map = dict()
        self._interface_table = dict()

    def _build_enrichments_map(self):
        self._enrichments_map = dict()
        for enrichment_oid in self.interface_enrichment_oids:
            for varbind in self._snmp_connection.bulk_walk(enrichment_oid,
                                                           max_repetitions=self.snmp_configuration.max_repetitions):
                self._enrichments_map[varbind.oid + '.' + varbind.index] = varbind.value

    def get_interface_name(self, index):
        return self._enrichments_map.get(ifName + '.' + index, self._MISSING_VALUE_STRING)

    @property
    def host(self):
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
            return self._interface_table[parent_index]['media_type']
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
            return self._interface_table[parent_index]['port_speed']
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
            return self._interface_table[parent_index]['configured_speed']
        else:
            return self._MISSING_METRIC_VALUE

    def get_description(self, index):
        return self._enrichments_map.get(ifDescr + '.' + index, self._MISSING_VALUE_STRING)

    def get_media_type(self, index):
        type_index = self._enrichments_map.get(ifType + '.' + index)
        return getIfTypeDesc(type_index) if type_index is not None else self._MISSING_VALUE_STRING

    def get_alias(self, index):
        alias = self._enrichments_map.get(ifAlias + '.' + index, self._MISSING_VALUE_STRING)
        return alias if len(alias) > 0 else self._MISSING_VALUE_STRING

    def get_if_speed(self, index):
        return int(self._enrichments_map.get(ifSpeed + '.' + index, self._MISSING_METRIC_VALUE))

    def get_configured_speed(self, index):
        high_speed = self._enrichments_map.get(ifHighSpeed + '.' + index)
        if high_speed in ['0', '1', None]:
            speed = self.get_if_speed(index)
            return int(speed) if speed != 0 else (int(high_speed) * 1000000 if high_speed is not None
                                                  else self._MISSING_METRIC_VALUE)
        else:
            return int(high_speed) * 1000000  # Mbps to bps

    def get_port_speed(self, index):
        return self.get_configured_speed(index)

    def get_physical_address(self, index):
        physical_address = self._enrichments_map.get(ifPhysAddress + '.' + index, self._MISSING_VALUE_STRING)
        return transform_octet_to_mac(physical_address) if physical_address not in [None, ""] else \
            self._MISSING_VALUE_STRING

    @property
    def interface_table(self):
        return self._interface_table

    def _build_interface_table(self):
        for oid in self._enrichments_map.keys():
            index = oid.split('.')[-1]
            self._interface_table[index] = dict()
            self._interface_table[index]['interface_name'] = self.get_interface_name(index)
            self._interface_table[index]['description'] = self.get_description(index)
            self._interface_table[index]['media_type'] = self.get_media_type(index)
            self._interface_table[index]['alias'] = self.get_alias(index)
            self._interface_table[index]['configured_speed'] = self.get_configured_speed(index)
            self._interface_table[index]['port_speed'] = self.get_port_speed(index)
            self._interface_table[index]['physical_address'] = self.get_physical_address(index)

    def _get_parent_interface_index(self, index):
        parent_interface_name = self.get_parent_interface_name(index)
        return self._get_index_from_interface_name(parent_interface_name)

    def _get_index_from_interface_name(self, name):
        for index in self._interface_table.keys():
            if name == self._interface_table[index]['interface_name']:
                return index
        return None

    def _add_parent_interface_enrichments(self):
        for oid in self._interface_table.keys():
            index = oid.split('.')[-1]
            self._interface_table[index]['parent_interface_name'] = self.get_parent_interface_name(index)
            self._interface_table[index]['parent_interface_media_type'] = self.get_parent_interface_media_type(index)
            self._interface_table[index]['parent_interface_configured_speed'] = \
                self.get_parent_interface_configured_speed(index)
            self._interface_table[index]['parent_interface_port_speed'] = self.get_parent_interface_port_speed(index)

    def get_results(self):
        self._interface_enrichment_group = PanoptesInterfaceEnrichmentGroup(enrichment_ttl=self.enrichment_ttl,
                                                                            execute_frequency=self.execute_frequency)
        self._interface_enrichment_group_set = PanoptesEnrichmentGroupSet(self.resource)

        self._build_enrichments_map()
        self._build_interface_table()
        self._add_parent_interface_enrichments()

        for index, enrichment_set in self.interface_table.items():
            try:
                self._interface_enrichment_group.add_enrichment_set(PanoptesEnrichmentSet(str(index), enrichment_set))
            except Exception as e:
                self._logger.error('Error while adding enrichment set {} to enrichment group for the device {}: {}'.
                                   format(str(index), self.host, repr(e)))

        self._interface_enrichment_group_set.add_enrichment_group(self._interface_enrichment_group)

        self._logger.debug('Interface enrichment for device {} PanoptesEnrichmentGroupSet {}'.
                           format(self.host, self._interface_enrichment_group_set))

        return self._interface_enrichment_group_set
