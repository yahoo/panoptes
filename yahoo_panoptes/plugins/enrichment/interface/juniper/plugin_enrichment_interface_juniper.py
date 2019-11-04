from yahoo_panoptes.plugins.enrichment.interface.plugin_enrichment_interface import PluginEnrichmentInterface

_PORT_SPEED_TABLE = {u'ge': 10 ** 9, u'xe': 10 ** 10, u'xle': 4 * 10 ** 10, u'et': 4 * 10 ** 10, u'fte': 4 * 10 ** 10}


class PluginEnrichmentJuniperInterface(PluginEnrichmentInterface):
    """
    InterfaceEnrichment class for Juniper devices.
    """
    def get_port_speed(self, index):
        """
        Gets the port speed enrichment value for the specified interface associated with the provided index

        Args:
            index (int): The index to look up the associated interface in self._interface_table

        Returns:
            integer: The port speed, based upon the interface name or copied from the configured_speed enrichment in
                     the default case
        """
        for port_speed_indicator in list(_PORT_SPEED_TABLE.keys()):
            if self._interface_table[index][u'interface_name'].startswith(port_speed_indicator):
                if port_speed_indicator == u'et':
                    if self.resource.resource_metadata[u'model'] in [u'QFX5200', u'QFX10000']:
                        return 10 ** 11
                return _PORT_SPEED_TABLE[port_speed_indicator]
        return super(PluginEnrichmentJuniperInterface, self).get_port_speed(index)

    def get_parent_interface_name(self, index):
        """
        Gets the parent interface name for the interface associated with the provided index

        Args:
            index (int): The index used to look up the associated interface in self._interface_table

        Returns:
            string: The name of the parent interface, or self._MISSING_VALUE_STRING if the interface has no parent.
                    For Juniper devices, this is everything to the left of the '.' in the interface name, if a '.' is
                    present.
        """
        interface_name = self.get_interface_name(index)
        if u':' in interface_name:
            if u'.' in interface_name:
                parent_interface_name = interface_name.split(u'.')[0]
            else:
                parent_interface_name = interface_name.split(u':')[0]
            return parent_interface_name
        elif u'.' in interface_name:
            parent_interface_name = interface_name.split(u'.')[0]
        else:
            parent_interface_name = self._MISSING_VALUE_STRING
        return parent_interface_name
