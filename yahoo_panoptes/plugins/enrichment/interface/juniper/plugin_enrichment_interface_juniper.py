from .....plugins.enrichment.interface.plugin_enrichment_interface import PluginEnrichmentInterface

_PORT_SPEED_TABLE = {'ge': 10 ** 9, 'xe': 10 ** 10, 'xle': 4 * 10 ** 10, 'et': 4 * 10 ** 10, 'fte': 4 * 10 ** 10}


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
        if self._interface_table[index]['interface_name'] is None:
            self._interface_table[index]['interface_name'] = self.get_interface_name(index)
        for port_speed_indicator in _PORT_SPEED_TABLE.keys():
            if self._interface_table[index]['interface_name'].startswith(port_speed_indicator):
                if port_speed_indicator == 'et':
                    if self.resource.resource_metadata['model'] in ['QFX5200', 'QFX10000']:
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
        if ':' in interface_name:
            if '.' in interface_name:
                parent_interface_name = interface_name.split('.')[0]
            else:
                parent_interface_name = interface_name.split(':')[0]
            return parent_interface_name
        elif '.' in interface_name:
            parent_interface_name = interface_name.split('.')[0]
        else:
            parent_interface_name = self._MISSING_VALUE_STRING
        return parent_interface_name
