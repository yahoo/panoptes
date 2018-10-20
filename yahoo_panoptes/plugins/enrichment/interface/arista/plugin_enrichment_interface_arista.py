from yahoo_panoptes.plugins.enrichment.interface.plugin_enrichment_interface import PluginEnrichmentInterface


class PluginEnrichmentAristaInterface(PluginEnrichmentInterface):
    """
    InterfaceEnrichment class for Arista devices.
    """
    def get_parent_interface_name(self, index):
        """
        Gets the parent interface name for the interface associated with the provided index

        Args:
            index (int): The index used to look up the associated interface in self._interface_table

        Returns:
            string: The name of the parent interface, or self._MISSING_VALUE_STRING if the interface has no parent.
                    For Arista devices, this is everything to the left of the '/' in the interface name, if a '/' is
                    present.
        """
        interface_name = self.get_interface_name(index)
        if '/' in interface_name:
            return interface_name.split('/')[0]
        return self._MISSING_VALUE_STRING

    def get_parent_interface_configured_speed(self, index):
        parent_name = self.get_parent_interface_name(index)
        if parent_name is not self._MISSING_VALUE_STRING:
            return 4 * self.get_configured_speed(index)
        return self._MISSING_METRIC_VALUE

    def get_parent_interface_media_type(self, index):
        return self.get_media_type(index)

    def get_parent_interface_port_speed(self, index):
        return self.get_parent_interface_configured_speed(index)
