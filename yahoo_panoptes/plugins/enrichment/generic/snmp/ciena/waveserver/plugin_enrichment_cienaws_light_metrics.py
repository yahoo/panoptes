"""
This module implements a Panoptes Plugin that can poll Ciena Waveserver devices for transceiver light level Metrics
"""

from cached_property import threaded_cached_property

from yahoo_panoptes.enrichment.schema.generic.snmp import PanoptesGenericSNMPMetricsEnrichmentGroup
from yahoo_panoptes.framework.enrichment import PanoptesEnrichmentSet
from yahoo.contrib.panoptes.plugins.enrichment.generic.snmp.plugin_enrichment_generic_snmp \
    import PanoptesEnrichmentGenericSNMPPlugin


MIB_CIENA_CHASSIS = '.1.3.6.1.4.1.1271'
cwsPortIdName = MIB_CIENA_CHASSIS + '.3.4.7.4.1.2'
cwsPtpPtpPropertiesXcvrType = MIB_CIENA_CHASSIS + '.3.4.8.6.1.2'
cwsXcvrRxPowerActual = MIB_CIENA_CHASSIS + '.3.4.8.11.1.2'
cwsXcvrTxPowerActual = MIB_CIENA_CHASSIS + '.3.4.8.13.1.2'


class CienaWSLightMetricsEnrichment(PanoptesGenericSNMPMetricsEnrichmentGroup):
    pass


class CienaPluginWSLightMetricsEnrichment(PanoptesEnrichmentGenericSNMPPlugin):
    def __init__(self):
        self._plugin_context = None
        self._logger = None
        self._ciena_model = None
        self._snmp_connection = None
        self._max_repetitions = None
        self._polling_execute_frequency = None
        super(CienaPluginWSLightMetricsEnrichment, self).__init__()

    @property
    def metrics_enrichment_class(self):
        return CienaWSLightMetricsEnrichment

    @threaded_cached_property
    def _xcvr_interfaces_id(self):

        varbinds_int_type = self._snmp_connection.bulk_walk(cwsPtpPtpPropertiesXcvrType)
        varbinds_interface = self._snmp_connection.bulk_walk(cwsPortIdName)
        interface_id = {}
        xcvr_index = []
        for varbind in varbinds_int_type:
            if varbind.value == '4':
                xcvr_index.append(varbind.index)
        for varbind_int in varbinds_interface:
            if varbind_int.index in xcvr_index:
                interface_id[varbind_int.index] = varbind_int.value
        return interface_id

    @threaded_cached_property
    def _xcvr_rx_power_levels(self):

        rx_power_actual = {}
        for ind, name in self._xcvr_interfaces_id.items():
            rx_oid = cwsXcvrRxPowerActual + '.' + ind.strip('.0')
            rx = self._snmp_connection.bulk_walk(rx_oid)
            for i in rx:
                rx_dbm = float(i.value) / 10
                rx_power_actual[name] = rx_dbm
        return rx_power_actual

    @threaded_cached_property
    def _xcvr_tx_power_levels(self):

        tx_power_actual = {}
        for ind, name in self._xcvr_interfaces_id.items():
            tx_oid = cwsXcvrTxPowerActual + '.' + ind.strip('.0')
            tx = self._snmp_connection.bulk_walk(tx_oid)
            for i in tx:
                tx_dbm = float(i.value) / 10
                tx_power_actual[name] = tx_dbm
        return tx_power_actual

    def _build_metrics_oids_map(self):
        self._oids_map = {
            "xcvr_interfaces": {
                "method": "static",
                "values": self._xcvr_interfaces_id
            },
            "rx_light_level": {
                "method": "bulk_walk",
                "oid": cwsXcvrRxPowerActual,
                "values": self._xcvr_rx_power_levels
            },
            "tx_light_level": {
                "method": "bulk_walk",
                "oid": cwsXcvrTxPowerActual,
                "values": self._xcvr_tx_power_levels
            }
        }

    def _build_metrics_groups_conf(self):
        self._metrics_groups = [
            {
                "group_name": "light_levels",
                "dimensions": {},
                "metrics": {
                    "xcvr_interfaces": {
                        "metric_type": "gauge",
                        "value": "xcvr_interfaces.$index"
                    },
                    "rx_light_level": {
                        "metric_type": "gauge",
                        "value": "rx_light_level.$index"
                    },
                    "tx_light_level": {
                        "metric_type": "gauge",
                        "value": "tx_light_level.$index"
                    }
                }
            }
        ]

    def get_enrichment(self):
        self._ciena_model = self._plugin_context.data.resource_metadata.get('model', 'unknown')
        self._build_metrics_oids_map()
        self._build_metrics_groups_conf()

        enrichment_set = {
            "oids": self.oids_map,
            "metrics_groups": self.metrics_groups
        }

        try:
            self.enrichment_group.add_enrichment_set(PanoptesEnrichmentSet(self.device_fqdn, enrichment_set))
        except Exception as e:
            self._logger.error('Error while adding enrichment set {} to enrichment group for the device {}: {}'.
                               format(enrichment_set, self.device_fqdn, repr(e)))

        self.enrichment_group_set.add_enrichment_group(self.enrichment_group)

        self._logger.debug('Metrics enrichment for device {}: {}'.format(self.device_fqdn, self.enrichment_group_set))

        return self.enrichment_group_set
