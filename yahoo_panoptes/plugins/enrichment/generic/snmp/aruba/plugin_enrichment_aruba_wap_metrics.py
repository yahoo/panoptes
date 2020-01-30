"""
This module implements a Panoptes Plugin that can poll Aruba devices for  WAP Metrics
"""

from cached_property import threaded_cached_property
from yahoo_panoptes.framework.enrichment import PanoptesEnrichmentSet

from yahoo_panoptes.plugins.enrichment.generic.snmp.plugin_enrichment_generic_snmp import \
    PanoptesEnrichmentGenericSNMPPlugin

ENTITY_MIB_PREFIX = '.1.3.6.1.4.1.14823'
controllerTotalNumOfUsers = ENTITY_MIB_PREFIX + '.2.2.1.4.1.1'
controllerTotalNumOfAccessPoints = ENTITY_MIB_PREFIX + '.2.2.1.1.3.1'
radioNumAssociatedClients = ENTITY_MIB_PREFIX + '.2.2.1.5.2.1.5.1.7'
radioUtilizationPercentage = ENTITY_MIB_PREFIX + '.2.2.1.5.2.1.5.1.6'
accessPointChannelNumStations = ENTITY_MIB_PREFIX + '.2.2.1.5.3.1.6.1.2'
accessPointChannelTotalPkts = ENTITY_MIB_PREFIX + '.2.2.1.5.3.1.6.1.3'
accessPointChannelNoise = ENTITY_MIB_PREFIX + '.2.2.1.5.3.1.6.1.9'
accessPointChannelRxUtilization = ENTITY_MIB_PREFIX + '.2.2.1.5.3.1.6.1.35'
accessPointChannelTxUtilization = ENTITY_MIB_PREFIX + '.2.2.1.5.3.1.6.1.36'
accessPointChannelUtilization = ENTITY_MIB_PREFIX + '.2.2.1.5.3.1.6.1.37'
accessPointIpAddress = ENTITY_MIB_PREFIX + '.2.2.1.5.2.1.4.1.2'
accessPointName = ENTITY_MIB_PREFIX + '.2.2.1.5.2.1.4.1.3'
accessPointStatus = ENTITY_MIB_PREFIX + '.2.2.1.5.2.1.4.1.19'


# This function will be added to the helpers.py in yahoo_panoptes next time the code is updated.
# In the meantime 'convert_if_bytes' will be defined locally.
# This is a temp function
def convert_if_bytes(maybe_bytes):
    return maybe_bytes.decode(u'utf-8', u'ignore') \
        if isinstance(maybe_bytes, bytes) else str(maybe_bytes)


class ArubaPluginEnrichmentWapMetrics(PanoptesEnrichmentGenericSNMPPlugin):

    def __init__(self):
        self._plugin_context = None
        self._logger = None
        self._aruba_model = None
        self._snmp_connection = None
        self._max_repetitions = None
        self._polling_execute_frequency = None

        super(ArubaPluginEnrichmentWapMetrics, self).__init__()

    @threaded_cached_property
    def _access_point_info(self):
        """
        Returns:
            dict: IPs of APs
        """
        access_point_info = {}
        varbind_ip = self._snmp_connection.bulk_walk(accessPointIpAddress)
        varbind_name = self._snmp_connection.bulk_walk(accessPointName)
        for ip, name in zip(varbind_ip, varbind_name):
            access_point_id = ip.index
            access_point_info[access_point_id] = {'access_point_ip': convert_if_bytes(ip.value),
                                                  'access_point_name': name.value}
        return access_point_info

    @threaded_cached_property
    def _access_point_num(self):
        """
        Returns:
            int: value of number of access points per controller
        """
        access_point_num = 0
        varbinds = self._snmp_connection.bulk_walk(controllerTotalNumOfAccessPoints)
        for varbind in varbinds:
            access_point_num = varbind.value
        return access_point_num

    @threaded_cached_property
    def _radio_index(self):
        """
        Returns:
            dict: Indices of the radio channel
        """
        radio_indices = {}
        varbinds = self._snmp_connection.bulk_walk(accessPointName)
        for varbind in varbinds:
            value = convert_if_bytes(varbind.value)
            varbind_radio1 = value + ".radio1"
            radio_indices[varbind_radio1] = varbind.index + ".1"
            varbind_radio2 = value + ".radio2"
            radio_indices[varbind_radio2] = varbind.index + ".2"
        return radio_indices

    def _build_oids_map(self):
        self._oids_map = {
            "controller_number_of_users": {
                 "method": "bulk_walk",
                 "oid": controllerTotalNumOfUsers},
            "number_of_access_point": {
                "method": "static",
                "values": self._access_point_num},
            "access_point_ip": {
                 "method": "static",
                 "values": {x: self._access_point_info[x]['access_point_ip'] for x in self._access_point_info}},
            "access_point_name": {
                "method": "static",
                "values": {x: self._access_point_info[x]['access_point_name'] for x in self._access_point_info}},
            "access_point_status": {
                "method": "bulk_walk",
                "oid": accessPointStatus},
            "radio_index": {
                "method": "static",
                "values": {v: k for k, v in self._radio_index.items()}},
            "radio_clients_number": {
                "method": "bulk_walk",
                "oid": radioNumAssociatedClients},
            "radio_utilization": {
                "method": "bulk_walk",
                "oid": radioUtilizationPercentage},
            "number_of_stations_per_channel": {
                "method": "bulk_walk",
                "oid": accessPointChannelNumStations},
            "channel_noise": {
                "method": "bulk_walk",
                "oid": accessPointChannelNoise},
            "channel_rx_utilization": {
                "method": "bulk_walk",
                "oid": accessPointChannelRxUtilization},
            "channel_tx_utilization": {
                "method": "bulk_walk",
                "oid": accessPointChannelTxUtilization},
            "channel_utilization": {
                "method": "bulk_walk",
                "oid": accessPointChannelUtilization}
        }

    def _build_metrics_groups_conf(self):
        self._metrics_groups = [
              {
                    "group_name": "user",
                    "dimensions": {},
                    "metrics": {
                        "number_of_users": {
                            "metric_type": "gauge",
                            "value": "controller_number_of_users.$index"}}
              },
              {
                    "group_name": "access_point",
                    "dimensions": {
                        "access_point_name": "access_point_name.$index",
                        "access_point_ip": "access_point_ip.$index",
                    },
                    "metrics": {
                        "access_point_status": {
                            "metric_type": "gauge",
                            "value": "access_point_status.$index"
                        }}
              },
              {
                    "group_name": "radio",
                    "dimensions": {
                        "radio_name": "radio_index.$index"
                    },
                    "metrics": {
                        "radio_utilization": {
                            "metric_type": "gauge",
                            "value": "radio_utilization.$index"
                        },
                        "number_of_radio_clients": {
                            "metric_type": "gauge",
                            "value": "radio_clients_number.$index"
                        },
                        "stations_per_channel": {
                            "metric_type": "gauge",
                            "value": "number_of_stations_per_channel.$index"
                        },
                        "channel_noise": {
                            "metric_type": "gauge",
                            "value": "channel_noise.$index"
                        },
                        "channel_rx_utilization": {
                            "metric_type": "gauge",
                            "value": "channel_rx_utilization.$index"
                        },
                        "channel_tx_utilization": {
                            "metric_type": "gauge",
                            "value": "channel_tx_utilization.$index"
                        },
                        "channel_utilization": {
                            "metric_type": "gauge",
                            "value": "channel_utilization.$index"
                        }
                    }

              }
        ]

    def get_enrichment(self):
        self._aruba_model = self._plugin_context.data.resource_metadata.get('model', 'unknown')

        self._build_oids_map()
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

        self._logger.debug('Metrics enrichment for WLC {}: {}'.format(self.device_fqdn, self.enrichment_group_set))

        return self.enrichment_group_set
