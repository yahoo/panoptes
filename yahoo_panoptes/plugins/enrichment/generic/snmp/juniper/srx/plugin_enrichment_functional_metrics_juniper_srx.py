from cached_property import threaded_cached_property

from yahoo_panoptes.enrichment.schema.generic.snmp import PanoptesGenericSNMPMetricsEnrichmentGroup
from yahoo_panoptes.framework.enrichment import PanoptesEnrichmentSet
from yahoo_panoptes.plugins.enrichment.generic.snmp.plugin_enrichment_generic_snmp \
    import PanoptesEnrichmentGenericSNMPPlugin

MIB_JNS_CHASSIS = u'.1.3.6.1.4.1.2636'

jnxOperatingTable = MIB_JNS_CHASSIS + u'.3.1.13'
jnxOperatingTemp = jnxOperatingTable + u'.1.7'
jnxOperatingCPU = jnxOperatingTable + u'.1.8'
jnxOperatingDescription = jnxOperatingTable + u'.1.5'
jnxOperatingDRAMSize = jnxOperatingTable + u'.1.10'
jnxOperatingBuffer = jnxOperatingTable + u'.1.11'

jnxJsSPUMonitoringObjectsTable = MIB_JNS_CHASSIS + u'.3.39.1.12.1.1'
jnxJsSPUMonitoringNodeDescr = jnxJsSPUMonitoringObjectsTable + u'.1.11'
jnxJsSPUMonitoringNodeIndex = jnxJsSPUMonitoringObjectsTable + u'.1.10'
jnxJsSPUMonitoringCurrentFlowSession = jnxJsSPUMonitoringObjectsTable + u'.1.6'
jnxJsSPUMonitoringMaxFlowSession = jnxJsSPUMonitoringObjectsTable + u'.1.7'

jnxJsNatObjects = MIB_JNS_CHASSIS + u'.3.39.1.7.1.1'
jnxJsNatSrcPoolName = jnxJsNatObjects + u'.4.1.1'
jnxJsNatSrcNumPortInuse = jnxJsNatObjects + u'.4.1.5'
jnxJsNatSrcNumSessions = jnxJsNatObjects + u'.4.1.6'


class JuniperSRXFunctionalMetricsEnrichment(PanoptesGenericSNMPMetricsEnrichmentGroup):
    """
    MetricsEnrichment class for Juniper devices
    """
    METRICS_SCHEMA_NAMESPACE = u"functional_metrics"


class JuniperPluginEnrichmentMetrics(PanoptesEnrichmentGenericSNMPPlugin):
    def __init__(self):
        super(JuniperPluginEnrichmentMetrics, self).__init__()

    @threaded_cached_property
    def _nat_source_pool_names(self):
        return self._snmp_connection.bulk_walk(oid=jnxJsNatSrcPoolName,
                                               non_repeaters=self._snmp_non_repeaters,
                                               max_repetitions=self.
                                               _snmp_max_repetitions)

    def _get_nat_source_pool_address(self, index):
        address = index.split(u'.')[-4:]
        return u".".join(address)

    @threaded_cached_property
    def _nat_stats_map(self):
        nat_stats_map = dict()
        for ent in self._nat_source_pool_names:  # pylint: disable=E1133
            nat_stats_map[ent.index] = dict()
            nat_stats_map[ent.index][u"source_pool_name"] = ent.value
            nat_stats_map[ent.index][u"address"] = self._get_nat_source_pool_address(ent.index)
        return nat_stats_map

    @threaded_cached_property
    def _session_enrichments(self):
        return self._snmp_connection.bulk_walk(oid=jnxJsSPUMonitoringMaxFlowSession,
                                               non_repeaters=self._snmp_non_repeaters,
                                               max_repetitions=self._snmp_max_repetitions)

    @threaded_cached_property
    def _monitoring_node_descriptions(self):
        return self._snmp_connection.bulk_walk(oid=jnxJsSPUMonitoringNodeDescr,
                                               non_repeaters=self._snmp_non_repeaters,
                                               max_repetitions=self._snmp_max_repetitions)

    @threaded_cached_property
    def _session_enrichments_map(self):
        session_stats_map = dict()
        for ent in self._session_enrichments:  # pylint: disable=E1133
            session_stats_map[ent.oid + u'.' + ent.index] = ent.value
        return session_stats_map

    @threaded_cached_property
    def nat_source_pools(self):
        nat_source_pool_indices = list()
        for ent in self._nat_source_pool_names:  # pylint: disable=E1133
            nat_source_pool_indices.append(ent.index)
        return nat_source_pool_indices

    @threaded_cached_property
    def _monitoring_node_descriptions_map(self):
        monitoring_node_descriptions_map = dict()
        for ent in self._monitoring_node_descriptions:  # pylint: disable=E1133
            value = ent.value

            if isinstance(value, bytes):
                value = value.decode(u'ascii', u'bytes')
            monitoring_node_descriptions_map[ent.index] = value
        return monitoring_node_descriptions_map

    def _build_oids_map(self):
        self._oids_map = {
            u"source_pool_name": {
                u"method": u"static",
                u"values": {x: self._nat_stats_map[x][u'source_pool_name'].decode(u'ascii', u'ignore') if
                            isinstance(self._nat_stats_map[x][u'source_pool_name'], bytes) else
                            self._nat_stats_map[x][u'source_pool_name'] for x in self._nat_stats_map}
            },
            u"nat_src_translated_address": {
                u"method": u"static",
                u"values": {x: self._nat_stats_map[x][u'address'] for x in self._nat_stats_map}
            },
            u"monitoring_node_descriptions": {
                u"method": u"static",
                u"values": self._monitoring_node_descriptions_map
            },
            u"current_session_flow": {
                u"method": u"bulk_walk",
                u"oid": jnxJsSPUMonitoringCurrentFlowSession
            },
            u"num_ports_in_use": {
                u"method": u"bulk_walk",
                u"oid": jnxJsNatSrcNumPortInuse
            },
            u"num_sessions": {
                u"method": u"bulk_walk",
                u"oid": jnxJsNatSrcNumSessions
            }
        }

    def _build_metrics_groups_conf(self):
        self._metrics_groups = [
            {
                u"group_name": u"nat",
                u"dimensions": {
                    u"source_pool_name": u"source_pool_name.$index",
                    u"address": u"nat_src_translated_address.$index"
                },
                u"metrics": {
                    u"num_sessions": {
                        u"metric_type": u"gauge",
                        u"value": u"num_sessions.$index"
                    },
                    u"ports_in_use": {
                        u"metric_type": u"gauge",
                        u"value": u"num_ports_in_use.$index"
                    },
                    u"max_ports": {
                        u"metric_type": u"gauge",
                        u"transform": u"lambda x: 64512",
                        u"value": u"num_ports_in_use.$index"
                    }
                }
            },
            {
                u"group_name": u"session",
                u"dimensions": {
                },
                u"metrics": {
                    u"current_session_flow": {
                        u"metric_type": u"gauge",
                        u"value": u"sum([int(x) for x in current_session_flow.values()])"
                    }
                }
            }
        ]

    @property
    def metrics_enrichment_class(self):
        return JuniperSRXFunctionalMetricsEnrichment

    def get_enrichment(self):
        self._build_oids_map()
        self._build_metrics_groups_conf()

        enrichment_set = {
            u"oids": self.oids_map,
            u"metrics_groups": self.metrics_groups
        }

        try:
            self.enrichment_group.add_enrichment_set(PanoptesEnrichmentSet(self.device_fqdn, enrichment_set))
        except Exception as e:
            self._logger.error(u'Error while adding enrichment set {} to enrichment group for the device {}: {}'.
                               format(enrichment_set, self.device_fqdn, repr(e)))

        self.enrichment_group_set.add_enrichment_group(self.enrichment_group)

        self._logger.debug(u'Metrics enrichment for device {}: {}'.format(self.device_fqdn, self.enrichment_group_set))

        return self.enrichment_group_set
