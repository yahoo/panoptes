"""
This module implements a Panoptes Plugin that can poll Ciena Waveserver devices for Device Metrics
"""

from cached_property import threaded_cached_property

from yahoo_panoptes.enrichment.schema.generic.snmp import PanoptesGenericSNMPMetricsEnrichmentGroup
from yahoo_panoptes.framework.enrichment import PanoptesEnrichmentSet
from yahoo_panoptes.plugins.enrichment.generic.snmp.plugin_enrichment_generic_snmp \
    import PanoptesEnrichmentGenericSNMPPlugin


MIB_CIENA_CHASSIS = u'.1.3.6.1.4.1.1271'
cwsChassisFanStateOperationalState = MIB_CIENA_CHASSIS + u'.3.4.6.25.1.3'
cwsChassisPsuStateOperationalState = MIB_CIENA_CHASSIS + u'.3.4.6.21.1.3'


class CienaWSDeviceMetricsEnrichment(PanoptesGenericSNMPMetricsEnrichmentGroup):
    pass


class CienaPluginWSDeviceMetricsEnrichment(PanoptesEnrichmentGenericSNMPPlugin):
    def __init__(self):
        self._plugin_context = None
        self._logger = None
        self._ciena_model = None
        self._snmp_connection = None
        self._max_repetitions = None
        self._polling_execute_frequency = None
        super(CienaPluginWSDeviceMetricsEnrichment, self).__init__()

    @property
    def metrics_enrichment_class(self):
        return CienaWSDeviceMetricsEnrichment

    @threaded_cached_property
    def _num_fan_units(self):

        varbinds = self._snmp_connection.bulk_walk(cwsChassisFanStateOperationalState, max_repetitions=2)
        fans_total = len(varbinds)

        return fans_total

    @threaded_cached_property
    def _num_power_supplies(self):

        varbinds = self._snmp_connection.bulk_walk(cwsChassisPsuStateOperationalState, max_repetitions=2)
        power_supplies = len(varbinds)

        return power_supplies

    def _build_metrics_oids_map(self):
        self._oids_map = {
            u"fan_status": {
                u"method": u"bulk_walk",
                u"oid": cwsChassisFanStateOperationalState
            },
            u"power_status": {
                u"method": u"bulk_walk",
                u"oid": cwsChassisPsuStateOperationalState
            }
        }

    def _build_metrics_groups_conf(self):
        self._metrics_groups = []
        if self._num_fan_units > 0:

            self._metrics_groups.append({
                u"group_name": u"environment",
                u"dimensions": {},
                u"metrics": {
                    u"fans_ok": {
                        u"metric_type": u"gauge",
                        u"value": u"len([x for x in fan_status.values() if x == '1'])"
                    },
                    u"fans_total": self._num_fan_units
                }
            }),
        if self._num_power_supplies > 0:
            self._metrics_groups.append({
                u"group_name": u"environment",
                u"dimensions": {},
                u"metrics": {
                    u"power_units_on": {
                        u"metric_type": u"gauge",
                        u"value": u"len([x for x in power_status.values() if x == '1'])"
                    },
                    u"power_units_total": self._num_power_supplies
                }
            })

    def get_enrichment(self):
        self._ciena_model = self._plugin_context.data.resource_metadata.get(u'model', u'unknown')
        self._build_metrics_oids_map()
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
