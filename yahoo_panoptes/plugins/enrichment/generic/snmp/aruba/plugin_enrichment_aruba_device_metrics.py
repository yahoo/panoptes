"""
This module implements a Panoptes Plugin that can poll Aruba devices for Device Metrics
"""

from cached_property import threaded_cached_property
from yahoo_panoptes.framework.enrichment import PanoptesEnrichmentSet

from yahoo_panoptes.plugins.enrichment.generic.snmp.plugin_enrichment_generic_snmp import \
    PanoptesEnrichmentGenericSNMPPlugin

ENTITY_MIB_PREFIX = u'.1.3.6.1.4.1.14823'
sysExtProcessorLoad = ENTITY_MIB_PREFIX + u'.2.2.1.2.1.13.1.3'
sysExtProcessorDescr = ENTITY_MIB_PREFIX + u'.2.2.1.2.1.13.1.2'
arubaMemoryPoolTotal = ENTITY_MIB_PREFIX + u'.2.2.1.1.1.11.1.2.1'
arubaMemoryPoolUsed = ENTITY_MIB_PREFIX + u'.2.2.1.1.1.11.1.3.1'
arubaMemoryPoolAvailable = ENTITY_MIB_PREFIX + u'.2.2.1.1.1.11.1.4.1'
sysExtIntTemperature = ENTITY_MIB_PREFIX + u'.2.2.1.2.1.10.0'
arubaFanStatus = ENTITY_MIB_PREFIX + u'.2.2.1.2.1.17.1.2'
arubaPowerSupplyStatus = ENTITY_MIB_PREFIX + u'.2.2.1.2.1.18.1.2'


class ArubaPluginEnrichmentDeviceMetrics(PanoptesEnrichmentGenericSNMPPlugin):
    def __init__(self):
        self._plugin_context = None
        self._logger = None
        self._aruba_model = None
        self._snmp_connection = None
        self._max_repetitions = None
        self._polling_execute_frequency = None

        super(ArubaPluginEnrichmentDeviceMetrics, self).__init__()

    @threaded_cached_property
    def _processor_names(self):
        """
        Returns:
            dict: names of the processors
        """
        entities = {}
        varbinds = self._snmp_connection.bulk_walk(sysExtProcessorDescr)
        for varbind in varbinds:
            entities[varbind.index] = varbind.value
        return entities

    @threaded_cached_property
    def _cpus(self):
        """
        cpu will always be a Gauge32
        Returns:
            dict: cpus in the system
        """
        cpus = {}
        varbinds = self._snmp_connection.bulk_walk(sysExtProcessorLoad)

        for varbind in varbinds:
            # grab the last element of the index to use as the cpu_id
            cpu_id = varbind.index.split(u'.')[-1]
            cpus[cpu_id] = {u'cpu_name': self._processor_names[cpu_id],
                            u'cpu_no': u'Module ' + str(cpu_id)}
        return cpus

    @threaded_cached_property
    def _memory(self):
        """
        Pulls the total memory for the device.
        All results are GAUGE32/int
        Returns:
            dict: total memory
        """
        memory = {}

        varbinds = self._snmp_connection.get(arubaMemoryPoolTotal)
        memory[u'memory_total'] = int(varbinds.value)

        return memory

    @threaded_cached_property
    def _num_fans(self):
        """
        Returns:
            int: The current number of system (non-psu) fans
        """
        varbinds = self._snmp_connection.bulk_walk(arubaFanStatus, max_repetitions=2)
        fans_total = len(varbinds)
        return fans_total

    @threaded_cached_property
    def _num_power_supplies(self):
        """
        Returns:
            the number of power supplies in device
        """
        varbinds = self._snmp_connection.bulk_walk(arubaPowerSupplyStatus, max_repetitions=2)
        power_supplies = len(varbinds)
        return power_supplies

    def _build_oids_map(self):
        self._oids_map = {
            u"cpu_name": {
                u"method": u"static",
                u"values": {x: self._cpus[x][u'cpu_name'].decode(u'ascii',
                                                                 u'ignore') if isinstance(self._cpus[x][u'cpu_name'],
                                                                                          bytes) else self._cpus[x][
                    u'cpu_name'] for x in self._cpus}
            },
            u"cpu_no": {
                u"method": u"static",
                u"values": {x: self._cpus[x][u'cpu_no'] for x in self._cpus}
            },
            u"cpu_util": {
                u"method": u"bulk_walk",
                u"oid": sysExtProcessorLoad
            },
            u"memory_used": {
                u"method": u"get",
                u"oid": arubaMemoryPoolUsed
            },
            u"fan_statuses": {
                u"method": u"bulk_walk",
                u"oid": arubaFanStatus
            },
            u"temperature": {
                u"method": u"get",
                u"oid": sysExtIntTemperature
            },
            u"power_status": {
                u"method": u"bulk_walk",
                u"oid": arubaPowerSupplyStatus
            }
        }

    def _build_metrics_groups_conf(self):
        self._metrics_groups = [
            {
                u"group_name": u"cpu",
                u"dimensions": {
                    u"cpu_name": u"cpu_name.$index",
                    u"cpu_no": u"cpu_no.$index",
                    u"cpu_type": u"'ctrl'"
                },
                u"metrics": {
                    u"cpu_utilization": {
                        u"metric_type": u"gauge",
                        u"value": u"cpu_util.$index"
                    }
                }
            },
            {
                u"group_name": u"memory",
                u"dimensions": {
                    u"memory_type": u"'dram'"
                },
                u"metrics": {
                    u"memory_used": {
                        u"metric_type": u"gauge",
                        u"value": u"memory_used"
                    },
                    u"memory_total": {
                        u"metric_type": u"gauge",
                        u"value": self._memory[u'memory_total']
                    }
                }
            },
            {
                u"group_name": u"environment",
                u"dimensions": {
                    u"sensor": u"'internal'"
                },
                u"metrics": {
                    u"temperature_fahrenheit": {
                        u"metric_type": u"gauge",
                        u"type": u"string",
                        u"transform": u"lambda x: round((float(x.split(' ')[0]) * 1.8) + 32,2)",
                        u"value": u"temperature"
                    }
                }
            }
        ]
        if self._num_fans > 0:
            self._metrics_groups.append(
                {
                    u"group_name": u"environment",
                    u"dimensions": {},
                    u"metrics": {
                        u"fans_ok": {
                            u"metric_type": u"gauge",
                            u"value": u"len([x for x in fan_statuses.values() if x == '1'])"
                        },
                        u"fans_total": self._num_fans
                    }
                })
        if self._num_power_supplies > 0:
            self._metrics_groups.append(
                {
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
        self._aruba_model = self._plugin_context.data.resource_metadata.get(u'model', u'unknown')

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
