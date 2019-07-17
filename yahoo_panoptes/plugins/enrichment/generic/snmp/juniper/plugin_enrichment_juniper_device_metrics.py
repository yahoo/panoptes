import collections
import re

from cached_property import threaded_cached_property

from yahoo_panoptes.enrichment.schema.generic import snmp
from yahoo_panoptes.framework import enrichment
from yahoo_panoptes.plugins.enrichment.generic.snmp import plugin_enrichment_generic_snmp

from yahoo_panoptes.framework.utilities.snmp.mibs.juniper import MibJuniper

# n.b. For QFX1000X devices, will report % fan_trays_ok, which is <= to fans_ok
FAN_TYPES = [r'Fan Tray \d+ Fan \d+', r'Fan Tray \d+', r'FAN \d+', r'node\d SRX\d+ \w+ fan \d', r'node\d Fan \d',
             r'node\d \w+ Tray Fan \d+', r'(Top|Bottom)\s(Rear|Middle|Front)\sFan']
POWER_MODULE_TYPES = [r'PDM \d{1,2}$', 'PEM', r'PSM \d{1,2}$', r'Power Supply \d$', r'Power Supply: Power Supply \d+ @',
                      r'node\d PEM \d']
TYPE_MAP = dict(zip(POWER_MODULE_TYPES, ['PDM', 'PEM', 'PSM', 'PEM', 'PEM', 'PEM']))


class JuniperDeviceMetricsEnrichment(snmp.PanoptesGenericSNMPMetricsEnrichmentGroup):
    pass


class JuniperPluginEnrichmentDeviceMetrics(plugin_enrichment_generic_snmp.PanoptesEnrichmentGenericSNMPPlugin):
    def __init__(self):
        self._juniper_model = None
        super(JuniperPluginEnrichmentDeviceMetrics, self).__init__()

    def _get_cpu_interval(self):
        """
        Checks the self._execute_frequency to figure out the oid to use for cpu_utilization
        Returns:
            string: the oid to use.
        """
        self._polling_execute_frequency = self._plugin_conf['main']['polling_frequency']
        if 5 <= self._polling_execute_frequency < 300:
            # TODO Need to divide by number of cores?
            # https://kb.juniper.net/InfoCenter/index?page=content&id=KB31764&cat=MX960_1&actp=LIST
            return str(MibJuniper.jnxOperating1MinLoadAvg)
        elif 300 <= self._polling_execute_frequency < 900:
            return str(MibJuniper.jnxOperating5MinLoadAvg)
        elif 900 <= self._polling_execute_frequency:
            return str(MibJuniper.jnxOperating15MinLoadAvg)
        else:
            return str(MibJuniper.jnxOperating1MinLoadAvg)

    @threaded_cached_property
    def _entity_names(self):
        entities = {}
        varbinds = self._snmp_connection.bulk_walk(MibJuniper.jnxOperatingDescr.oid)
        for varbind in varbinds:
            entities[varbind.index] = varbind.value
        return entities

    @threaded_cached_property
    def _temp_sensors(self):
        """
        Returns:
             dict: temperature stats for the system
        """
        temps = {}
        varbinds = self._snmp_connection.bulk_walk(str(MibJuniper.jnxOperatingTemp))
        for varbind in varbinds:
            temp_id = varbind.index
            temps[temp_id] = {'sensor_name': self._entity_names[temp_id]}
        return temps

    @threaded_cached_property
    def _cpus(self):
        """
        cpu will always be a Gauge32
        Returns:
            dict: cpus in the system
        """
        cpus = {}
        varbinds = self._snmp_connection.bulk_walk(self._get_cpu_interval())
        for varbind in varbinds:
            # grab the last element of the index to use as the cpu_id
            cpu_id = varbind.index  # TODO trim off prepending OID?
            cpus[cpu_id] = {'cpu_name': self._entity_names[cpu_id],
                            'cpu_no': 'Module ' + str(cpu_id)}
        return cpus

    @threaded_cached_property
    def _memory(self):
        """
        Returns:
             dict: memory stats for the system
        """
        memory = {}
        varbinds = self._snmp_connection.bulk_walk(str(MibJuniper.jnxOperatingMemory))
        for varbind in varbinds:
            memory_id = varbind.index
            memory[memory_id] = {'memory_total': int(varbind.value) * (2 ** 20)}  # reported in megabytes
        return memory

    @threaded_cached_property
    def _fans(self):
        """
        Reports fan status for Junipers.
        Returns:
            dict: fan metrics
        """
        fans = {}
        for index, name in self._entity_names.items():
            for type in FAN_TYPES:
                if re.match(type, name):
                    fans[index] = {'name': name}

        return fans

    @threaded_cached_property
    def _power_modules(self):
        """
        Reports power entry module stats for Juniper devices.
        Returns:
            dict: power_supplies
        """
        power_modules = {}
        for index, name in self._entity_names.items():
            for type in POWER_MODULE_TYPES:
                if re.match(type, name):
                    power_modules[index] = {'name': name}
                    power_modules[index]['type'] = TYPE_MAP[type]

        return power_modules

    def _add_power_module_types_mapping(self):
        types_mapping = {x: x for x in self._oids_map["power_module_types"]["values"].values()}
        self._oids_map["power_module_types"]["values"].update(types_mapping)

    def _build_oids_map(self):
        self._oids_map = {
            "cpu_name": {
                "method": "static",
                "values": {x: self._cpus[x]['cpu_name'] for x in self._cpus}
            },
            "cpu_no": {
                "method": "static",
                "values": {x: self._cpus[x]['cpu_no'] for x in self._cpus}
            },
            "cpu_util": {
                "method": "bulk_walk",
                "oid": MibJuniper.jnxOperatingCPU.oid if re.match(r'SRX.*', self._juniper_model) else
                self._get_cpu_interval()
            },
            "memory_used": {
                "method": "bulk_walk",
                "oid": str(MibJuniper.jnxOperatingBuffer)
            },
            "memory_total": {
                "method": "static",
                "values": {x: self._memory[x]['memory_total'] for x in self._memory if
                           self._memory[x]['memory_total'] != 0}
            },
            "oper_status": {
                "method": "bulk_walk",
                "oid": str(MibJuniper.jnxOperatingState)
            },
            "fans": {
                "method": "static",
                "values": {x: self._fans[x]['name'] for x in self._fans}
            },
            "power_modules": {
                "method": "static",
                "values": {x: self._power_modules[x]['name'] for x in self._power_modules}
            },
            "power_module_types": {
                "method": "static",
                "values": {x: self._power_modules[x]['type'] for x in self._power_modules}
            },
            "power_units_total": {
                "method": "static",
                "values": dict(collections.Counter([self._power_modules[x]['type'] for x in self._power_modules]))
            },
            "temp_sensor_values": {
                "method": "bulk_walk",
                "oid": str(MibJuniper.jnxOperatingTemp)
            },
            "temp_sensor_name": {
                "method": "static",
                "values": {x: self._temp_sensors[x]['sensor_name'] for x in self._temp_sensors}
            }
        }

        self._add_power_module_types_mapping()

    def _build_metrics_groups_conf(self):
        self._metrics_groups = [
            {
                "group_name": "environment",
                "dimensions": {
                    "sensor": "temp_sensor_name.$index"
                },
                "metrics": {
                    "temperature_fahrenheit": {
                        "metric_type": "gauge",
                        "type": "float",
                        "transform": "lambda x: round((x * 1.8) + 32, 2) if x != 0 else 0.0",
                        "value": "temp_sensor_values.$index"
                    }
                }
            },
            {
                "group_name": "cpu",
                "dimensions": {
                    "cpu_name": "cpu_name.$index",
                    "cpu_no": "cpu_no.$index",
                    "cpu_type": "'data' if 'Routing Engine' in cpu_name.$index else 'ctrl'"
                },
                "metrics": {
                    "cpu_utilization": {
                        "metric_type": "gauge",
                        "value": "cpu_util.$index"
                    }
                }
            },
            {
                "group_name": "memory",
                "dimensions": {
                    "memory_type": "cpu_name.$index"
                },
                "metrics": {
                    "memory_used": {
                        "metric_type": "gauge",
                        "indices_from": "memory_total",
                        "value": "float(memory_used.$index) / 100.0 * memory_total.$index"
                    },
                    "memory_total": {
                        "metric_type": "gauge",
                        "value": "memory_total.$index"
                    }
                }
            }
        ]

        if len(self._power_modules) > 0:
            self._metrics_groups.append(
                {
                    "group_name": "environment",
                    "dimensions": {
                        "power_module_type": "power_module_types.$index"
                    },
                    "metrics": {
                        "power_units_on": {
                            "metric_type": "gauge",
                            "indices_from": "power_units_total",
                            "value": "len([(x,y) for (x,y) in oper_status.items() if x in "
                                     "power_module_types and y not in "
                                     "['6'] and power_module_types[x] == $index])"
                        },
                        "power_units_total": {
                            "metric_type": "gauge",
                            "value": "power_units_total.$index"
                        }
                    }
                }
            )

        if len(self._fans) > 0:
            self._metrics_groups.append(
                {
                    "group_name": "environment",
                    "dimensions": {},
                    "metrics": {
                        "fans_ok": {
                            "metric_type": "gauge",
                            "value": "len([(x,y) for (x,y) in oper_status.items() if x in fans and y not in ['6']])"
                        },
                        "fans_total": len(self._fans)
                    }
                }
            )

    @property
    def metrics_enrichment_class(self):
        return JuniperDeviceMetricsEnrichment

    def get_results(self):
        self._juniper_model = self._plugin_context.data.resource_metadata.get('model', 'unknown')

        self._build_oids_map()
        self._build_metrics_groups_conf()

        enrichment_set = {
            "oids": self.oids_map,
            "metrics_groups": self.metrics_groups
        }

        try:
            self.enrichment_group.add_enrichment_set(enrichment.PanoptesEnrichmentSet(self.device_fqdn, enrichment_set))
        except Exception as e:
            self._logger.error('Error while adding enrichment set {} to enrichment group for the device {}: {}'.
                               format(enrichment_set, self.device_fqdn, repr(e)))

        self.enrichment_group_set.add_enrichment_group(self.enrichment_group)

        self._logger.debug('Metrics enrichment for device {}: {}'.format(self.device_fqdn, self.enrichment_group_set))

        return self.enrichment_group_set
