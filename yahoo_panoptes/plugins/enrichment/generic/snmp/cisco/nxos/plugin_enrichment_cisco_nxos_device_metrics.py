"""
This module implements a Panoptes Plugin that can poll Cisco NX-OS devices for Device Metrics
"""
import math

from cached_property import threaded_cached_property

from yahoo_panoptes.enrichment.schema.generic import snmp
from yahoo_panoptes.framework import enrichment
from yahoo_panoptes.plugins.enrichment.generic.snmp import plugin_enrichment_generic_snmp

ENTITY_MIB_PREFIX = '.1.3.6.1.2.1.47'
CISCO_ENTITY_SENSOR_MIB_PREFIX = '.1.3.6.1.4.1.9.9.91.1'
CISCO_PROCESS_MIB_PREFIX = '.1.3.6.1.4.1.9.9.109'
CISCO_ENTITY_FRU_CONTROL_PREFIX = '.1.3.6.1.4.1.9.9.117'

cpmCPUTotal1minRev = CISCO_PROCESS_MIB_PREFIX + '.1.1.1.1.7'
cpmCPUTotal5minRev = CISCO_PROCESS_MIB_PREFIX + '.1.1.1.1.8'
cpmCPUTotalMonIntervalValue = CISCO_PROCESS_MIB_PREFIX + '.1.1.1.1.10'
cpmCPUTotalPhysicalIndex = CISCO_PROCESS_MIB_PREFIX + '.1.1.1.1.2'

entSensorScales = CISCO_ENTITY_SENSOR_MIB_PREFIX + '.1.1.1.2'
entSensorValues = CISCO_ENTITY_SENSOR_MIB_PREFIX + '.1.1.1.4'
entSensorType = CISCO_ENTITY_SENSOR_MIB_PREFIX + '.1.1.1.1'

cefcFanTrayOperStatus = CISCO_ENTITY_FRU_CONTROL_PREFIX + '.1.4.1.1.1'
cefcFRUPowerOperStatus = CISCO_ENTITY_FRU_CONTROL_PREFIX + '.1.1.2.1.2'

entPhysicalClass = ENTITY_MIB_PREFIX + '.1.1.1.1.5'
entPhysicalParentRelPos = ENTITY_MIB_PREFIX + '.1.1.1.1.6'
entPhysicalName = ENTITY_MIB_PREFIX + '.1.1.1.1.7'

cpmCPUMemoryUsed = CISCO_PROCESS_MIB_PREFIX + '.1.1.1.1.12'
cpmCPUMemoryFree = CISCO_PROCESS_MIB_PREFIX + '.1.1.1.1.13'


class CiscoNXOSDeviceMetricsEnrichment(snmp.PanoptesGenericSNMPMetricsEnrichmentGroup):
    pass


class CiscoNXOSPluginEnrichmentMetrics(plugin_enrichment_generic_snmp.PanoptesEnrichmentGenericSNMPPlugin):
    def __init__(self):
        self._plugin_context = None
        self._logger = None
        self._cisco = None
        self._cisco_host = None
        self._cisco_model = None
        self._execute_frequency = None
        self._snmp_connection = None
        self._polling_status = None
        self._max_repetitions = None
        self._polling_execute_frequency = None

        super(CiscoNXOSPluginEnrichmentMetrics, self).__init__()

    def _get_cpu_interval(self):
        """
        Checks the self._execute_frequency to figure out the oid to use for cpu_utilization
        Returns:
            string: the oid to use.
        """
        self._polling_execute_frequency = self._plugin_conf['main']['polling_frequency']

        if 5 <= self._polling_execute_frequency < 60:
            return cpmCPUTotalMonIntervalValue  # replaces cpmCPUTotal5SecRev
        elif 60 <= self._polling_execute_frequency < 300:
            return cpmCPUTotal1minRev
        elif 300 <= self._polling_execute_frequency:
            return cpmCPUTotal5minRev
        else:
            return cpmCPUTotal1minRev

    @staticmethod
    def replace_celcius(string):
        """
         Replaces 'celsius' with 'fahrenheit' in the provided string.

        Returns:
             string: The modified string
        """
        return string.replace('celsius', 'fahrenheit')

    @staticmethod
    def _entity_sensor_scale_to_exponent(sensor_scale):
        """
        scale is an integer index that refers to an exponent applied to entSensorValue.  The sensor_exponent comes from
        the cisco definitions, and I want to move those out of the plugin at some point.
        Args:
            sensor_scale(int): entSensorScale value
        Returns:
            int: signed integer exponent to be applied to entSensorValue to normalize
        """
        sensor_exponent = ['-24', '-21', '-18', '-15', '-12', '-9', '-6', '-3', '0', '3', '6', '9', '12', '15', '18',
                           '21', '24']
        return int(sensor_exponent[sensor_scale - 1])

    @threaded_cached_property
    def _n3k_models(self):
        return self._plugin_conf['main']['n3k_models']

    @threaded_cached_property
    def _temp_sensors(self):
        """
        Returns:
            dict mapping sensor_id to the sensor_name and the scale to apply to the entSensorValue
        """
        temp_sensors = {}
        sensor_scales = {}

        # Pull the entSensorScales - these tell you the scale of the metric
        scales = self._snmp_connection.bulk_walk(entSensorScales)
        for scale in scales:
            sensor_scales[int(scale.index)] = int(scale.value)

        varbinds = self._snmp_connection.bulk_walk(entSensorType)
        for varbind in varbinds:
            """example varbind;
            {'_index': u'21590',
            '_queried_oid': '.1.3.6.1.4.1.9.9.91.1.1.1.1.1',
            '_snmp_type': u'INTEGER',
            '_value': u'8'}
            """

            # temperature sensor - I'll break this out if it gets more complex.
            if varbind.value == '8':
                sensor_id = int(varbind.index)
                temp_sensors[sensor_id] = {'sensor_scale':
                                           self._entity_sensor_scale_to_exponent(sensor_scales[sensor_id]),
                                           'sensor_name': self.replace_celcius(
                                               self._entity_physical_names.get(sensor_id, ""))}

        if not len(temp_sensors):
            self._logger.warn(
                'Failed to get temperature enrichments on device "%s" with model "%s"' %
                (self._device_fqdn, self._cisco_model))

        return temp_sensors

    @threaded_cached_property
    def _num_fans(self):
        """
        Returns:
            int: The current number of system (non-psu) fans
        """
        varbinds = self._snmp_connection.bulk_walk(cefcFanTrayOperStatus)

        if not len(varbinds):
            self._logger.warn(
                'Failed to get fan enrichments on device "%s" with model "%s"' %
                (self._device_fqdn, self._cisco_model))

        return len(varbinds)

    @threaded_cached_property
    def _power_supplies(self):
        """
        Pulls the entPhysicalClass items tree. We're interested in '6' (Power Supply Units).  note that this could be
        expanded out if there's a need.
        Returns:
            dict: power_supplies
        """
        power_supplies = {}
        varbinds = self._snmp_connection.bulk_walk(entPhysicalClass)
        for varbind in varbinds:
            if varbind.value == '6':
                psu_id = int(varbind.index)
                power_supplies[psu_id] = {'psu_id': psu_id, 'psu_name': self._entity_physical_names[psu_id]}

        """
        Note that this will return a power state for the device as a whole, which we ignore because we care about
        supplies
        """
        varbinds = self._snmp_connection.bulk_walk(cefcFRUPowerOperStatus)
        for varbind in varbinds:
            psu_id = int(varbind.index)
            if psu_id in power_supplies:
                power_supplies[psu_id]['psu_state'] = int(varbind.value)

        if not len(power_supplies):
            self._logger.warn(
                'Failed to get power enrichments on device "%s" with model "%s"' %
                (self._device_fqdn, self._cisco_model))

        return power_supplies

    @threaded_cached_property
    def _module_numbers(self):
        """
        Maps indices used in the Cisco Entity Mib to the module number
        Returns:
            dict: physical parent relative position of the proved index
        """
        module_numbers = {}
        varbinds = self._snmp_connection.bulk_walk(entPhysicalParentRelPos)
        for varbind in varbinds:
            module_numbers[int(varbind.index)] = varbind.value
        return module_numbers

    @threaded_cached_property
    def _entity_physical_names(self):
        """
        Returns:
            dict: Names of physical entities in the device
        """
        physical_names = {}
        varbinds = self._snmp_connection.bulk_walk(entPhysicalName)
        for varbind in varbinds:
            physical_names[int(varbind.index)] = varbind.value
        return physical_names

    @threaded_cached_property
    def _memory(self):
        """
        Pulls the free memory and memory in use for the device and adds them together to produce total memory.
        All results are GAUGE32/int
        cpmCPUMemoryUsed has more ubiquity than cemp_mem_pool_hcused
        Returns:
            dict: memory reported by the Cisco device under CISCO-PROCESS-MIB
        """
        memory = {}
        memory_used = cpmCPUMemoryUsed
        varbinds = self._snmp_connection.bulk_walk(memory_used)
        for varbind in varbinds:
            # grab the last element of the index to use as the memory_id
            if self._cisco_model in self._n3k_models:
                memory_id = self._process_mib_indices_table[int(varbind.index.split('.')[-1])]
            else:
                memory_id = int(varbind.index.split('.')[-1])
            memory[memory_id] = {'memory_used': int(varbind.value)}

        memory_free = cpmCPUMemoryFree
        varbinds = self._snmp_connection.bulk_walk(memory_free)
        for varbind in varbinds:
            # grab the last element of the index to use as the memory_id
            if self._cisco_model in self._n3k_models:
                memory_id = self._process_mib_indices_table[int(varbind.index.split('.')[-1])]
            else:
                memory_id = int(varbind.index.split('.')[-1])
            memory[memory_id]['memory_free'] = int(varbind.value)
            memory[memory_id]['memory_total'] = memory[memory_id]['memory_used'] + int(varbind.value)

        for memory_id in memory.keys():
            if memory_id in self._module_numbers:
                if int(self._module_numbers[memory_id]) in self._entity_physical_names:
                    memory[memory_id]['memory_type'] = "Module {} ({})".format(self._module_numbers[memory_id],
                                                                               self._entity_physical_names[
                                                                                   int(self._module_numbers[
                                                                                           memory_id])])
                else:
                    memory[memory_id]['memory_type'] = "Module {}".format(self._module_numbers[memory_id])

        if not len(memory):
            self._logger.warn(
                'Failed to get memory enrichments on device "%s" with model "%s"' %
                (self._device_fqdn, self._cisco_model))

        return memory

    @threaded_cached_property
    def _process_mib_indices_table(self):
        """
        Returns:
            A mapping of module numbers to the Cisco Entity Mib that refers to it. In the case of collisions, the lowest
            index is retained.
        """
        inverse_dict = {}
        for k, v in self._module_numbers.items():
            if int(v) not in inverse_dict:
                inverse_dict[int(v)] = []
            inverse_dict[int(v)].append(k)
        return {k: min(v) for k, v in inverse_dict.items()}

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
            if self._cisco_model in self._n3k_models:
                cpu_id = self._process_mib_indices_table[int(varbind.index.split('.')[-1])]
            else:
                cpu_id = int(varbind.index.split('.')[-1])
            if cpu_id in self._entity_physical_names.keys() and cpu_id in self._module_numbers.keys():
                cpus[cpu_id] = {'cpu_name': self._entity_physical_names[cpu_id],
                                'cpu_no': "Module " + self._module_numbers[cpu_id]}

        if not len(cpus):
            self._logger.warn(
                'Failed to get cpu enrichments on device "%s" with model "%s"' %
                (self._device_fqdn, self._cisco_model))

        return cpus

    def _build_oids_map(self):
        """See base class."""
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
                "oid": self._get_cpu_interval(),
                "index_transform": {str(k): str(v) for k, v in self._process_mib_indices_table.items() if
                                    self._cisco_model in self._n3k_models}
            },
            "memory_type": {
                "method": "static",
                "values": {x: self._memory[x]['memory_type'] for x in self._memory}
            },
            "memory_used": {
                "method": "bulk_walk",
                "oid": cpmCPUMemoryUsed,
                "index_transform": {str(k): str(v) for k, v in self._process_mib_indices_table.items() if
                                    self._cisco_model in self._n3k_models}
            },
            "memory_total": {
                "method": "static",
                "values": {x: self._memory[x]['memory_total'] for x in self._memory}
            },
            "cefc_fru_fan": {
                "method": "bulk_walk",
                "oid": cefcFanTrayOperStatus
            },
            "entity_fru_control": {
                "method": "bulk_walk",
                "oid": cefcFRUPowerOperStatus
            },
            "power_supplies": {
                "method": "static",
                "values": {x: self._power_supplies[x]['psu_name'] for x in self._power_supplies}
            },
            "ent_sensor_values": {
                "method": "bulk_walk",
                "oid": entSensorValues
            },
            "temp_sensor_scales": {
                "method": "static",
                "values": {x: math.pow(10, self._temp_sensors[x]['sensor_scale']) for x in self._temp_sensors}
            },
            "temp_sensor_name": {
                "method": "static",
                "values": {x: self._temp_sensors[x]['sensor_name'] for x in self._temp_sensors}
            }
        }

    def _build_metrics_groups_conf(self):
        """See base class."""
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
                        "indices_from": "temp_sensor_scales",
                        "transform": "lambda x: round((x * 1.8) + 32, 2)",
                        "value": "int(ent_sensor_values.$index) * temp_sensor_scales.$index"
                    }
                }
            },
            {
                "group_name": "environment",
                "dimensions": {},
                "metrics": {
                    "fans_ok": {
                        "metric_type": "gauge",
                        "value": "len([x for x in cefc_fru_fan.values() if x == '2'])"
                    },
                    "fans_total": self._num_fans
                }
            },
            {
                "group_name": "environment",
                "dimensions": {},
                "metrics": {
                    "power_units_on": {
                        "metric_type": "gauge",
                        # http://www.circitor.fr/Mibs/Html/C/CISCO-ENTITY-FRU-CONTROL-MIB.php#PowerOperType
                        "value":
                            "len([(x,y) for (x,y) in entity_fru_control.items() if x in power_supplies and "
                            "y in ['2', '9', '12']])"
                    },
                    "power_units_total": len(self._power_supplies)
                }
            },
            {
                "group_name": "cpu",
                "dimensions": {
                    "cpu_name": "cpu_name.$index",
                    "cpu_no": "cpu_no.$index",
                    "cpu_type": "'ctrl'"
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
                    "memory_type": "memory_type.$index"
                },
                "metrics": {
                    "memory_used": {
                        "metric_type": "gauge",
                        "value": "memory_used.$index"
                    },
                    "memory_total": {
                        "metric_type": "gauge",
                        "value": "memory_total.$index"
                    }
                }
            }
        ]

    @property
    def metrics_enrichment_class(self):
        return CiscoNXOSDeviceMetricsEnrichment

    def get_results(self):
        """See base class."""
        self._cisco_model = self._plugin_context.data.resource_metadata.get('model', 'unknown')
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
