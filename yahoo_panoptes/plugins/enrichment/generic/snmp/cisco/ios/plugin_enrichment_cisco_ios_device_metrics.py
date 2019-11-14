"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements a Panoptes Plugin that can poll Cisco NX-OS devices for Device Metrics
"""
import math
import re

from cached_property import threaded_cached_property

from yahoo_panoptes.enrichment.schema.generic import snmp
from yahoo_panoptes.framework import enrichment
from yahoo_panoptes.plugins.enrichment.generic.snmp import plugin_enrichment_generic_snmp

ENTITY_MIB_PREFIX = u'.1.3.6.1.2.1.47'
CISCO_ENV_MON_MIB_PREFIX = u'.1.3.6.1.4.1.9.9.13'
CISCO_MEMORY_POOL_MIB_PREFIX = u'.1.3.6.1.4.1.9.9.48'
CISCO_ENTITY_SENSOR_MIB_PREFIX = u'.1.3.6.1.4.1.9.9.91.1'
CISCO_PROCESS_MIB_PREFIX = u'.1.3.6.1.4.1.9.9.109'
CISCO_ENTITY_FRU_CONTROL_PREFIX = u'.1.3.6.1.4.1.9.9.117'

entPhysicalClass = ENTITY_MIB_PREFIX + u'.1.1.1.1.5'
entPhysicalParentRelPos = ENTITY_MIB_PREFIX + u'.1.1.1.1.6'
entPhysicalName = ENTITY_MIB_PREFIX + u'.1.1.1.1.7'
entPhysicalDescr = ENTITY_MIB_PREFIX + u'.1.1.1.1.2'

ciscoMemoryPoolName = CISCO_MEMORY_POOL_MIB_PREFIX + u'.1.1.1.2'
ciscoMemoryPoolUsed = CISCO_MEMORY_POOL_MIB_PREFIX + u'.1.1.1.5'
ciscoMemoryPoolFree = CISCO_MEMORY_POOL_MIB_PREFIX + u'.1.1.1.6'

cpmCPUTotal1minRev = CISCO_PROCESS_MIB_PREFIX + u'.1.1.1.1.7'
cpmCPUTotal5minRev = CISCO_PROCESS_MIB_PREFIX + u'.1.1.1.1.8'
cpmCPUTotalMonIntervalValue = CISCO_PROCESS_MIB_PREFIX + u'.1.1.1.1.10'
cpmCPUTotalPhysicalIndex = CISCO_PROCESS_MIB_PREFIX + u'.1.1.1.1.2'

entSensorType = CISCO_ENTITY_SENSOR_MIB_PREFIX + u'.1.1.1.1'
entSensorScales = CISCO_ENTITY_SENSOR_MIB_PREFIX + u'.1.1.1.2'
entSensorValues = CISCO_ENTITY_SENSOR_MIB_PREFIX + u'.1.1.1.4'

cefcFRUPowerOperStatus = CISCO_ENTITY_FRU_CONTROL_PREFIX + u'.1.1.2.1.2'
cefcFanTrayOperStatus = CISCO_ENTITY_FRU_CONTROL_PREFIX + u'.1.4.1.1.1'


ciscoEnvMonFanState = CISCO_ENV_MON_MIB_PREFIX + u'.1.4.1.3'
ciscoEnvMonSupplyState = CISCO_ENV_MON_MIB_PREFIX + u'.1.5.1.3'  # Use at least for 3560s

THIRTYFIVESIXTY_MODELS = [u"3560G-48TS-S", u"3560X-48PF-L", u"3560-48PS"]
ENV_MON_MIB_MODELS = [u"6509-E", u"none-network-sw"] + THIRTYFIVESIXTY_MODELS

FORTYNINEHUNDRED_MODEL_BUG_PATTERN = r"49\d\d.+"  # Use Kleene star to avoid matching u'4948' specifically


class CiscoIOSDeviceMetricsEnrichment(snmp.PanoptesGenericSNMPMetricsEnrichmentGroup):
    pass


class CiscoIOSPluginEnrichmentMetrics(plugin_enrichment_generic_snmp.PanoptesEnrichmentGenericSNMPPlugin):
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

        super(CiscoIOSPluginEnrichmentMetrics, self).__init__()

    def _get_cpu_interval(self):
        """
        Checks the self._execute_frequency to figure out the oid to use for cpu_utilization
        Returns:
            string: the oid to use.
        """
        self._polling_execute_frequency = int(self._plugin_conf[u'main'][u'polling_frequency'])
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
        return string.replace(u'celsius', u'fahrenheit')

    @staticmethod
    def _entity_sensor_scale_to_exponent(sensor_scale):
        """
        sensor_scale is an integer index that refers to an exponent applied to entSensorValue.  The sensor_exponent
        comes from the Cisco definitions.

        Args:
            sensor_scale(int): entSensorScale value
        Returns:
            int: signed integer exponent to be applied to entSensorValue to normalize
        """
        sensor_exponent = [u'-24', u'-21', u'-18', u'-15', u'-12', u'-9', u'-6', u'-3', u'0', u'3', u'6',
                           u'9', u'12', u'15', u'18', u'21', u'24']
        return int(sensor_exponent[sensor_scale - 1])

    @threaded_cached_property
    def _fan_status_oid(self):
        return ciscoEnvMonFanState if self._cisco_model in ENV_MON_MIB_MODELS else cefcFanTrayOperStatus

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
            if varbind.value == u'8':
                sensor_id = int(varbind.index)
                temp_sensors[sensor_id] = {u'sensor_scale':
                                           self._entity_sensor_scale_to_exponent(sensor_scales[sensor_id]),
                                           u'sensor_name': self.replace_celcius(
                                               self._entity_physical_names.get(sensor_id, u""))}

        return temp_sensors

    @threaded_cached_property
    def _num_fans(self):
        """
        Returns:
            int: The current number of system (non-psu) fans
        """

        varbinds = self._snmp_connection.bulk_walk(self._fan_status_oid)

        return len(varbinds)

    @threaded_cached_property
    def _power_supplies(self):
        """
        Pulls the entPhysicalClass items tree. We're interested in '6' (Power Supply Units).
        Returns:
            dict: power_supplies
        """
        power_supplies = {}
        varbinds = self._snmp_connection.bulk_walk(entPhysicalClass)
        for varbind in varbinds:
            if varbind.value == u'6':
                psu_id = int(varbind.index)
                power_supplies[psu_id] = {u'psu_id': psu_id, u'psu_name': self._entity_physical_names[psu_id]}

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
    def _entity_physical_descriptions(self):
        """
        Returns:
            dict: Descriptions of physical entities in the device
        """
        physical_descriptions = {}
        varbinds = self._snmp_connection.bulk_walk(entPhysicalDescr)
        for varbind in varbinds:
            physical_descriptions[int(varbind.index)] = varbind.value
        return physical_descriptions

    @threaded_cached_property
    def _memory(self):
        """
        Pulls the free memory and memory in use for the device and adds them together to produce total memory.
        All results are GAUGE32/int
        ciscoMemoryPoolUsed has more ubiquity than cemp_mem_pool_hcused
        Returns:
            dict: memory reported by the Cisco device under CISCO-PROCESS-MIB
        """
        memory = {}
        varbinds = self._snmp_connection.bulk_walk(ciscoMemoryPoolName)
        for varbind in varbinds:
            # grab the last element of the index to use as the memory_id
            memory_id = int(varbind.index.split(u'.')[-1])
            memory[memory_id] = {u'memory_name': str(varbind.value)}

        varbinds = self._snmp_connection.bulk_walk(ciscoMemoryPoolUsed)
        for varbind in varbinds:
            # grab the last element of the index to use as the memory_id
            memory_id = int(varbind.index.split(u'.')[-1])
            memory[memory_id][u'memory_used'] = int(varbind.value)

        varbinds = self._snmp_connection.bulk_walk(ciscoMemoryPoolFree)
        for varbind in varbinds:
            # grab the last element of the index to use as the memory_id
            memory_id = int(varbind.index.split(u'.')[-1])
            memory_free = int(varbind.value)
            memory[memory_id][u'memory_total'] = memory[memory_id][u'memory_used'] + memory_free

        return memory

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
            cpu_id = int(varbind.index.split('.')[-1])
            if self._cisco_model in THIRTYFIVESIXTY_MODELS:
                if str(cpu_id) in list(self._entity_physical_names.values()):  # get key for value of name == cpu_id
                    cpu_entity_table_id = int(list(self._entity_physical_names.keys())[
                                                   list(self._entity_physical_names.values()).index(str(cpu_id))])
                    cpus[cpu_id] = {u'cpu_name': self._entity_physical_descriptions[cpu_entity_table_id]}
                    cpus[cpu_id][u'cpu_no'] = u'Module ' + str(cpu_id)
            else:
                if cpu_id in list(self._entity_physical_names.keys()) and cpu_id in list(self._module_numbers.keys()):
                    cpus[cpu_id] = {u'cpu_name': self._entity_physical_names[cpu_id]}
                    cpus[cpu_id][u'cpu_no'] = u'Module ' + str(cpu_id)

        return cpus

    def _build_oids_map(self):
        """See base class."""
        self._oids_map = {
            u"cpu_name": {
                u"method": u"static",
                u"values": {x: self._cpus[x][u'cpu_name'] for x in self._cpus}
            },
            u"cpu_no": {
                u"method": u"static",
                u"values": {x: self._cpus[x][u'cpu_no'] for x in self._cpus}
            },
            u"cpu_util": {
                u"method": u"bulk_walk",
                u"oid": self._get_cpu_interval()
            },
            u"memory_used": {
                u"method": u"bulk_walk",
                u"oid": ciscoMemoryPoolUsed
            },
            u"memory_total": {
                u"method": u"static",
                u"values": {x: self._memory[x][u'memory_total'] for x in self._memory}
            },
            u"memory_name": {
                u"method": u"static",
                u"values": {x: self._memory[x][u'memory_name'] for x in self._memory}
            },
            u"fan_statuses": {
                u"method": u"bulk_walk",
                u"oid": self._fan_status_oid
            },
            u"entity_fru_control": {
                u"method": u"bulk_walk",
                u"oid": cefcFRUPowerOperStatus
            },
            u"power_status": {
                u"method": u"bulk_walk",
                u"oid": ciscoEnvMonSupplyState
            },
            u"power_supplies": {
                u"method": u"static",
                u"values": {x: self._power_supplies[x][u'psu_name'] for x in self._power_supplies}
            },
            u"ent_sensor_values": {
                u"method": u"bulk_walk",
                u"oid": entSensorValues
            }
        }

        if self._cisco_model not in THIRTYFIVESIXTY_MODELS:
            if re.match(FORTYNINEHUNDRED_MODEL_BUG_PATTERN, self._cisco_model):
                self._oids_map[u"temp_sensor_scales"] = {
                    u"method": u"static",
                    #  IOS temperature metrics for 4900 models appear to be off by a factor of 10, so adjust.
                    u"values": {x: math.pow(10, self._temp_sensors[x][u'sensor_scale'] - 1) for x in self._temp_sensors}
                }
            else:
                self._oids_map[u"temp_sensor_scales"] = {
                    u"method": u"static",
                    u"values": {x: math.pow(10, self._temp_sensors[x][u'sensor_scale']) for x in self._temp_sensors}
                }

            self._oids_map[u"temp_sensor_name"] = {
                u"method": u"static",
                u"values": {x: self._temp_sensors[x][u'sensor_name'] for x in self._temp_sensors}
            }

        self._logger.debug('fan status oid: %s' % self._fan_status_oid)

    def _build_metrics_groups_conf(self):
        """See base class."""
        self._metrics_groups = [
            {
                u"group_name": u"environment",
                u"dimensions": {},
                u"metrics": {
                    u"fans_ok": {
                        u"metric_type": "gauge",
                        u"value": "len([x for x in fan_statuses.values() if x == '2'])"
                        if self._fan_status_oid == cefcFanTrayOperStatus else
                        u"len([x for x in fan_statuses.values() if x in ['1', '2', '3']])"
                    },
                    u"fans_total": self._num_fans
                }
            },
            {
                u"group_name": u"environment",
                u"dimensions": {},
                u"metrics": {
                    u"power_units_on": {
                        u"metric_type": u"gauge",
                        # http://www.circitor.fr/Mibs/Html/C/CISCO-ENVMON-MIB.php#CiscoEnvMonState
                        # http://www.circitor.fr/Mibs/Html/C/CISCO-ENTITY-FRU-CONTROL-MIB.php#PowerOperType
                        u"value": u"len([(x,y) for (x,y) in power_status.items() if x in power_supplies and y "
                                  u"in ['1', '2', '3']])" if self._cisco_model in THIRTYFIVESIXTY_MODELS else
                        u"len([(x,y) for (x,y) in entity_fru_control.items() if x in power_supplies and "
                        u"y in ['2', '9', '12']])"
                    },
                    u"power_units_total": len(self._power_supplies)
                }
            },
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
                    u"memory_type": u"memory_name.$index"
                },
                u"metrics": {
                    u"memory_used": {
                        u"metric_type": u"gauge",
                        u"value": u"memory_used.$index"
                    },
                    u"memory_total": {
                        u"metric_type": u"gauge",
                        u"value": u"memory_total.$index"
                    }
                }
            }
        ]
        if self._cisco_model not in THIRTYFIVESIXTY_MODELS:
            self._metrics_groups.append({
                u"group_name": u"environment",
                u"dimensions": {
                    u"sensor": u"temp_sensor_name.$index"
                },
                u"metrics": {
                    u"temperature_fahrenheit": {
                        u"metric_type": u"gauge",
                        u"type": u"float",
                        u"indices_from": u"temp_sensor_scales",
                        u"transform": u"lambda x: round((x * 1.8) + 32, 2)",
                        u"value": u"int(ent_sensor_values.$index) * temp_sensor_scales.$index"
                    }
                }
            })

    @property
    def metrics_enrichment_class(self):
        return CiscoIOSDeviceMetricsEnrichment

    def get_enrichment(self):
        """See base class."""
        self._cisco_model = self._plugin_context.data.resource_metadata.get(u'model', u'unknown')
        self._build_oids_map()
        self._build_metrics_groups_conf()

        enrichment_set = {
            u"oids": self.oids_map,
            u"metrics_groups": self.metrics_groups
        }

        try:
            self.enrichment_group.add_enrichment_set(enrichment.PanoptesEnrichmentSet(self.device_fqdn, enrichment_set))
        except Exception as e:
            self._logger.error(u'Error while adding enrichment set {} to enrichment group for the device {}: {}'.
                               format(enrichment_set, self.device_fqdn, repr(e)))

        self.enrichment_group_set.add_enrichment_group(self.enrichment_group)

        self._logger.debug(u'Metrics enrichment for device {}: {}'.format(self.device_fqdn, self.enrichment_group_set))

        return self.enrichment_group_set
