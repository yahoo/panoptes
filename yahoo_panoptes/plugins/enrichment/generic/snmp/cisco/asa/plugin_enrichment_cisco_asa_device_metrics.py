"""
This module implements a Panoptes Plugin that can poll Cisco ASA 5500 devices for Device Metrics
ftp://ftp.cisco.com/pub/mibs/supportlists/asa/asa-supportlist.html

API: https://www.cisco.com/c/en/us/td/docs/security/asa/api/qsg-asa-api.html
CLI: https://www.cisco.com/c/en/us/td/docs/security/asa/asa96/configuration/general/asa-96-general-config.html

"""
from cached_property import threaded_cached_property

from yahoo_panoptes.framework.enrichment import PanoptesEnrichmentSet
from yahoo_panoptes.plugins.enrichment.generic.snmp.plugin_enrichment_generic_snmp import \
    PanoptesEnrichmentGenericSNMPPlugin

# entities
ENTITY_MIB_PREFIX = u'.1.3.6.1.2.1.47'
# Physical entries
entPhysicalEntry = ENTITY_MIB_PREFIX + u'.1.1.1.1'
entPhysicalDescr = ENTITY_MIB_PREFIX + u'.1.1.1.1.2'
entPhysicalClass = ENTITY_MIB_PREFIX + u'.1.1.1.1.5'
entPhysicalParentRelPos = ENTITY_MIB_PREFIX + u'.1.1.1.1.6'
entPhysicalName = ENTITY_MIB_PREFIX + u'.1.1.1.1.7'

# ciscoMgmt
CISCO_MANAGEMENT_PREFIX = u'.1.3.6.1.4.1.9.9'

# ciscoSensorMIB (91) is not available for temperature sensors
# ciscoEntityFRUControlMIB (117) not available for fans/power
# ciscoEntityQfpMIB (715) not available for QFP
# ciscoEntityPerformanceMIB (756) not available for packets in/out

# CPU
CISCO_PROCESS_MIB_PREFIX = CISCO_MANAGEMENT_PREFIX + u'.109'
cpmCPUTotal1minRev = CISCO_PROCESS_MIB_PREFIX + u'.1.1.1.1.7'
cpmCPUTotal5minRev = CISCO_PROCESS_MIB_PREFIX + u'.1.1.1.1.8'
cpmCPUTotalMonIntervalValue = CISCO_PROCESS_MIB_PREFIX + u'.1.1.1.1.10'
cpmCPUTotalPhysicalIndex = CISCO_PROCESS_MIB_PREFIX + u'.1.1.1.1.2'

# Enhanced memory pool
CISCO_ENHANCED_MEMPOOL_PREFIX = CISCO_MANAGEMENT_PREFIX + u'.221'
ciscoMemoryPoolFree = CISCO_ENHANCED_MEMPOOL_PREFIX + u'.1.1.1.1.20'
ciscoMemoryPoolUsed = CISCO_ENHANCED_MEMPOOL_PREFIX + u'.1.1.1.1.18'
ciscoMemoryPoolName = CISCO_ENHANCED_MEMPOOL_PREFIX + u'.1.1.1.1.3'


class CiscoASAPluginEnrichmentMetrics(PanoptesEnrichmentGenericSNMPPlugin):
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

        super(CiscoASAPluginEnrichmentMetrics, self).__init__()

    def _get_cpu_interval(self):
        """
        Checks the self._execute_frequency to figure out the oid to use for cpu_utilization

        Returns:
            str: the oid to use.
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
        physical_names = {}
        varbinds = self._snmp_connection.bulk_walk(entPhysicalName)
        for varbind in varbinds:
            value = varbind.value
            if isinstance(value, bytes):
                value = value.decode(u'ascii', u'ignore')
            physical_names[int(varbind.index)] = value
        return physical_names

    @threaded_cached_property
    def _entity_physical_classes(self):
        physical_classes = {}
        varbinds = self._snmp_connection.bulk_walk(entPhysicalClass)
        for varbind in varbinds:
            physical_classes[int(varbind.index)] = varbind.value
        return physical_classes

    @threaded_cached_property
    def _memory(self):
        """
        Pulls the free memory and memory in use for the device and adds them together to produce total memory.
        All results are GAUGE32/int

        ASA devices require the use of the Enhanced Mempool OID
        Returns:
            dict: memory reported by the Cisco device under CISCO_ENHANCED_MEMPOOL
        """
        memory = {}
        varbinds = self._snmp_connection.bulk_walk(ciscoMemoryPoolName)
        for varbind in varbinds:
            value = varbind.value

            if isinstance(value, bytes):
                value = value.decode(u'ascii', u'ignore')
            # grab the last element of the index to use as the memory_id
            memory_id = int(varbind.index.split(u'.')[-1])
            memory[memory_id] = {u'memory_name': str(value)}

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
            dict: cpus in the system defined by self._entity_physical_classes
        """
        cpus = {}
        varbinds = self._snmp_connection.bulk_walk(self._get_cpu_interval())
        for varbind in varbinds:
            # grab the last element of the index to use as the cpu_id
            cpu_id = int(varbind.index.split(u'.')[-1])
            if cpu_id in list(self._entity_physical_names.keys()) and cpu_id in list(self._module_numbers.keys()) \
                    and self._entity_physical_classes[cpu_id] == u'12':
                cpus[cpu_id] = {u'cpu_name': self._entity_physical_names[cpu_id]}
                cpus[cpu_id][u'cpu_no'] = u'Module ' + str(cpu_id)

        return cpus

    def _build_oids_map(self):
        self._oids_map = {
            "cpu_name": {
                "method": "static",
                "values": {x: self._cpus[x][u'cpu_name'] for x in self._cpus}
            },
            "cpu_no": {
                "method": "static",
                "values": {x: self._cpus[x][u'cpu_no'] for x in self._cpus}
            },
            "cpu_util": {
                "method": "bulk_walk",
                "oid": self._get_cpu_interval()
            },
            "memory_used": {
                "method": "bulk_walk",
                "oid": ciscoMemoryPoolUsed
            },
            "memory_total": {
                "method": "static",
                "values": {x: self._memory[x][u'memory_total'] for x in self._memory}
            },
            "memory_name": {
                "method": "static",
                "values": {x: self._memory[x][u'memory_name'] for x in self._memory}
            }
        }

    def _build_metrics_groups_conf(self):
        self._metrics_groups = [
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
                    "memory_type": "memory_name.$index"
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

    def get_enrichment(self):
        self._build_oids_map()
        self._build_metrics_groups_conf()

        enrichment_set = {
            "oids": self.oids_map,
            "metrics_groups": self.metrics_groups
        }
        try:
            self.enrichment_group.add_enrichment_set(PanoptesEnrichmentSet(self.device_fqdn, enrichment_set))
        except Exception as e:
            self._logger.error(u'Error while adding enrichment set {} to enrichment group for the device {}: {}'.
                               format(enrichment_set, self.device_fqdn, repr(e)))

        self.enrichment_group_set.add_enrichment_group(self.enrichment_group)

        self._logger.debug(u'Metrics enrichment for device {}: {}'.format(self.device_fqdn, self.enrichment_group_set))

        return self.enrichment_group_set
