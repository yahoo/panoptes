"""
This module implements a Panoptes Plugin that can poll Cisco ASR 1000 devices for Device Metrics
"""
from __future__ import division
from past.utils import old_div
import time
from cached_property import threaded_cached_property

from yahoo_panoptes.framework.metrics import PanoptesMetricsGroupSet, PanoptesMetricsGroup, PanoptesMetric, \
    PanoptesMetricType, PanoptesMetricDimension
from yahoo_panoptes.polling.polling_plugin import PanoptesPollingPlugin, PanoptesPollingPluginConfigurationError
from yahoo_panoptes.plugins.polling.utilities.polling_status import PanoptesPollingStatus, DEVICE_METRICS_STATES
from yahoo_panoptes.framework.metrics import PanoptesMetricsNullException
from yahoo_panoptes.plugins.helpers.snmp_connections import PanoptesSNMPConnectionFactory
from yahoo_panoptes.framework.utilities.helpers import convert_celsius_to_fahrenheit

CISCO_PROCESS_MIB_PREFIX = u'.1.3.6.1.4.1.9.9.109'
ENTITY_MIB_PREFIX = u'.1.3.6.1.2.1.47'
CISCO_ENHANCED_MEMPOOL_PREFIX = u'.1.3.6.1.4.1.9.9.221'
CISCO_ENTITY_SENSOR_MIB_PREFIX = u'.1.3.6.1.4.1.9.9.91.1'
CISCO_ENTITY_FRU_CONTROL_PREFIX = u'.1.3.6.1.4.1.9.9.117'

cpmCPUTotal1minRev = CISCO_PROCESS_MIB_PREFIX + u'.1.1.1.1.7'
cpmCPUTotal5minRev = CISCO_PROCESS_MIB_PREFIX + u'.1.1.1.1.8'
cpmCPUTotalMonIntervalValue = CISCO_PROCESS_MIB_PREFIX + u'.1.1.1.1.10'
cpmCPUTotalPhysicalIndex = CISCO_PROCESS_MIB_PREFIX + u'.1.1.1.1.2'

entPhysicalEntry = ENTITY_MIB_PREFIX + u'.1.1.1.1'
entPhysicalDescrPrefix = ENTITY_MIB_PREFIX + u'.1.1.1.1.2'
entPhysicalClassPrefix = ENTITY_MIB_PREFIX + u'.1.1.1.1.5'
entPhysicalNamePrefix = ENTITY_MIB_PREFIX + u'.1.1.1.1.7'

cempMemPoolHCFree = CISCO_ENHANCED_MEMPOOL_PREFIX + u'.1.1.1.1.20.7000.1'
cempMemPoolHCUsed = CISCO_ENHANCED_MEMPOOL_PREFIX + u'.1.1.1.1.18.7000.1'

cefcFRUPowerOperStatus = CISCO_ENTITY_FRU_CONTROL_PREFIX + u'.1.1.2.1.2'

entSensorValueEntry = CISCO_ENTITY_SENSOR_MIB_PREFIX + u'.1.1.1'
entSensorType = entSensorValueEntry + u'.1'
entSensorScale = entSensorValueEntry + u'.2'
entSensorValue = entSensorValueEntry + u'.4'
entSensorStatus = entSensorValueEntry + u'.5'

CISCO_ENTITY_QFP_MIB_PREFIX = u'.1.3.6.1.4.1.9.9.715'
ceqfpUtilizationEntry = CISCO_ENTITY_QFP_MIB_PREFIX + u'.1.1.6.1'
ceqfpUtilProcessingLoad = ceqfpUtilizationEntry + u'.14'
ceqfpMemoryResourceEntry = CISCO_ENTITY_QFP_MIB_PREFIX + u'.1.1.7.1'
ceqfpMemoryResTotal = ceqfpMemoryResourceEntry + u'.2'  # TODO Seems to report '0'; use inUse + Free instead.
ceqfpMemoryResInUse = ceqfpMemoryResourceEntry + u'.3'
ceqfpMemoryResFree = ceqfpMemoryResourceEntry + u'.4'

CISCO_ENTITY_PERFORMANCE_MIB_PREFIX = u'.1.3.6.1.4.1.9.9.756'
cepStatsMeasurement = CISCO_ENTITY_PERFORMANCE_MIB_PREFIX + u'.1.3.1.2'
cpuUtil = u'.1'
pktsIn = u'.5'
pktsOut = u'.6'

ENT_PHYSICAL_CLASSES = {1: u'other',
                        2: u'unknown',
                        3: u'chassis',
                        4: u'backplane',
                        5: u'container',
                        6: u'powerSupply',
                        7: u'fan',
                        8: u'sensor',
                        9: u'module',
                        10: u'port',
                        11: u'stack',
                        12: u'cpu'
                        }

MILLI_ENT_STRINGS = [u'subslot 0/0 transceiver 0 Temperature Sensor',
                     u'subslot 0/1 transceiver 0 Temperature Sensor',
                     u'subslot 0/2 transceiver 0 Temperature Sensor',
                     u'subslot 0/3 transceiver 0 Temperature Sensor']

_MAX_REPETITIONS = 25


class PluginPollingASRDeviceMetrics(PanoptesPollingPlugin):
    def __init__(self):
        self._plugin_context = None
        self._logger = None
        self._device = None
        self._device_host = None
        self._device_model = None
        self._execute_frequency = None
        self._snmp_connection = None
        self._asr_device_metrics = PanoptesMetricsGroupSet()
        self._polling_status = None
        self._max_repetitions = None

        self._cpu_metrics = None
        self._memory_metrics = None
        self._temp_metrics = None
        self._power_metrics = None
        self._crypto_metrics = None
        self._load_metrics = None

        super(PluginPollingASRDeviceMetrics, self).__init__()

    def _get_crypto_cpu_interval(self):
        if self._execute_frequency < 60:
            return u'2'
        elif 60 < self._execute_frequency < 300:
            return u'3'
        elif 300 < self._execute_frequency < 900:
            return u'4'
        else:
            return u'2'

    def _get_qfp_interval(self):
        if 5 <= self._execute_frequency < 60:
            return u'1'
        elif 60 <= self._execute_frequency < 300:
            return u'2'
        elif 300 <= self._execute_frequency < 3600:
            return u'3'
        elif 3600 <= self._execute_frequency:
            return u'4'
        else:
            return u'2'

    # TODO is mutable type allowed?
    def _get_entity_indices(self, ent_physical_class, ent_strings):
        ent_indices = []
        # https://github.com/PyCQA/pylint/issues/1694
        for ent in self.entities:  # pylint: disable=E1133
            value = ent.value
            if isinstance(value, bytes):
                value = value.decode(u'ascii', u'ignore')
            physical_class = entPhysicalClassPrefix + u'.' + ent.index.split(u'.')[-1]  # pylint: disable=E1133
            physical_class_index = int(self.entity_physical_entries_map[physical_class])  # pylint: disable=E1136
            if ENT_PHYSICAL_CLASSES[physical_class_index] == ent_physical_class:
                for s in ent_strings:
                    if s in value:
                        ent_indices.append(ent.index)
        return ent_indices

    @threaded_cached_property
    def entities(self):
        return self._snmp_connection.bulk_walk(oid=entPhysicalEntry, non_repeaters=0,
                                               max_repetitions=25)

    @threaded_cached_property
    def entity_physical_entries_map(self):
        """Maps child oids of entPhysicalEntry to their respective values as PanoptesSNMPVariables"""
        ent_physical_entries_map = {}
        # https://github.com/PyCQA/pylint/issues/1694
        for ent in self.entities:  # pylint: disable=E1133
            value = ent.value
            if isinstance(value, bytes):
                value = value.decode(u'ascii', u'ignore')
            ent_physical_entries_map[ent.oid + u'.' + ent.index] = value
        return ent_physical_entries_map

    @threaded_cached_property
    def sensor_entities(self):
        return self._snmp_connection.bulk_walk(oid=entSensorValueEntry, non_repeaters=0,
                                               max_repetitions=25)

    @threaded_cached_property
    def sensor_entity_map(self):
        """Maps child oids of entSensorValueEntry to their respective values as PanoptesSNMPVariables"""
        sensor_ent_map = {}
        # https://github.com/PyCQA/pylint/issues/1694
        for ent in self.sensor_entities:  # pylint: disable=E1133
            sensor_ent_map[ent.oid + u'.' + ent.index] = ent.value
        return sensor_ent_map

    def _get_sensor_details(self, index):
        try:
            details = dict()

            entPhysicalNamePrefixIndex = entPhysicalNamePrefix + u'.' + index
            entSensorValueIndex = entSensorValue + u'.' + index
            # https://github.com/PyCQA/pylint/issues/1694
            details[u'sensor_value'] = int(self.sensor_entity_map[entSensorValueIndex])  # pylint: disable=E1136

            # https://github.com/PyCQA/pylint/issues/1694
            entity_description = self.entity_physical_entries_map[entPhysicalNamePrefixIndex]  # pylint: disable=E1136
            if entity_description in MILLI_ENT_STRINGS:
                # TODO how many sig digs?
                details[u'sensor_value'] = old_div(details[u'sensor_value'], 1000)

            sensor_scale_code = int(self.sensor_entity_map[entSensorScale + u'.' + index])  # pylint: disable=E1136
            details[u'sensor_scale'] = sensor_scale_code
            return details
        except Exception as e:
            raise e

    def _is_celsius_sensor_type(self, index):
        ans = int(self.sensor_entity_map[entSensorType + u'.' + index])  # pylint: disable=E1136
        if ans != 8:
            self._logger.warn(u"Entity Sensor Type not Celsius: %d" % ans)
        return ans == 8

    def _get_cpu_name(self, cpu_id):
        cpu_name = self.entity_physical_entries_map[entPhysicalNamePrefix + u'.' + cpu_id]  # pylint: disable=E1136
        if isinstance(cpu_name, bytes):
            cpu_name = cpu_name.decode(u'ascii', u'ignore')
        return cpu_name

    def _get_cpu_id(self, temp_id):
        id = self._snmp_connection.get(oid=cpmCPUTotalPhysicalIndex + u'.' + str(temp_id))
        return id.value

    def _get_cpu_interval(self):
        if 5 <= self._execute_frequency < 60:
            return cpmCPUTotalMonIntervalValue  # replaces cpmCPUTotal5SecRev
        elif 60 <= self._execute_frequency < 300:
            return cpmCPUTotal1minRev
        elif 300 <= self._execute_frequency:
            return cpmCPUTotal5minRev
        else:
            return cpmCPUTotal1minRev

    def _get_load_metrics(self):
        try:
            interval = self._get_qfp_interval()
            # n.b. There should only be one qfp entry per crypto device.
            qfp_entry_index = self._get_entity_indices(ent_physical_class=u'cpu',
                                                       ent_strings=[u'qfp', u'QFP'])[0].split(u'.')[-1]
            self._load_metrics = dict()
            processing_load = int(self._snmp_connection.get(
                oid=ceqfpUtilProcessingLoad + u'.' + qfp_entry_index + u'.' + interval).value)
            self._load_metrics[u'processing_load'] = processing_load
        except Exception as e:
            self._polling_status.handle_exception(u'load', e)

        try:
            if self._load_metrics:
                load_metrics_group = PanoptesMetricsGroup(self._device, u'load', self._execute_frequency)
                load_metrics_group.add_metric(PanoptesMetric(u'processing_load',
                                                             self._load_metrics[u'processing_load'],
                                                             PanoptesMetricType.GAUGE))
                self._asr_device_metrics.add(load_metrics_group)
                self._polling_status.handle_success(u'load')
                self._logger.debug(
                    u'Found load metrics "%s" for %s: %s' % (self._load_metrics, self._polling_status.device_type,
                                                             self._device_host))
        except Exception as e:
            self._polling_status.handle_exception(u'load', e)

    def _get_crypto_metrics(self):
        self._crypto_metrics = dict()
        try:
            crypto_cpu_entry_indices = set([x.split(u'.')[-1] for x in
                                            self._get_entity_indices(ent_physical_class=u'cpu',
                                            ent_strings=[u'Crypto Asic'])])
            interval = self._get_crypto_cpu_interval()
            for index in crypto_cpu_entry_indices:
                self._crypto_metrics[index] = dict()
                packets_in = int(self._snmp_connection.get(
                    oid=cepStatsMeasurement + u'.' + index + u'.' + interval + pktsIn).value)
                packets_out = int(self._snmp_connection.get(
                    oid=cepStatsMeasurement + u'.' + index + u'.' + interval + pktsOut).value)
                self._crypto_metrics[index][u'packets_in'] = packets_in
                self._crypto_metrics[index][u'packets_out'] = packets_out
                self._crypto_metrics[index][u'cpu_name'] = self._get_cpu_name(index)
        except Exception as e:
            self._polling_status.handle_exception(u'crypto', e)

        try:
            if self._crypto_metrics:
                for cpu_id in self._crypto_metrics:
                    crypto_metrics_group = PanoptesMetricsGroup(self._device, u'crypto', self._execute_frequency)
                    crypto_metrics_group.add_dimension(PanoptesMetricDimension(u'cpu_no', cpu_id))
                    crypto_metrics_group.add_dimension(PanoptesMetricDimension(u'cpu_name',
                                                                               self._crypto_metrics[cpu_id]
                                                                               [u'cpu_name']))
                    crypto_metrics_group.add_metric(PanoptesMetric(u'packets_in',
                                                                   self._crypto_metrics[cpu_id][u'packets_in'],
                                                                   PanoptesMetricType.COUNTER))
                    crypto_metrics_group.add_metric(PanoptesMetric(u'packets_out',
                                                                   self._crypto_metrics[cpu_id][u'packets_out'],
                                                                   PanoptesMetricType.COUNTER))
                    self._asr_device_metrics.add(crypto_metrics_group)
                self._polling_status.handle_success(u'crypto')
                self._logger.debug(
                    u'Found crypto metrics "%s" for %s: %s' % (self._crypto_metrics, self._polling_status.device_type,
                                                               self._device_host))
        except Exception as e:
            self._polling_status.handle_exception(u'crypto', e)

    def _convert_scaled_celsius_value_to_units(self, x, scale):  # TODO introducing bug when I make static
        """Convert the provided value in scale-units Celsius to Celsius. N.B. The indices for 'peta' and 'exa'
        appear to be flip-flopped"""
        if scale == 14:
            scale = 15
        elif scale == 15:
            scale = 14
        return x * (10 ** (scale - 9))

    def _get_power_metrics(self):
        try:
            power_metrics = dict()
            power_metrics[u'power_module_map'] = dict()
            power_entity_indices = self._get_entity_indices(ent_physical_class=u'powerSupply',
                                                            ent_strings=[u'Power Supply Module'])
            power_metrics[u'power_units_total'] = len(power_entity_indices)
            for index in power_entity_indices:
                index = index.split(u'.')[-1]
                entity_name_index = entPhysicalNamePrefix + u'.' + index
                entity_name = self.entity_physical_entries_map[entity_name_index]  # pylint: disable=E1136
                power_on = True if int(self._snmp_connection.get(oid=cefcFRUPowerOperStatus + u'.' + index).value) \
                    == 2 else False
                power_metrics[u'power_module_map'][index] = {
                    u'entity_name': entity_name,
                    u'power_on': power_on
                }
                # Todo Collect current as well? -- Not yet
            self._power_metrics = power_metrics
        except Exception as e:
            raise e

    def _get_temperature_metrics(self):
        """Don't rely on the ASR's thresholds; report everything and do our own alerting."""
        try:
            temp_metrics = dict()
            temperature_entity_indices = self._get_entity_indices(ent_physical_class=u'sensor',
                                                                  ent_strings=[u'temp', u'Temp'])
            for index in temperature_entity_indices:
                index = index.split(u'.')[-1]
                temp_metrics[index] = {
                    u'entity_name':
                        self.entity_physical_entries_map[entPhysicalNamePrefix + u'.' + index]  # pylint: disable=E1136
                }
                if self._is_celsius_sensor_type(index):
                    temp_metrics[index][u'sensor_details'] = self._get_sensor_details(index)
                    celsius_value = temp_metrics[index][u'sensor_details'][u'sensor_value']
                    scale = temp_metrics[index][u'sensor_details'][u'sensor_scale']
                    celsius_value_in_units = self._convert_scaled_celsius_value_to_units(celsius_value, scale)
                    temp_f = convert_celsius_to_fahrenheit(celsius_value_in_units)

                    if 33 < temp_f < 200:
                        temp_metrics[index]['temp_f'] = temp_f
                    else:
                        del temp_metrics[index]

            self._temp_metrics = temp_metrics
        except Exception as e:
            raise e

    def _get_environment_metrics(self):
        try:
            self._get_temperature_metrics()
            self._logger.debug(
                u'Found Temperature metrics "%s" for %s: %s' % (self._temp_metrics, self._polling_status.device_type,
                                                                self._device_host))

        except Exception as e:
            self._polling_status.handle_exception(u'environment', e)

        try:
            self._get_power_metrics()
            self._logger.debug(
                u'Found Power metrics "%s" for %s: %s' % (self._power_metrics, self._polling_status.device_type,
                                                          self._device_host)
            )
            # TODO Valuable to monitor cefcFRUPowerOperStatus for status "9: onButFanFail"? -- Yes, but not now
        except Exception as e:
            self._polling_status.handle_exception(u'environment', e)

        try:
            if self._temp_metrics:
                for index in list(self._temp_metrics.keys()):
                    environment_metrics_group = PanoptesMetricsGroup(self._device, u'environment',
                                                                     self._execute_frequency)
                    environment_metrics_group.add_dimension(
                        PanoptesMetricDimension(u'entity_name',
                                                self._temp_metrics[index][u'entity_name']))
                    environment_metrics_group.add_metric(PanoptesMetric(u'temperature_fahrenheit',
                                                                        self._temp_metrics[index][u'temp_f'],
                                                                        PanoptesMetricType.GAUGE))
                    self._asr_device_metrics.add(environment_metrics_group)
                    self._polling_status.handle_success(u'environment')
        except Exception as e:
            self._polling_status.handle_exception(u'environment', e)

        # TODO Do we need to report sensor details as well? -- Not yet
        try:
            if self._power_metrics:
                environment_metrics_group = PanoptesMetricsGroup(self._device, u'environment',
                                                                 self._execute_frequency)
                num_power_units_on = 0
                for index in list(self._power_metrics[u'power_module_map'].keys()):
                    if self._power_metrics[u'power_module_map'][index][u'power_on']:
                        num_power_units_on += 1
                environment_metrics_group.add_metric(PanoptesMetric(u'power_units_total',
                                                                    self._power_metrics[u'power_units_total'],
                                                                    PanoptesMetricType.GAUGE))
                environment_metrics_group.add_metric(PanoptesMetric(u'power_units_on',
                                                                    num_power_units_on,
                                                                    PanoptesMetricType.GAUGE))
                self._asr_device_metrics.add(environment_metrics_group)
                self._polling_status.handle_success(u'environment')
        except Exception as e:
            self._polling_status.handle_exception(u'environment', e)

    def _get_memory_metrics(self):
        self._memory_metrics = dict()
        self._memory_metrics[u'dram'] = dict()
        try:
            memory_used = int(self._snmp_connection.get(oid=cempMemPoolHCUsed).value)
            self._memory_metrics[u'dram'][u'memory_used'] = memory_used
            memory_free = int(self._snmp_connection.get(oid=cempMemPoolHCFree).value)
            self._memory_metrics[u'dram'][u'memory_total'] = memory_used + memory_free
        except Exception as e:
            self._polling_status.handle_exception(u'memory', e)
            self._memory_metrics.pop(u'dram')

        self._memory_metrics[u'qfp'] = dict()  # TODO Safe to assume only one qfp_entry?
        try:
            qfp_entry_indices = set([x.split(u'.')[-1] for x in self._get_entity_indices(ent_physical_class=u'cpu',
                                                                                         ent_strings=[u'qfp', u'QFP'])])
            for index in qfp_entry_indices:
                qfp_memory_used = int(self._snmp_connection.get(oid=ceqfpMemoryResInUse + u'.' + index + u'.' + u'1')
                                      .value)
                self._memory_metrics[u'qfp'][u'memory_used'] = qfp_memory_used
                qfp_memory_free = int(self._snmp_connection.get(
                    oid=ceqfpMemoryResFree + u'.' + index + u'.' + u'1').value)
                self._memory_metrics[u'qfp'][u'memory_total'] = qfp_memory_used + qfp_memory_free

        except Exception as e:
            self._polling_status.handle_exception(u'memory', e)
            self._memory_metrics.pop(u'qfp')  # TODO Safe to assume only one qfp_entry?

        try:
            if len(self._memory_metrics) > 0:
                for memory_type in self._memory_metrics:
                    memory_metrics_group = PanoptesMetricsGroup(self._device, u'memory', self._execute_frequency)
                    memory_metrics_group.add_dimension(PanoptesMetricDimension(u'memory_type', memory_type))
                    memory_metrics_group.add_metric(PanoptesMetric(u'memory_used',
                                                                   self._memory_metrics[memory_type][u'memory_used'],
                                                                   PanoptesMetricType.GAUGE))
                    memory_metrics_group.add_metric(PanoptesMetric(u'memory_total',
                                                                   self._memory_metrics[memory_type][u'memory_total'],
                                                                   PanoptesMetricType.GAUGE))
                    self._asr_device_metrics.add(memory_metrics_group)
                self._polling_status.handle_success(u'memory')
                self._logger.debug(
                    u'Found Memory metrics "%s" for %s: %s' % (self._memory_metrics, self._polling_status.device_type,
                                                               self._device_host))
        except Exception as e:
            self._polling_status.handle_exception(u'memory', e)

    def _get_system_cpu_metrics(self):

        self._cpu_metrics = dict()
        self._cpu_metrics[u'ctrl'] = dict()

        try:
            cpus = self._snmp_connection.bulk_walk(oid=self._get_cpu_interval(), non_repeaters=0, max_repetitions=25)
            if len(cpus) == 0:
                raise PanoptesMetricsNullException

            for cpu in cpus:
                # The last int for each cpu is a temporary index we will append to the entPhysicalNamePrefix
                # and cpmCPUTotalPhysicalIndex OIDS to get the cpu name and id values, respectively
                temp_id = int(cpu.index.rsplit(u'.', 1)[-1])  # last object
                cpu_id = self._get_cpu_id(temp_id)
                self._cpu_metrics[u'ctrl'][cpu_id] = dict()
                self._cpu_metrics[u'ctrl'][cpu_id][u'cpu_util'] = int(cpu.value)
                self._cpu_metrics[u'ctrl'][cpu_id][u'cpu_name'] = self._get_cpu_name(cpu_id)  # report name, num as dim

        except Exception as e:
            self._polling_status.handle_exception(u'cpu', e)
            self._cpu_metrics.pop(u'ctrl')

        self._cpu_metrics[u'data'] = dict()

        try:
            interval = self._get_crypto_cpu_interval()
            crypto_cpu_entry_indices = set([x.split(u'.')[-1] for x in
                                            self._get_entity_indices(ent_physical_class=u'cpu',
                                            ent_strings=[u'Crypto Asic'])])
            for index in crypto_cpu_entry_indices:
                self._cpu_metrics[u'data'][index] = dict()
                # todo special def for u'1'/util?
                cpu_util = int(self._snmp_connection.get(
                    oid=cepStatsMeasurement + u'.' + index + u'.' + interval + cpuUtil).value)
                self._cpu_metrics[u'data'][index][u'cpu_util'] = cpu_util
                self._cpu_metrics[u'data'][index][u'cpu_name'] = self._get_cpu_name(index)
        except Exception as e:
            self._polling_status.handle_exception(u'cpu', e)
            self._cpu_metrics.pop(u'data')

        try:
            if len(self._cpu_metrics) > 0:
                for cpu_type in self._cpu_metrics:
                    for cpu_id in list(self._cpu_metrics[cpu_type].keys()):
                        cpu_metrics_group = PanoptesMetricsGroup(self._device, u'cpu', self._execute_frequency)
                        cpu_metrics_group.add_dimension(PanoptesMetricDimension(u'cpu_type', cpu_type))
                        cpu_metrics_group.add_dimension(PanoptesMetricDimension(u'cpu_no', cpu_id))
                        cpu_metrics_group.add_dimension(PanoptesMetricDimension(u'cpu_name',
                                                                                self._cpu_metrics[cpu_type][cpu_id]
                                                                                [u'cpu_name']))
                        cpu_metrics_group.add_metric(PanoptesMetric(u'cpu_utilization',
                                                                    self._cpu_metrics[cpu_type][cpu_id][u'cpu_util'],
                                                                    PanoptesMetricType.GAUGE))
                        self._asr_device_metrics.add(cpu_metrics_group)
                self._polling_status.handle_success(u'cpu')
                self._logger.debug(
                    u'Found CPU metrics "%s" for %s: %s' % (self._cpu_metrics, self._polling_status.device_type,
                                                            self._device_host))
        except Exception as e:
            self._polling_status.handle_exception(u'cpu', e)

    def get_device_metrics(self):
        start_time = time.time()

        try:
            start_time = time.time()
            self._snmp_connection = PanoptesSNMPConnectionFactory.get_snmp_connection(
                plugin_context=self._plugin_context, resource=self._device)
        except Exception as e:
            self._polling_status.handle_exception(u'device', e)
        finally:
            if self._polling_status.device_status != DEVICE_METRICS_STATES.SUCCESS:
                self._asr_device_metrics.add(self._polling_status.device_status_metrics_group)
                return self._asr_device_metrics

        self._get_system_cpu_metrics()
        self._get_memory_metrics()
        self._get_environment_metrics()
        self._get_crypto_metrics()
        self._get_load_metrics()

        end_time = time.time()

        self._logger.info(u'SNMP calls for ASR %s completed in %.2f seconds' %
                          (self._device_host, end_time - start_time))

        self._asr_device_metrics.add(self._polling_status.device_status_metrics_group)
        return self._asr_device_metrics

    def run(self, context):
        self._plugin_context = context
        self._logger = context.logger
        self._device = context.data
        self._device_host = self._device.resource_endpoint
        self._device_model = self._device.resource_metadata.get(u'model', u'unknown')
        self._execute_frequency = int(context.config[u'main'][u'execute_frequency'])
        self._snmp_connection = None
        self._asr_device_metrics = PanoptesMetricsGroupSet()

        try:
            polling_status_metric_name = context.config[u'main'][u'polling_status_metric_name']
        except:
            self._logger.error(u'Polling status metric name not defined for %s' % self._device_host)
            raise PanoptesPollingPluginConfigurationError(u'Polling status metric name not defined for %s' %
                                                          self._device_host)

        self._polling_status = PanoptesPollingStatus(resource=self._device, execute_frequency=self._execute_frequency,
                                                     logger=self._logger, metric_name=polling_status_metric_name)
        self._max_repetitions = _MAX_REPETITIONS  # Todo

        self._logger.info(
            u'Going to poll ASR Device "%s" (model "%s") for device metrics' % (
                self._device_host, self._device_model))

        start_time = time.time()

        device_results = self.get_device_metrics()

        end_time = time.time()

        if device_results:
            self._logger.info(
                u'Done polling ASR Device metrics for device "%s" in %.2f seconds, %s metrics' % (
                    self._device_host, end_time - start_time, len(device_results)))
        else:
            self._logger.warn(u'Error polling device metrics for ASR Device "%s" (model "%s")' %
                              (self._device_host, self._device_model))

        return device_results
