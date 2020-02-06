"""
This module implements a Panoptes Plugin that can poll Arista EOS devices for Device Metrics
"""
import time
from cached_property import threaded_cached_property

from yahoo_panoptes.framework.metrics import PanoptesMetricsGroupSet, PanoptesMetricsGroup, PanoptesMetric, \
    PanoptesMetricType, PanoptesMetricDimension
from yahoo_panoptes.polling.polling_plugin import PanoptesPollingPlugin
from yahoo_panoptes.plugins.polling.utilities.polling_status import PanoptesPollingStatus, DEVICE_METRICS_STATES
from yahoo_panoptes.framework.metrics import PanoptesMetricsNullException
from yahoo_panoptes.plugins.helpers.snmp_connections import PanoptesSNMPConnectionFactory
from yahoo_panoptes.framework.utilities.helpers import convert_celsius_to_fahrenheit, is_python_2

hrStorageVirtualMemory = u'.1.3.6.1.2.1.25.2.1.3'
hrStorageFlashMemory = u'.1.3.6.1.2.1.25.2.1.9'

STORAGE_TYPE_REVERSE_MAP = {hrStorageVirtualMemory: u'virtual',
                            hrStorageFlashMemory: u'flash'}

HOST_RESOURCES_MIB_PREFIX = u'.1.3.6.1.2.1.25.2.3.1'
hrStorageType = HOST_RESOURCES_MIB_PREFIX + u'.2'
hrStorageDescr = HOST_RESOURCES_MIB_PREFIX + u'.3'
hrStorageAllocationUnits = HOST_RESOURCES_MIB_PREFIX + u'.4'
hrStorageSize = HOST_RESOURCES_MIB_PREFIX + u'.5'
hrStorageUsed = HOST_RESOURCES_MIB_PREFIX + u'.6'

hrProcessorLoad = u'.1.3.6.1.2.1.25.3.3.1.2'
hrDeviceDescription = u'.1.3.6.1.2.1.25.3.2.1.3'

ENTITY_MIB_PREFIX = u'.1.3.6.1.2.1.47'
entPhysicalEntry = ENTITY_MIB_PREFIX + u'.1.1.1.1'
entPhysicalDescrPrefix = ENTITY_MIB_PREFIX + u'.1.1.1.1.2'
entPhysicalClassPrefix = ENTITY_MIB_PREFIX + u'.1.1.1.1.5'
entPhysicalNamePrefix = ENTITY_MIB_PREFIX + u'.1.1.1.1.7'  # all blank

ENT_PHY_SENSOR_PREFIX = u'.1.3.6.1.2.1.99.1.1.1'
entPhySensorScale = ENT_PHY_SENSOR_PREFIX + u'.2'
entPhySensorValue = ENT_PHY_SENSOR_PREFIX + u'.4'

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

_MAX_REPETITIONS = 25
_INPUT_CURRENT_SENSOR_OFFSET = 102


class PluginPollingAristaDeviceMetrics(PanoptesPollingPlugin):
    def __init__(self):
        self._plugin_context = None
        self._logger = None
        self._device = None
        self._device_host = None
        self._device_model = None
        self._execute_frequency = None
        self._snmp_connection = None
        self._arista_device_metrics = PanoptesMetricsGroupSet()
        self._polling_status = None
        self._max_repetitions = None

        self._cpu_metrics = None
        self._memory_metrics = None
        self._temp_metrics = None
        self._power_metrics = None
        self._fan_metrics = None
        self._storage_metrics = None
        self._device_metrics = None

        super(PluginPollingAristaDeviceMetrics, self).__init__()

    @threaded_cached_property
    def device_descriptions(self):
        return self._snmp_connection.bulk_walk(oid=hrDeviceDescription, non_repeaters=0,
                                               max_repetitions=_MAX_REPETITIONS)

    @threaded_cached_property
    def _device_descriptions_map(self):
        device_descriptions_map = dict()
        for ent in self.device_descriptions:  # pylint: disable=E1133
            value = ent.value
            if not is_python_2() and isinstance(value, bytes):
                value = value.decode(u'ascii', u'ignore')
            device_descriptions_map[ent.oid + u'.' + ent.index] = value
        return device_descriptions_map

    def _get_cpu_name(self, temp_id):
        core_num = self._device_descriptions_map[hrDeviceDescription + u'.' + str(temp_id)]  # pylint: disable=E1136
        return self._device_descriptions_map[
                   hrDeviceDescription + u'.' + u'1'] + u'/' + core_num  # pylint: disable=E1136

    # TODO is mutable type allowed?
    def _get_entity_indices(self, ent_physical_class, ent_strings):
        ent_indices = []
        # https://github.com/PyCQA/pylint/issues/1694
        for ent in self.entities:  # pylint: disable=E1133
            value = ent.value
            if not is_python_2() and isinstance(value, bytes):
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
                                               max_repetitions=_MAX_REPETITIONS)

    @threaded_cached_property
    def host_resources(self):
        return self._snmp_connection.bulk_walk(oid=HOST_RESOURCES_MIB_PREFIX, non_repeaters=0,
                                               max_repetitions=_MAX_REPETITIONS)

    @threaded_cached_property
    def host_resources_map(self):
        host_resources_map = dict()
        for ent in self.host_resources:  # pylint: disable=E1133
            value = ent.value
            if not is_python_2() and isinstance(value, bytes):
                value = value.decode(u'ascii', u'ignore')
            host_resources_map[ent.oid + u'.' + ent.index] = value
        return host_resources_map

    @threaded_cached_property
    def entity_physical_entries_map(self):
        """Maps child oids of entPhysicalEntry to their respective values as PanoptesSNMPVariables"""
        ent_physical_entries_map = {}
        # https://github.com/PyCQA/pylint/issues/1694
        for ent in self.entities:  # pylint: disable=E1133
            value = ent.value
            if not is_python_2() and isinstance(value, bytes):
                value = value.decode(u'ascii', u'ignore')
            ent_physical_entries_map[ent.oid + u'.' + ent.index] = value
        return ent_physical_entries_map

    @threaded_cached_property
    def sensor_entities(self):
        return self._snmp_connection.bulk_walk(oid=ENT_PHY_SENSOR_PREFIX, non_repeaters=0,
                                               max_repetitions=25)

    @threaded_cached_property
    def sensor_entity_map(self):
        """Maps child oids of ENT_PHY_SENSOR_PREFIX to their respective values as PanoptesSNMPVariables"""
        sensor_ent_map = {}
        # https://github.com/PyCQA/pylint/issues/1694
        for ent in self.sensor_entities:  # pylint: disable=E1133
            sensor_ent_map[ent.oid + u'.' + ent.index] = ent.value
        return sensor_ent_map

    def _get_sensor_details(self, index):
        try:
            details = dict()

            entSensorValueIndex = entPhySensorValue + u'.' + index
            # https://github.com/PyCQA/pylint/issues/1694
            details[u'sensor_value'] = int(self.sensor_entity_map[entSensorValueIndex])  # pylint: disable=E1136
            details[u'sensor_scale'] = int(self.sensor_entity_map[
                                               entPhySensorScale + u'.' + index])  # pylint: disable=E1136

            return details
        except Exception as e:
            raise e

    def _convert_scaled_celsius_value_to_units(self, x, scale):  # TODO introducing bug when I make static
        """Convert the provided value in scale-units Celsius to Celsius. N.B. The indices for u'peta' and u'exa'
        appear to be flip-flopped"""
        if scale == 14:
            scale = 15
        elif scale == 15:
            scale = 14
        return x * (10 ** (scale - 9))

    def _get_temperature_metrics(self):
        try:
            self._temp_metrics = dict()
            temperature_entity_indices = self._get_entity_indices(ent_physical_class=u'sensor',
                                                                  ent_strings=[u'temp', u'Temp'])
            for index in temperature_entity_indices:
                index = index.split(u'.')[-1]
                # TODO should be name, even though blank? -- Can't be; name must be non-empty
                self._temp_metrics[index] = {
                    u'sensor':
                        self.entity_physical_entries_map[
                            entPhysicalDescrPrefix + u'.' + index]  # pylint: disable=E1136
                }
                self._temp_metrics[index][u'sensor_details'] = self._get_sensor_details(index)
                # reported in deci-degrees C
                celsius_value = self._temp_metrics[index][u'sensor_details'][u'sensor_value'] / 10.0
                scale = self._temp_metrics[index][u'sensor_details'][u'sensor_scale']
                celsius_value_in_units = self._convert_scaled_celsius_value_to_units(celsius_value, scale)
                self._temp_metrics[index][u'temp_f'] = convert_celsius_to_fahrenheit(celsius_value_in_units)

        except Exception as e:
            raise e

    def _power_is_on(self, index):
        # TODO better to base off of current AND voltage difference, or current alone will suffice?
        return True if int(self.sensor_entity_map[
                           entPhySensorValue + u'.' + str((int(  # pylint: disable=E1136
                               index) + _INPUT_CURRENT_SENSOR_OFFSET))]) > 0 else False  # pylint: disable=E1136

    def _get_power_metrics(self):
        try:
            power_metrics = dict()
            power_metrics[u'power_module_map'] = dict()

            # won't work here alone b/c on/off not given
            power_entity_indices = self._get_entity_indices(ent_physical_class=u'powerSupply',
                                                            ent_strings=[u'PowerSupply'])
            power_metrics[u'power_units_total'] = len(power_entity_indices)
            for index in power_entity_indices:
                index = index.split(u'.')[-1]
                entity_name_index = entPhysicalDescrPrefix + u'.' + index  # Use Descr b/c name is empty
                entity_name = self.entity_physical_entries_map[entity_name_index]  # pylint: disable=E1136
                power_on = self._power_is_on(index)
                power_metrics[u'power_module_map'][index] = {
                    u'entity_name': entity_name,
                    u'power_on': power_on
                }
                # Todo Collect current as well? -- Not yet
                # TODO is this a better pattern b/c if an Exception is raised, the property won't have yet been updated?
            self._power_metrics = power_metrics
        except Exception as e:
            raise e

    def _fan_is_ok(self, index):
        return 1 if int(self.sensor_entity_map[entPhySensorValue + u'.' + index]) > 0 else 0  # pylint: disable=E1136

    # Maximum nominal fan speed is 27000 RPM
    def _get_fan_metrics(self):
        try:
            fan_metrics = dict()
            fan_entity_indices = self._get_entity_indices(ent_physical_class=u'sensor',
                                                          ent_strings=[u'Fan 1 Sensor 1'])
            fan_metrics[u'fans_total'] = len(fan_entity_indices)
            fans_ok = 0
            for index in fan_entity_indices:
                index = index.split(u'.')[-1]
                fans_ok += self._fan_is_ok(index)
            fan_metrics[u'fans_ok'] = fans_ok
            self._fan_metrics = fan_metrics
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
        except Exception as e:
            self._polling_status.handle_exception(u'environment', e)

        try:
            self._get_fan_metrics()
            self._logger.debug(
                u'Found Fan metrics "%s" for %s: %s' % (self._fan_metrics, self._polling_status.device_type,
                                                        self._device_host))
        except Exception as e:
            self._polling_status.handle_exception(u'environment', e)

        try:
            if self._temp_metrics:
                for index in list(self._temp_metrics.keys()):
                    environment_metrics_group = PanoptesMetricsGroup(self._device, u'environment',
                                                                     self._execute_frequency)
                    environment_metrics_group.add_dimension(
                        PanoptesMetricDimension(u'sensor',
                                                self._temp_metrics[index][u'sensor']))
                    environment_metrics_group.add_metric(PanoptesMetric(u'temperature_fahrenheit',
                                                                        self._temp_metrics[index][u'temp_f'],
                                                                        PanoptesMetricType.GAUGE))
                    self._arista_device_metrics.add(environment_metrics_group)
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
                self._arista_device_metrics.add(environment_metrics_group)
                self._polling_status.handle_success(u'environment')
        except Exception as e:
            self._polling_status.handle_exception(u'environment', e)

        try:
            if self._fan_metrics:
                environment_metrics_group = PanoptesMetricsGroup(self._device, u'environment',
                                                                 self._execute_frequency)
                environment_metrics_group.add_metric(PanoptesMetric(u'fans_total',
                                                                    self._fan_metrics[u'fans_total'],
                                                                    PanoptesMetricType.GAUGE))
                environment_metrics_group.add_metric(PanoptesMetric(u'fans_ok',
                                                                    self._fan_metrics[u'fans_ok'],
                                                                    PanoptesMetricType.GAUGE))
                self._arista_device_metrics.add(environment_metrics_group)
                self._polling_status.handle_success(u'environment')
        except Exception as e:
            self._polling_status.handle_exception(u'environment', e)

    def _get_memory_metrics(self):
        self._memory_metrics = dict()
        self._memory_metrics[u'dram'] = dict()
        try:
            allocation_units = int(self.host_resources_map[hrStorageAllocationUnits + u'.1'])  # pylint: disable=E1136
            memory_used = (int(self.host_resources_map[
                                   hrStorageUsed + u'.1']) - int(
                self.host_resources_map[hrStorageUsed + u'.3'])) * allocation_units  # total - cached
            self._memory_metrics[u'dram'][u'memory_used'] = memory_used
            memory_total = \
                int(self.host_resources_map[hrStorageSize + u'.1']) * allocation_units  # pylint: disable=E1136
            self._memory_metrics[u'dram'][u'memory_total'] = memory_total
        except Exception as e:
            self._polling_status.handle_exception(u'memory', e)
            self._memory_metrics.pop(u'dram')

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
                    self._arista_device_metrics.add(memory_metrics_group)
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
            cpus = self._snmp_connection.bulk_walk(oid=hrProcessorLoad, non_repeaters=0,
                                                   max_repetitions=_MAX_REPETITIONS)
            if len(cpus) == 0:
                raise PanoptesMetricsNullException

            for cpu in cpus:
                # The last int for each cpu is a temporary index we will append to hrDeviceDescription to get the name
                temp_id = int(cpu.index.rsplit(u'.', 1)[-1])  # last object
                if temp_id != 1:  # only include individual core info
                    self._cpu_metrics[u'ctrl'][temp_id] = dict()
                    self._cpu_metrics[u'ctrl'][temp_id][u'cpu_util'] = int(cpu.value)
                    self._cpu_metrics[u'ctrl'][temp_id][u'cpu_name'] = self._get_cpu_name(temp_id)

        except Exception as e:
            self._polling_status.handle_exception(u'cpu', e)
            self._cpu_metrics.pop(u'ctrl')

        try:
            if len(self._cpu_metrics) > 0:
                for cpu_type in self._cpu_metrics:
                    for cpu_id in list(self._cpu_metrics[cpu_type].keys()):
                        cpu_metrics_group = PanoptesMetricsGroup(self._device, u'cpu', self._execute_frequency)
                        cpu_metrics_group.add_dimension(PanoptesMetricDimension(u'cpu_type', cpu_type))
                        cpu_metrics_group.add_dimension(PanoptesMetricDimension(u'cpu_no', u'1.' + str(cpu_id)))
                        cpu_metrics_group.add_dimension(PanoptesMetricDimension(u'cpu_name',
                                                                                self._cpu_metrics[cpu_type][cpu_id]
                                                                                [u'cpu_name']))
                        cpu_metrics_group.add_metric(PanoptesMetric(u'cpu_utilization',
                                                                    self._cpu_metrics[cpu_type][cpu_id][u'cpu_util'],
                                                                    PanoptesMetricType.GAUGE))
                        self._arista_device_metrics.add(cpu_metrics_group)
                self._polling_status.handle_success(u'cpu')
                self._logger.debug(
                    u'Found CPU metrics "%s" for %s: %s' % (self._cpu_metrics, self._polling_status.device_type,
                                                            self._device_host))
        except Exception as e:
            self._polling_status.handle_exception(u'cpu', e)

    def _get_host_resource_indices(self, oid_filter=u'', host_resource_strings=list()):
        host_resource_indices = []
        # https://github.com/PyCQA/pylint/issues/1694
        for key, value in list(self.host_resources_map.items()):  # pylint: disable=E1101
            if oid_filter in key:
                for s in host_resource_strings:
                    if s in value:
                        host_resource_indices.append(key.split(u'.')[-1])
        return host_resource_indices

    def _get_storage_metrics(self):
        self._storage_metrics = dict()

        try:
            host_resource_storage_indices = \
                self._get_host_resource_indices(oid_filter=hrStorageType,
                                                host_resource_strings=[hrStorageFlashMemory, hrStorageVirtualMemory])

            for index in host_resource_storage_indices:
                storage_descriptor = self.host_resources_map[hrStorageDescr + u'.' + index]  # pylint: disable=E1136
                self._storage_metrics[storage_descriptor] = dict()

                allocation_units = int(self.host_resources_map[
                                           hrStorageAllocationUnits + u'.' + index])  # pylint: disable=E1136
                self._storage_metrics[storage_descriptor][u'storage_used'] = \
                    int(self.host_resources_map[
                            hrStorageUsed + u'.' + index]) * allocation_units  # pylint: disable=E1136
                self._storage_metrics[storage_descriptor][u'storage_total'] = \
                    int(self.host_resources_map[
                            hrStorageSize + u'.' + index]) * allocation_units  # pylint: disable=E1136
                self._storage_metrics[storage_descriptor][u'storage_type'] = \
                    STORAGE_TYPE_REVERSE_MAP[self.host_resources_map[
                        hrStorageType + u'.' + index]]  # pylint: disable=E1136
        except Exception as e:
            self._polling_status.handle_exception(u'storage', e)
            # todo Do we need to pop the stats from self._storage_metrics?

        try:
            if len(self._storage_metrics) > 0:
                for storage_entity in self._storage_metrics:
                    storage_metrics_group = PanoptesMetricsGroup(self._device, u'storage', self._execute_frequency)
                    storage_metrics_group.add_dimension(PanoptesMetricDimension(u'storage_type',
                                                                                self._storage_metrics[storage_entity]
                                                                                [u'storage_type']))
                    storage_metrics_group.add_dimension(PanoptesMetricDimension(u'storage_entity',
                                                                                storage_entity))
                    storage_metrics_group.add_metric(PanoptesMetric(u'storage_used',
                                                                    self._storage_metrics[storage_entity]
                                                                    [u'storage_used'],
                                                                    PanoptesMetricType.GAUGE))
                    storage_metrics_group.add_metric(PanoptesMetric(u'storage_total',
                                                                    self._storage_metrics[storage_entity]
                                                                    [u'storage_total'],
                                                                    PanoptesMetricType.GAUGE))
                    self._arista_device_metrics.add(storage_metrics_group)
                self._polling_status.handle_success(u'storage')
                self._logger.debug(
                    u'Found Storage metrics "%s" for %s: %s' % (self._storage_metrics, self._polling_status.device_type,
                                                                self._device_host))
        except Exception as e:
            self._polling_status.handle_exception(u'storage', e)

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
                self._arista_device_metrics.add(self._polling_status.device_status_metrics_group)
                return self._arista_device_metrics

        self._get_system_cpu_metrics()
        self._get_memory_metrics()
        self._get_environment_metrics()
        self._get_storage_metrics()

        end_time = time.time()

        self._logger.info(u'SNMP calls for Arista %s completed in %.2f seconds' %
                          (self._device_host, end_time - start_time))

        self._arista_device_metrics.add(self._polling_status.device_status_metrics_group)
        return self._arista_device_metrics

    def run(self, context):
        self._plugin_context = context
        self._logger = context.logger
        self._device = context.data
        self._device_host = self._device.resource_endpoint
        self._device_model = self._device.resource_metadata.get(u'model', u'unknown')
        self._execute_frequency = int(context.config[u'main'][u'execute_frequency'])
        self._snmp_connection = None
        self._arista_device_metrics = PanoptesMetricsGroupSet()
        self._polling_status = PanoptesPollingStatus(resource=self._device, execute_frequency=self._execute_frequency,
                                                     logger=self._logger)
        self._max_repetitions = _MAX_REPETITIONS

        self._logger.info(
            u'Going to poll Arista device "%s" (model "%s") for device metrics' % (
                self._device_host, self._device_model))

        start_time = time.time()

        device_results = self.get_device_metrics()

        end_time = time.time()

        if device_results:
            self._logger.info(
                u'Done polling Arista Device metrics for device "%s" in %.2f seconds, %s metrics' % (
                    self._device_host, end_time - start_time, len(device_results)))
        else:
            self._logger.warn(u'Error polling device metrics for Arista device %s' % self._device_host)

        return device_results
