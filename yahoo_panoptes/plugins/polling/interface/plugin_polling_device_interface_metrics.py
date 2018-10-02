import numbers
import time

from cached_property import threaded_cached_property

from yahoo_panoptes.polling.polling_plugin import PanoptesPollingPlugin
from yahoo_panoptes.framework.metrics import PanoptesMetric, PanoptesMetricType, PanoptesMetricsGroup, \
    PanoptesMetricsGroupSet, PanoptesMetricDimension
from yahoo_panoptes.framework.plugins.base_snmp_plugin import PanoptesSNMPBasePlugin
from yahoo_panoptes.framework.utilities.snmp.mibs.dot3StatsTable import *
from yahoo_panoptes.framework.utilities.snmp.mibs.ifTable import *
from yahoo_panoptes.framework.utilities.snmp.mibs.ifXTable import *
from yahoo_panoptes.framework.validators import PanoptesValidators
from yahoo_panoptes.plugins.polling.utilities.polling_status import PanoptesPollingStatus


class _INTERFACE_STATES(object):
    UP, DOWN, UNKNOWN = range(3)


_MAX_REPETITIONS = 25
_MISSING_METRIC_VALUE = -1
_DEFAULT_DIMENSION_VALUE = '<not set>'

_METRIC_TYPE_MAP = {
    'mtu': PanoptesMetricType.GAUGE,
    'admin_state': PanoptesMetricType.GAUGE,
    'oper_state': PanoptesMetricType.GAUGE,
    'oper_admin_state_mismatch': PanoptesMetricType.GAUGE,
    'configured_speed': PanoptesMetricType.GAUGE,
    'type': PanoptesMetricType.COUNTER,
    'errors_in': PanoptesMetricType.COUNTER,
    'errors_out': PanoptesMetricType.COUNTER,
    'discards_in': PanoptesMetricType.COUNTER,
    'discards_out': PanoptesMetricType.COUNTER,
    'bits_in': PanoptesMetricType.COUNTER,
    'bits_out': PanoptesMetricType.COUNTER,
    'unicast_packets_in': PanoptesMetricType.COUNTER,
    'unicast_packets_out': PanoptesMetricType.COUNTER,
    'multicast_packets_in': PanoptesMetricType.COUNTER,
    'multicast_packets_out': PanoptesMetricType.COUNTER,
    'broadcast_packets_in': PanoptesMetricType.COUNTER,
    'broadcast_packets_out': PanoptesMetricType.COUNTER,
    'total_packets_in': PanoptesMetricType.COUNTER,
    'total_packets_out': PanoptesMetricType.COUNTER,
    'errors_frame': PanoptesMetricType.COUNTER,
    'errors_crc': PanoptesMetricType.COUNTER,
    'errors_giants': PanoptesMetricType.COUNTER
}


class PluginPollingDeviceInterfaceMetrics(PanoptesSNMPBasePlugin, PanoptesPollingPlugin):
    def __init__(self):
        super(PluginPollingDeviceInterfaceMetrics, self).__init__()
        self._device_interface_metrics = PanoptesMetricsGroupSet()
        self._polling_status = None
        self._interface_metrics_group = None

        self._dot3stats_map = None
        self._if_table_stats_map = None
        self._ifx_table_stats_map = None

        self._DIMENSION_MAP = {
            'alias': self.get_alias,
            'media_type': self.get_media_type,
            'description': self.get_description,
            'configured_speed': self.get_configured_speed,
            'port_speed': self.get_port_speed,
            'interface_name': self.get_interface_name,
            'parent_interface_name': self.get_parent_interface_name,
            'parent_interface_media_type': self.get_parent_interface_media_type,
            'parent_interface_configured_speed': self.get_parent_interface_configured_speed,
            'parent_interface_port_speed': self.get_parent_interface_port_speed
        }

    # Dimensions
    def get_interface_name(self, interface_index):
        return self.enrichment.get_enrichment_value('self', 'interface', interface_index).get('interface_name')

    def get_alias(self, interface_index):
        return self.enrichment.get_enrichment_value('self', 'interface', interface_index).get('alias')

    def get_description(self, interface_index):
        return self.enrichment.get_enrichment_value('self', 'interface', interface_index).get('description')

    def get_media_type(self, interface_index):
        return self.enrichment.get_enrichment_value('self', 'interface', interface_index).get('media_type')

    def get_port_speed(self, interface_index):
        return self.enrichment.get_enrichment_value('self', 'interface', interface_index).get('port_speed')

    def get_parent_interface_name(self, interface_index):
        return self.enrichment.get_enrichment_value('self', 'interface', interface_index).get('parent_interface_name')

    def get_parent_interface_media_type(self, interface_index):
        return self.enrichment.get_enrichment_value('self', 'interface', interface_index).get(
                'parent_interface_media_type')

    def get_parent_interface_port_speed(self, interface_index):
        return self.enrichment.get_enrichment_value('self', 'interface', interface_index).get(
                'parent_interface_port_speed')

    def get_parent_interface_configured_speed(self, interface_index):
        return self.enrichment.get_enrichment_value('self', 'interface', interface_index).get(
                'parent_interface_configured_speed')

    # Metrics
    def get_bits_in(self, interface_index):
        if (ifHCInOctets + '.' + interface_index) in self._ifx_table_stats_map:
            return int(self._ifx_table_stats_map[ifHCInOctets + '.' + interface_index]) * 8
        else:
            return _MISSING_METRIC_VALUE

    def get_unicast_packets_in(self, interface_index):
        return int(self._ifx_table_stats_map.get(ifHCInUcastPkts + '.' + interface_index, _MISSING_METRIC_VALUE))

    def get_bits_out(self, interface_index):
        if (ifHCOutOctets + '.' + interface_index) in self._ifx_table_stats_map:
            return int(self._ifx_table_stats_map[ifHCOutOctets + '.' + interface_index]) * 8
        else:
            return _MISSING_METRIC_VALUE

    def get_unicast_packets_out(self, interface_index):
        return int(self._ifx_table_stats_map.get(ifHCOutUcastPkts + '.' + interface_index, _MISSING_METRIC_VALUE))

    def get_multicast_packets_in(self, interface_index):
        return int(self._ifx_table_stats_map.get(ifHCInMulticastPkts + '.' + interface_index, _MISSING_METRIC_VALUE))

    def get_multicast_packets_out(self, interface_index):
        return int(self._ifx_table_stats_map.get(ifHCOutMulticastPkts + '.' + interface_index, _MISSING_METRIC_VALUE))

    def get_broadcast_packets_in(self, interface_index):
        return int(self._ifx_table_stats_map.get(ifHCInBroadcastPkts + '.' + interface_index, _MISSING_METRIC_VALUE))

    def get_broadcast_packets_out(self, interface_index):
        return int(self._ifx_table_stats_map.get(ifHCOutBroadcastPkts + '.' + interface_index, _MISSING_METRIC_VALUE))

    def get_total_packets_in(self, interface_index):
        unicast_packets_in = self.get_unicast_packets_in(interface_index)
        multicast_packets_in = self.get_multicast_packets_in(interface_index)
        broadcast_packets_in = self.get_broadcast_packets_in(interface_index)

        if _MISSING_METRIC_VALUE not in [unicast_packets_in, multicast_packets_in, broadcast_packets_in]:
            return unicast_packets_in + multicast_packets_in + broadcast_packets_in
        else:
            return _MISSING_METRIC_VALUE

    def get_total_packets_out(self, interface_index):
        unicast_packets_out = self.get_unicast_packets_out(interface_index)
        multicast_packets_out = self.get_multicast_packets_out(interface_index)
        broadcast_packets_out = self.get_broadcast_packets_out(interface_index)

        if _MISSING_METRIC_VALUE not in [unicast_packets_out, multicast_packets_out, broadcast_packets_out]:
            return unicast_packets_out + multicast_packets_out + broadcast_packets_out
        else:
            return _MISSING_METRIC_VALUE

    def get_admin_state(self, interface_index):
        return int(self._if_table_stats_map.get(ifAdminStatus + '.' + interface_index, _MISSING_METRIC_VALUE))

    def get_oper_state(self, interface_index):
        return int(self._if_table_stats_map.get(ifOperStatus + '.' + interface_index, _MISSING_METRIC_VALUE))

    def get_oper_admin_state_mismatch(self, interface_index):
        return 0 if self.get_oper_state(interface_index) == self.get_admin_state(interface_index) else 1

    def get_discards_in(self, interface_index):
        return int(self._if_table_stats_map.get(ifInDiscards + '.' + interface_index, _MISSING_METRIC_VALUE))

    def get_errors_in(self, interface_index):
        return int(self._if_table_stats_map.get(ifInErrors + '.' + interface_index, _MISSING_METRIC_VALUE))

    def get_discards_out(self, interface_index):
        return int(self._if_table_stats_map.get(ifOutDiscards + '.' + interface_index, _MISSING_METRIC_VALUE))

    def get_errors_out(self, interface_index):
        return int(self._if_table_stats_map.get(ifOutErrors + '.' + interface_index, _MISSING_METRIC_VALUE))

    def get_mtu(self, interface_index):
        return int(self._if_table_stats_map.get(ifMtu + '.' + interface_index, _MISSING_METRIC_VALUE))

    def get_if_high_speed(self, interface_index):
        # n.b. adjusted value means I can't use 'get(..., _MISSING_METRIC_VALUE)' idiom
        # Mbps by definition
        return int(self._ifx_table_stats_map.get(ifHighSpeed + '.' + interface_index, _MISSING_METRIC_VALUE))

    def get_if_speed(self, interface_index):
        return int(self._if_table_stats_map.get(ifSpeed + '.' + interface_index, _MISSING_METRIC_VALUE))

    def get_configured_speed(self, index):
        return self.enrichment.get_enrichment_value('self', 'interface', index).get('configured_speed',
                                                                                    _MISSING_METRIC_VALUE)

    def get_errors_frame(self, interface_index):
        return int(self._dot3stats_map.get(dot3StatsAlignmentErrors + '.' + interface_index, _MISSING_METRIC_VALUE))

    def get_errors_crc(self, interface_index):
        return int(self._dot3stats_map.get(dot3StatsFCSErrors + '.' + interface_index, _MISSING_METRIC_VALUE))

    def get_errors_giants(self, interface_index):
        return int(self._dot3stats_map.get(dot3StatsFrameTooLongs + '.' + interface_index, _MISSING_METRIC_VALUE))

    @threaded_cached_property
    def interface_indices(self):
        result = set()
        for oid in self._ifx_table_stats_map:
            result.add(oid.split('.')[-1])
        return result

    def _getif_table_stats(self):
        result = dict()
        try:
            for oid in self._if_table_stats_map:
                index = oid.split('.')[-1]
                result[index] = dict()
                result[index]['admin_state'] = self.get_admin_state(index)
                result[index]['oper_state'] = self.get_oper_state(index)
                result[index]['oper_admin_state_mismatch'] = self.get_oper_admin_state_mismatch(index)
                result[index]['errors_in'] = self.get_errors_in(index)
                result[index]['errors_out'] = self.get_errors_out(index)
                result[index]['discards_in'] = self.get_discards_in(index)
                result[index]['discards_out'] = self.get_discards_out(index)
                result[index]['mtu'] = self.get_mtu(index)
            return result
        except Exception as e:
            self._polling_status.handle_exception('interface', e)

    def _getifx_table_stats(self):
        result = dict()
        try:
            for oid in self._ifx_table_stats_map:
                index = oid.split('.')[-1]
                result[index] = dict()
                result[index]['bits_in'] = self.get_bits_in(index)
                result[index]['bits_out'] = self.get_bits_out(index)
                result[index]['unicast_packets_in'] = self.get_unicast_packets_in(index)
                result[index]['unicast_packets_out'] = self.get_unicast_packets_out(index)
                result[index]['multicast_packets_in'] = self.get_multicast_packets_in(index)
                result[index]['multicast_packets_out'] = self.get_multicast_packets_out(index)
                result[index]['broadcast_packets_in'] = self.get_broadcast_packets_in(index)
                result[index]['broadcast_packets_out'] = self.get_broadcast_packets_out(index)
                result[index]['total_packets_in'] = self.get_total_packets_in(index)
                result[index]['total_packets_out'] = self.get_total_packets_out(index)
                result[index]['configured_speed'] = self.get_configured_speed(index)
            return result
        except Exception as e:
            self._polling_status.handle_exception('interface', e)

    def _getdot3stats(self):
        result = dict()
        try:
            # Use ifx_table_stats_map b/c dot3stats is not defined for every machine
            for oid in self._ifx_table_stats_map:
                index = oid.split('.')[-1]
                result[index] = dict()
                result[index]['errors_frame'] = self.get_errors_frame(index)
                result[index]['errors_crc'] = self.get_errors_crc(index)
                result[index]['errors_giants'] = self.get_errors_giants(index)
            return result
        except Exception as e:
            self._polling_status.handle_exception('interface', e)

    @staticmethod
    def _get_state_val(state):
        s = int(state)
        if s == 2:
            return _INTERFACE_STATES.DOWN
        elif s == 1:
            return _INTERFACE_STATES.UP
        else:
            return _INTERFACE_STATES.UNKNOWN

    def _build_ifx_table_stats_map(self):
        """Maps child oids of ifXTable to their respective values as PanoptesSNMPVariables"""
        ifx_table_stats = list()
        for metric in ifx_table_oids:
            for varbind in self._snmp_connection.bulk_walk(metric,
                                                           max_repetitions=self.snmp_configuration.max_repetitions):
                ifx_table_stats.append(varbind)

        self._ifx_table_stats_map = dict()
        for ent in ifx_table_stats:
            self._ifx_table_stats_map[ent.oid + '.' + ent.index] = ent.value

    def _build_if_table_stats_map(self):
        """Maps child oids of ifTable to their respective values as PanoptesSNMPVariables"""
        if_table_stats = list()
        for metric in if_table_oids:
            for varbind in self._snmp_connection.bulk_walk(metric,
                                                           max_repetitions=self.snmp_configuration.max_repetitions):
                if_table_stats.append(varbind)

        self._if_table_stats_map = dict()
        for ent in if_table_stats:
            self._if_table_stats_map[ent.oid + '.' + ent.index] = ent.value

    def _build_dot3stats_map(self):
        """Maps child oids of dot3statsTable to their respective values as PanoptesSNMPVariables"""
        dot3stats = list()
        for metric in dots3stats_table_oids:
            for varbind in self._snmp_connection.bulk_walk(metric,
                                                           max_repetitions=self.snmp_configuration.max_repetitions):
                dot3stats.append(varbind)

        self._dot3stats_map = dict()
        for ent in dot3stats:
            self._dot3stats_map[ent.oid + '.' + ent.index] = ent.value

    def _smart_add_dimension(self, method, dimension_name, index):
        dimension = method(index)
        if dimension is not None and PanoptesValidators.valid_nonempty_string(str(dimension)):
            self._interface_metrics_group.add_dimension(PanoptesMetricDimension(dimension_name, str(dimension)))
        else:
            self._interface_metrics_group.add_dimension(PanoptesMetricDimension(dimension_name,
                                                                                _DEFAULT_DIMENSION_VALUE))

    def get_results(self):
        self._polling_status = PanoptesPollingStatus(resource=self.resource,
                                                     execute_frequency=self.execute_frequency,
                                                     logger=self.logger,
                                                     metric_name='interface_polling_status')

        interface_metrics = dict()

        try:
            start_time = time.time()

            self._build_dot3stats_map()
            self._build_if_table_stats_map()
            self._build_ifx_table_stats_map()

            end_time = time.time()

            self._logger.info('SNMP calls for device %s completed in %.2f seconds' % (
                self.host, end_time - start_time))

            interface_metrics.update(self._getdot3stats())
            if_interface_metrics = self._getif_table_stats()
            ifx_interface_metrics = self._getifx_table_stats()

            # https://github.com/PyCQA/pylint/issues/1694
            for i in self.interface_indices:  # pylint: disable=E1133
                if i not in interface_metrics:
                    interface_metrics[i] = dict()
                interface_metrics[i].update(ifx_interface_metrics[i])
                interface_metrics[i].update(if_interface_metrics[i])

            for interface_index in interface_metrics.keys():
                self._interface_metrics_group = PanoptesMetricsGroup(self.resource, 'interface',
                                                                     self.execute_frequency)
                interface = interface_metrics[interface_index]

                for dimension_name, dimension_method in self._DIMENSION_MAP.items():
                    self._smart_add_dimension(method=dimension_method,
                                              dimension_name=dimension_name,
                                              index=interface_index
                                              )

                for metric in interface.keys():
                    metric_type = _METRIC_TYPE_MAP[metric]

                    if not isinstance(interface[metric], numbers.Number):
                        self._interface_metrics_group.add_metric(PanoptesMetric(str(metric),
                                                                                _MISSING_METRIC_VALUE,
                                                                                metric_type))
                    else:
                        self._interface_metrics_group.add_metric(PanoptesMetric(str(metric),
                                                                                interface[metric],
                                                                                metric_type))

                self._device_interface_metrics.add(self._interface_metrics_group)

            self._polling_status.handle_success('interface')
            self._logger.debug('Found interface metrics: "%s" for device "%s"' % (
                interface_metrics, self.host))
        except Exception as e:
            self._polling_status.handle_exception('interface', e)
        finally:
            self._device_interface_metrics.add(self._polling_status.device_status_metrics_group)
            return self._device_interface_metrics
