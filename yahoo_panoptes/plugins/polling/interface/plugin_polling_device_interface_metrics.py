"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
from builtins import range
from builtins import object
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
    UP, DOWN, UNKNOWN = list(range(3))


_MAX_REPETITIONS = 25
_MISSING_METRIC_VALUE = -1
_DEFAULT_DIMENSION_VALUE = u'<not set>'

_METRIC_TYPE_MAP = {
    u'mtu': PanoptesMetricType.GAUGE,
    u'admin_state': PanoptesMetricType.GAUGE,
    u'oper_state': PanoptesMetricType.GAUGE,
    u'oper_admin_state_mismatch': PanoptesMetricType.GAUGE,
    u'configured_speed': PanoptesMetricType.GAUGE,
    u'type': PanoptesMetricType.COUNTER,
    u'errors_in': PanoptesMetricType.COUNTER,
    u'errors_out': PanoptesMetricType.COUNTER,
    u'discards_in': PanoptesMetricType.COUNTER,
    u'discards_out': PanoptesMetricType.COUNTER,
    u'bits_in': PanoptesMetricType.COUNTER,
    u'bits_out': PanoptesMetricType.COUNTER,
    u'unicast_packets_in': PanoptesMetricType.COUNTER,
    u'unicast_packets_out': PanoptesMetricType.COUNTER,
    u'multicast_packets_in': PanoptesMetricType.COUNTER,
    u'multicast_packets_out': PanoptesMetricType.COUNTER,
    u'broadcast_packets_in': PanoptesMetricType.COUNTER,
    u'broadcast_packets_out': PanoptesMetricType.COUNTER,
    u'total_packets_in': PanoptesMetricType.COUNTER,
    u'total_packets_out': PanoptesMetricType.COUNTER,
    u'errors_frame': PanoptesMetricType.COUNTER,
    u'errors_crc': PanoptesMetricType.COUNTER,
    u'errors_giants': PanoptesMetricType.COUNTER
}


class PluginPollingDeviceInterfaceMetrics(PanoptesSNMPBasePlugin, PanoptesPollingPlugin):
    def __init__(self):
        """
        Call the interface table.

        Args:
            self: (todo): write your description
        """
        super(PluginPollingDeviceInterfaceMetrics, self).__init__()
        self._device_interface_metrics = PanoptesMetricsGroupSet()
        self._polling_status = None
        self._interface_metrics_group = None

        self._dot3stats_map = None
        self._if_table_stats_map = None
        self._ifx_table_stats_map = None

        self._DIMENSION_MAP = {
            u'alias': self.get_alias,
            u'media_type': self.get_media_type,
            u'description': self.get_description,
            u'configured_speed': self.get_configured_speed,
            u'port_speed': self.get_port_speed,
            u'interface_name': self.get_interface_name,
            u'parent_interface_name': self.get_parent_interface_name,
            u'parent_interface_media_type': self.get_parent_interface_media_type,
            u'parent_interface_configured_speed': self.get_parent_interface_configured_speed,
            u'parent_interface_port_speed': self.get_parent_interface_port_speed
        }

    # Dimensions
    def get_interface_name(self, interface_index):
        """
        Get the interface name

        Args:
            self: (todo): write your description
            interface_index: (str): write your description
        """
        return self.enrichment.get_enrichment_value(u'self', u'interface', interface_index).get(u'interface_name')

    def get_alias(self, interface_index):
        """
        Return the alias of the alias.

        Args:
            self: (todo): write your description
            interface_index: (str): write your description
        """
        return self.enrichment.get_enrichment_value(u'self', u'interface', interface_index) \
            .get(u'alias') \
            .encode(u'ascii', u'ignore') \
            .decode(u'ascii')

    def get_description(self, interface_index):
        """
        Get description.

        Args:
            self: (todo): write your description
            interface_index: (str): write your description
        """
        return self.enrichment.get_enrichment_value(u'self', u'interface', interface_index) \
            .get(u'description') \
            .encode(u'ascii', u'ignore') \
            .decode(u'ascii')

    def get_media_type(self, interface_index):
        """
        Return the media type for an interface.

        Args:
            self: (todo): write your description
            interface_index: (str): write your description
        """
        return self.enrichment.get_enrichment_value(u'self', u'interface', interface_index).get(u'media_type')

    def get_port_speed(self, interface_index):
        """
        Returns the port speed

        Args:
            self: (todo): write your description
            interface_index: (int): write your description
        """
        return self.enrichment.get_enrichment_value(u'self', u'interface', interface_index).get(u'port_speed')

    def get_parent_interface_name(self, interface_index):
        """
        The parent name.

        Args:
            self: (todo): write your description
            interface_index: (str): write your description
        """
        return self.enrichment.get_enrichment_value(u'self', u'interface', interface_index).get(
                u'parent_interface_name')

    def get_parent_interface_media_type(self, interface_index):
        """
        Return the parent interface type.

        Args:
            self: (todo): write your description
            interface_index: (str): write your description
        """
        return self.enrichment.get_enrichment_value(u'self', u'interface', interface_index).get(
                u'parent_interface_media_type')

    def get_parent_interface_port_speed(self, interface_index):
        """
        Return the parent speed of the interface.

        Args:
            self: (todo): write your description
            interface_index: (str): write your description
        """
        return self.enrichment.get_enrichment_value(u'self', u'interface', interface_index).get(
                u'parent_interface_port_speed')

    def get_parent_interface_configured_speed(self, interface_index):
        """
        Return the parent parent interface configuration.

        Args:
            self: (todo): write your description
            interface_index: (int): write your description
        """
        return self.enrichment.get_enrichment_value(u'self', u'interface', interface_index).get(
                u'parent_interface_configured_speed')

    # Metrics
    def get_bits_in(self, interface_index):
        """
        Return the number of an interface.

        Args:
            self: (todo): write your description
            interface_index: (str): write your description
        """
        if (ifHCInOctets + u'.' + interface_index) in self._ifx_table_stats_map:
            return int(self._ifx_table_stats_map[ifHCInOctets + u'.' + interface_index]) * 8
        else:
            return _MISSING_METRIC_VALUE

    def get_unicast_packets_in(self, interface_index):
        """
        : return the interface interface_index.

        Args:
            self: (todo): write your description
            interface_index: (int): write your description
        """
        return int(self._ifx_table_stats_map.get(ifHCInUcastPkts + u'.' + interface_index, _MISSING_METRIC_VALUE))

    def get_bits_out(self, interface_index):
        """
        Return the number of bits for the interface.

        Args:
            self: (todo): write your description
            interface_index: (int): write your description
        """
        if (ifHCOutOctets + u'.' + interface_index) in self._ifx_table_stats_map:
            return int(self._ifx_table_stats_map[ifHCOutOctets + u'.' + interface_index]) * 8
        else:
            return _MISSING_METRIC_VALUE

    def get_unicast_packets_out(self, interface_index):
        """
        Gets the unicode_stats_stats.

        Args:
            self: (todo): write your description
            interface_index: (int): write your description
        """
        return int(self._ifx_table_stats_map.get(ifHCOutUcastPkts + u'.' + interface_index, _MISSING_METRIC_VALUE))

    def get_multicast_packets_in(self, interface_index):
        """
        Gets the multicast packet.

        Args:
            self: (todo): write your description
            interface_index: (int): write your description
        """
        return int(self._ifx_table_stats_map.get(ifHCInMulticastPkts + u'.' + interface_index, _MISSING_METRIC_VALUE))

    def get_multicast_packets_out(self, interface_index):
        """
        Gets the packet packet_index.

        Args:
            self: (todo): write your description
            interface_index: (int): write your description
        """
        return int(self._ifx_table_stats_map.get(ifHCOutMulticastPkts + u'.' + interface_index, _MISSING_METRIC_VALUE))

    def get_broadcast_packets_in(self, interface_index):
        """
        Gets the number of interface.

        Args:
            self: (todo): write your description
            interface_index: (int): write your description
        """
        return int(self._ifx_table_stats_map.get(ifHCInBroadcastPkts + u'.' + interface_index, _MISSING_METRIC_VALUE))

    def get_broadcast_packets_out(self, interface_index):
        """
        Returns the number ofcast from interface.

        Args:
            self: (todo): write your description
            interface_index: (int): write your description
        """
        return int(self._ifx_table_stats_map.get(ifHCOutBroadcastPkts + u'.' + interface_index, _MISSING_METRIC_VALUE))

    def get_total_packets_in(self, interface_index):
        """
        Get the total number of packets in an interface.

        Args:
            self: (todo): write your description
            interface_index: (int): write your description
        """
        unicast_packets_in = self.get_unicast_packets_in(interface_index)
        multicast_packets_in = self.get_multicast_packets_in(interface_index)
        broadcast_packets_in = self.get_broadcast_packets_in(interface_index)

        if _MISSING_METRIC_VALUE not in [unicast_packets_in, multicast_packets_in, broadcast_packets_in]:
            return unicast_packets_in + multicast_packets_in + broadcast_packets_in
        else:
            return _MISSING_METRIC_VALUE

    def get_total_packets_out(self, interface_index):
        """
        Gets the number of packet packets that interface_index.

        Args:
            self: (todo): write your description
            interface_index: (int): write your description
        """
        unicast_packets_out = self.get_unicast_packets_out(interface_index)
        multicast_packets_out = self.get_multicast_packets_out(interface_index)
        broadcast_packets_out = self.get_broadcast_packets_out(interface_index)

        if _MISSING_METRIC_VALUE not in [unicast_packets_out, multicast_packets_out, broadcast_packets_out]:
            return unicast_packets_out + multicast_packets_out + broadcast_packets_out
        else:
            return _MISSING_METRIC_VALUE

    def get_admin_state(self, interface_index):
        """
        Return the state of the admin state.

        Args:
            self: (todo): write your description
            interface_index: (str): write your description
        """
        return int(self._if_table_stats_map.get(ifAdminStatus + u'.' + interface_index, _MISSING_METRIC_VALUE))

    def get_oper_state(self, interface_index):
        """
        : return : py : py : class : interface.

        Args:
            self: (todo): write your description
            interface_index: (int): write your description
        """
        return int(self._if_table_stats_map.get(ifOperStatus + u'.' + interface_index, _MISSING_METRIC_VALUE))

    def get_oper_admin_state_mismatch(self, interface_index):
        """
        Gets the admin admin admin admin admin.

        Args:
            self: (todo): write your description
            interface_index: (str): write your description
        """
        return 0 if self.get_oper_state(interface_index) == self.get_admin_state(interface_index) else 1

    def get_discards_in(self, interface_index):
        """
        Returns the number of the interface in the given interface.

        Args:
            self: (todo): write your description
            interface_index: (int): write your description
        """
        return int(self._if_table_stats_map.get(ifInDiscards + u'.' + interface_index, _MISSING_METRIC_VALUE))

    def get_errors_in(self, interface_index):
        """
        Gets the number of errors in the interface

        Args:
            self: (todo): write your description
            interface_index: (str): write your description
        """
        return int(self._if_table_stats_map.get(ifInErrors + u'.' + interface_index, _MISSING_METRIC_VALUE))

    def get_discards_out(self, interface_index):
        """
        Return the number of a particular interface.

        Args:
            self: (todo): write your description
            interface_index: (int): write your description
        """
        return int(self._if_table_stats_map.get(ifOutDiscards + u'.' + interface_index, _MISSING_METRIC_VALUE))

    def get_errors_out(self, interface_index):
        """
        Return the number of errors for the interface.

        Args:
            self: (todo): write your description
            interface_index: (int): write your description
        """
        return int(self._if_table_stats_map.get(ifOutErrors + u'.' + interface_index, _MISSING_METRIC_VALUE))

    def get_mtu(self, interface_index):
        """
        Returns the interface interface interface_index.

        Args:
            self: (todo): write your description
            interface_index: (int): write your description
        """
        return int(self._if_table_stats_map.get(ifMtu + u'.' + interface_index, _MISSING_METRIC_VALUE))

    def get_if_high_speed(self, interface_index):
        """
        The interface interface interface speed.

        Args:
            self: (todo): write your description
            interface_index: (int): write your description
        """
        # n.b. adjusted value means I can't use 'get(..., _MISSING_METRIC_VALUE)' idiom
        # Mbps by definition
        return int(self._ifx_table_stats_map.get(ifHighSpeed + u'.' + interface_index, _MISSING_METRIC_VALUE))

    def get_if_speed(self, interface_index):
        """
        Return the interface interface interface interface interface.

        Args:
            self: (todo): write your description
            interface_index: (str): write your description
        """
        return int(self._if_table_stats_map.get(ifSpeed + u'.' + interface_index, _MISSING_METRIC_VALUE))

    def get_configured_speed(self, index):
        """
        Gets the speed speed.

        Args:
            self: (todo): write your description
            index: (int): write your description
        """
        return self.enrichment.get_enrichment_value(u'self', u'interface', index).get(u'configured_speed',
                                                                                      _MISSING_METRIC_VALUE)

    def get_errors_frame(self, interface_index):
        """
        Gets the error frame.

        Args:
            self: (todo): write your description
            interface_index: (int): write your description
        """
        return int(self._dot3stats_map.get(dot3StatsAlignmentErrors + u'.' + interface_index, _MISSING_METRIC_VALUE))

    def get_errors_crc(self, interface_index):
        """
        Gets the error statistics.

        Args:
            self: (todo): write your description
            interface_index: (str): write your description
        """
        return int(self._dot3stats_map.get(dot3StatsFCSErrors + u'.' + interface_index, _MISSING_METRIC_VALUE))

    def get_errors_giants(self, interface_index):
        """
        Gets the errors for the given interface.

        Args:
            self: (todo): write your description
            interface_index: (str): write your description
        """
        return int(self._dot3stats_map.get(dot3StatsFrameTooLongs + u'.' + interface_index, _MISSING_METRIC_VALUE))

    @threaded_cached_property
    def interface_indices(self):
        """
        Returns the interface indices.

        Args:
            self: (todo): write your description
        """
        result = set()
        for oid in self._ifx_table_stats_map:
            result.add(oid.split(u'.')[-1])
        return result

    def _getif_table_stats(self):
        """
        Returns the stats table.

        Args:
            self: (todo): write your description
        """
        result = dict()
        try:
            for oid in self._if_table_stats_map:
                index = oid.split(u'.')[-1]
                result[index] = dict()
                result[index][u'admin_state'] = self.get_admin_state(index)
                result[index][u'oper_state'] = self.get_oper_state(index)
                result[index][u'oper_admin_state_mismatch'] = self.get_oper_admin_state_mismatch(index)
                result[index][u'errors_in'] = self.get_errors_in(index)
                result[index][u'errors_out'] = self.get_errors_out(index)
                result[index][u'discards_in'] = self.get_discards_in(index)
                result[index][u'discards_out'] = self.get_discards_out(index)
                result[index][u'mtu'] = self.get_mtu(index)
            return result
        except Exception as e:
            self._polling_status.handle_exception(u'interface', e)

    def _getifx_table_stats(self):
        """
        Parse the packet.

        Args:
            self: (todo): write your description
        """
        result = dict()
        try:
            for oid in self._ifx_table_stats_map:
                index = oid.split(u'.')[-1]
                result[index] = dict()
                result[index][u'bits_in'] = self.get_bits_in(index)
                result[index][u'bits_out'] = self.get_bits_out(index)
                result[index][u'unicast_packets_in'] = self.get_unicast_packets_in(index)
                result[index][u'unicast_packets_out'] = self.get_unicast_packets_out(index)
                result[index][u'multicast_packets_in'] = self.get_multicast_packets_in(index)
                result[index][u'multicast_packets_out'] = self.get_multicast_packets_out(index)
                result[index][u'broadcast_packets_in'] = self.get_broadcast_packets_in(index)
                result[index][u'broadcast_packets_out'] = self.get_broadcast_packets_out(index)
                result[index][u'total_packets_in'] = self.get_total_packets_in(index)
                result[index][u'total_packets_out'] = self.get_total_packets_out(index)
                result[index][u'configured_speed'] = self.get_configured_speed(index)
            return result
        except Exception as e:
            self._polling_status.handle_exception(u'interface', e)

    def _getdot3stats(self):
        """
        Gather dot stats table.

        Args:
            self: (todo): write your description
        """
        result = dict()
        try:
            # Use ifx_table_stats_map b/c dot3stats is not defined for every machine
            for oid in self._ifx_table_stats_map:
                index = oid.split(u'.')[-1]
                result[index] = dict()
                result[index][u'errors_frame'] = self.get_errors_frame(index)
                result[index][u'errors_crc'] = self.get_errors_crc(index)
                result[index][u'errors_giants'] = self.get_errors_giants(index)
            return result
        except Exception as e:
            self._polling_status.handle_exception(u'interface', e)

    @staticmethod
    def _get_state_val(state):
        """
        Get the value of the given state.

        Args:
            state: (str): write your description
        """
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
            self._ifx_table_stats_map[ent.oid + u'.' + ent.index] = ent.value

    def _build_if_table_stats_map(self):
        """Maps child oids of ifTable to their respective values as PanoptesSNMPVariables"""
        if_table_stats = list()
        for metric in if_table_oids:
            for varbind in self._snmp_connection.bulk_walk(metric,
                                                           max_repetitions=self.snmp_configuration.max_repetitions):
                if_table_stats.append(varbind)

        self._if_table_stats_map = dict()
        for ent in if_table_stats:
            self._if_table_stats_map[ent.oid + u'.' + ent.index] = ent.value

    def _build_dot3stats_map(self):
        """Maps child oids of dot3statsTable to their respective values as PanoptesSNMPVariables"""
        dot3stats = list()
        for metric in dots3stats_table_oids:
            for varbind in self._snmp_connection.bulk_walk(metric,
                                                           max_repetitions=self.snmp_configuration.max_repetitions):
                dot3stats.append(varbind)

        self._dot3stats_map = dict()
        for ent in dot3stats:
            self._dot3stats_map[ent.oid + u'.' + ent.index] = ent.value

    def _smart_add_dimension(self, method, dimension_name, index):
        """
        Adds a dimension to the series.

        Args:
            self: (todo): write your description
            method: (str): write your description
            dimension_name: (str): write your description
            index: (todo): write your description
        """
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
                                                     metric_name=u'interface_polling_status')

        interface_metrics = dict()

        try:
            start_time = time.time()

            self._build_dot3stats_map()
            self._build_if_table_stats_map()
            self._build_ifx_table_stats_map()

            end_time = time.time()

            self._logger.info(u'SNMP calls for device %s completed in %.2f seconds' % (
                self.host, end_time - start_time))

            interface_metrics.update(self._getdot3stats())
            if_interface_metrics = self._getif_table_stats()
            ifx_interface_metrics = self._getifx_table_stats()

            if self._plugin_config.get('dimension', {}).get('include_interface_index', 0):
                """
                #Interface indexes are ephemeral and can change after the restart of a device or the snmp agent.
                To add this field as a dimension include the following in the .panoptes-plugin configuration file.

                [dimension]
                include_interface_index = 1    
                """
                self._DIMENSION_MAP.update({'interface_index': lambda x: str(x)})

            # https://github.com/PyCQA/pylint/issues/1694
            for i in self.interface_indices:  # pylint: disable=E1133
                if i not in interface_metrics:
                    interface_metrics[i] = dict()
                interface_metrics[i].update(ifx_interface_metrics[i])
                interface_metrics[i].update(if_interface_metrics[i])

            for interface_index in list(interface_metrics.keys()):
                self._interface_metrics_group = PanoptesMetricsGroup(self.resource, u'interface',
                                                                     self.execute_frequency)
                interface = interface_metrics[interface_index]

                for dimension_name, dimension_method in list(self._DIMENSION_MAP.items()):
                    self._smart_add_dimension(method=dimension_method,
                                              dimension_name=dimension_name,
                                              index=interface_index
                                              )

                for metric in list(interface.keys()):
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

            self._polling_status.handle_success(u'interface')
            self._logger.debug(u'Found interface metrics: "%s" for device "%s"' % (
                interface_metrics, self.host))
        except Exception as e:
            self._polling_status.handle_exception(u'interface', e)
        finally:
            self._device_interface_metrics.add(self._polling_status.device_status_metrics_group)
            return self._device_interface_metrics
