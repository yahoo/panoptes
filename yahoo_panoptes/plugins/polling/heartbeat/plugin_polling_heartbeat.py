from builtins import object
from time import time
from yahoo_panoptes.polling.polling_plugin import PanoptesPollingPlugin, PanoptesPollingPluginError
from yahoo_panoptes.framework.metrics import PanoptesMetricsGroupSet, PanoptesMetricsGroup, PanoptesMetric, \
    PanoptesMetricType


class HeartbeatMetrics(object):
    def __init__(self, plugin_context, device_resource, execute_frequency):
        self._plugin_context = plugin_context
        self._logger = plugin_context.logger
        self._enrichment = plugin_context.enrichment
        self._device_resource = device_resource
        self._device_fqdn = device_resource.resource_endpoint
        self._execute_frequency = execute_frequency
        self._device_heartbeat_metrics = PanoptesMetricsGroupSet()

    def get_metrics(self):
        try:
            logger = self._logger

            events_ts_metric_group = PanoptesMetricsGroup(self._device_resource, u'heartbeat', self._execute_frequency)
            events_ts_metric_group.add_metric(PanoptesMetric(u'status', 1, PanoptesMetricType.GAUGE))
            events_ts_metric_group.add_metric(PanoptesMetric(
                u'heartbeat_enrichment_timestamp', self._get_enrichment_ts(), PanoptesMetricType.GAUGE))
            self._device_heartbeat_metrics.add(events_ts_metric_group)

            logger.debug(u'Heartbeat metrics for host {} PanoptesMetricsGroupSet {}'
                         .format(self._device_fqdn, self._device_heartbeat_metrics))

            return self._device_heartbeat_metrics

        except Exception as e:
            raise PanoptesPollingPluginError(
                u'Failed to get timestamp metrics for the host "%s": %s' % (
                    self._device_fqdn, repr(e)))

    def _get_enrichment_ts(self):
        try:
            heartbeat_enrichment = self._enrichment.get_enrichment_value(u'self', u'heartbeat_ns', u'heartbeat')
            heartbeat_enrichment_timestamp = heartbeat_enrichment[u'timestamp']
            return int(heartbeat_enrichment_timestamp)
        except Exception as e:
            self._logger.error(u'Error while fetching enrichment heartbeat timestamp for Host {}: {}'.
                               format(self._device_fqdn, repr(e)))
            return -1


class PluginPollingHeartbeat(PanoptesPollingPlugin):
    def run(self, context):
        """
        The main entry point to the plugin

        Args:
            context (PanoptesPluginContext): The Plugin Context passed by the Plugin Agent

        Returns:
            PanoptesMetricsGroupSet: A non-empty resource set

        Raises:
            PanoptesPollingPluginError: This exception is raised if any part of the metrics process has errors
        """

        conf = context.config
        logger = context.logger

        start_time = time()

        device_resource = context.data

        execute_frequency = int(conf[u'main'][u'execute_frequency'])

        logger.info(
            u'Going to poll host "%s" for heartbeat metrics' % (
                device_resource.resource_endpoint))

        device_polling = HeartbeatMetrics(plugin_context=context, device_resource=device_resource,
                                          execute_frequency=execute_frequency)

        device_results = device_polling.get_metrics()

        end_time = time()

        if device_results:
            logger.info(
                u'Done polling heartbeat metrics for host "%s" in %.2f seconds, %s metrics' % (
                    device_resource.resource_endpoint, end_time - start_time, len(device_results)))
        else:
            logger.warn(u'Error polling heartbeat metrics for host %s' % device_resource.resource_endpoint)

        return device_results
