"""
This module contains a plugin that can ping the given device and return the loss percentage and round trip min, max,
average and standard deviation (all in ms) as well the ping status
"""
from time import time
from yahoo_panoptes.framework.metrics import PanoptesMetricsGroup, PanoptesMetricsGroupSet, PanoptesMetric, \
    PanoptesMetricType
from yahoo_panoptes.framework.utilities.ping import PanoptesPing
from yahoo_panoptes.polling.polling_plugin import PanoptesPollingPlugin, PanoptesPollingPluginConfigurationError
from yahoo_panoptes.plugins.polling.utilities.polling_status import DEVICE_METRICS_STATES

PING_METRICS = {
    u'packet_loss_percent': u'packet_loss_pct',
    u'round_trip_minimum': u'round_trip_min',
    u'round_trip_maximum': u'round_trip_max',
    u'round_trip_average': u'round_trip_avg',
    u'round_trip_standard_deviation': u'round_trip_stddev'
}

DEFAULT_PING_COUNT = 10
DEFAULT_PING_TIMEOUT = 10


class PluginPollingPing(PanoptesPollingPlugin):
    def run(self, context):
        logger = context.logger
        resource = context.data
        host = resource.resource_endpoint
        config = context.config[u'main']
        execute_frequency = int(config[u'execute_frequency'])

        start_time = time()

        try:
            count = int(config[u'count'])
        except KeyError:
            count = DEFAULT_PING_COUNT
            logger.info(u'For device {}, count not set - setting it to {}'.format(host, DEFAULT_PING_COUNT))
        except ValueError:
            raise PanoptesPollingPluginConfigurationError(
                u'For device {}, configured count is not an integer: {}'.format(host, config[u'count']))

        try:
            timeout = int(config[u'timeout'])
        except KeyError:
            timeout = DEFAULT_PING_TIMEOUT
            logger.info(u'For device {}, timeout not set - setting it to {}s'.format(host, DEFAULT_PING_TIMEOUT))
        except ValueError:
            raise PanoptesPollingPluginConfigurationError(
                u'For device {}, configured timeout is not an integer: {}'.format(host, config[u'timeout']))

        ping_metrics_group = PanoptesMetricsGroup(resource, u'ping', execute_frequency)

        try:
            panoptes_ping = PanoptesPing(hostname=host, count=count, timeout=timeout)
            for metric, object_property in list(PING_METRICS.items()):
                ping_metrics_group.add_metric(PanoptesMetric(metric,
                                                             getattr(panoptes_ping, object_property),
                                                             PanoptesMetricType.GAUGE))
            if panoptes_ping.packet_loss_pct == 100.0:
                ping_status = DEVICE_METRICS_STATES.PING_FAILURE
            else:
                ping_status = DEVICE_METRICS_STATES.SUCCESS
        except Exception as e:
            logger.warn(u'For device {}, ping failed: {}'.format(host, repr(e)))
            ping_status = DEVICE_METRICS_STATES.PING_FAILURE

        ping_metrics_group.add_metric(PanoptesMetric(u'ping_status', ping_status, PanoptesMetricType.GAUGE))

        logger.debug(u'For device {}, ping results are: {}'.format(host, str(ping_metrics_group.json)))

        ping_metrics_group_set = PanoptesMetricsGroupSet()
        ping_metrics_group_set.add(ping_metrics_group)

        end_time = time()

        logger.info(u'Done pinging device "{}" in {} seconds, {} metric groups'.format(host,
                                                                                       round(end_time - start_time, 2),
                                                                                       len(ping_metrics_group_set)))

        return ping_metrics_group_set
