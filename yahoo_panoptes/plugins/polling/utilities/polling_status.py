"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
from future import standard_library
from builtins import range
import inspect
import traceback
import logging
from collections import Counter

from requests.exceptions import ConnectTimeout, ConnectionError
from urllib3.exceptions import ConnectTimeoutError

from yahoo_panoptes.framework.metrics import PanoptesMetricsGroup, PanoptesMetric, PanoptesMetricType
from yahoo_panoptes.framework.metrics import PanoptesMetricsNullException
from yahoo_panoptes.framework.resources import PanoptesResource
from yahoo_panoptes.framework.enrichment import PanoptesEnrichmentCacheError
from yahoo_panoptes.framework.utilities.ping import *
from yahoo_panoptes.framework.utilities.snmp.exceptions import *

standard_library.install_aliases()
from urllib.error import URLError  # noqa


class DEVICE_METRICS_STATES(object):
    """
    DEVICE_METRICS_STATES encapsulates all the states a device may have during the process of collecting or attempting
        to collect metrics from said device.
    """
    SUCCESS, \
        AUTHENTICATION_FAILURE, \
        NETWORK_FAILURE, \
        TIMEOUT, \
        PARTIAL_METRIC_FAILURE, \
        INTERNAL_FAILURE, \
        MISSING_METRICS, \
        PING_FAILURE,\
        ENRICHMENT_FAILURE = list(range(9))


exceptions_dict = {
    ConnectTimeout: DEVICE_METRICS_STATES.TIMEOUT,
    ConnectTimeoutError: DEVICE_METRICS_STATES.TIMEOUT,
    ConnectionError: DEVICE_METRICS_STATES.NETWORK_FAILURE,
    PanoptesSNMPTimeoutException: DEVICE_METRICS_STATES.TIMEOUT,
    PanoptesSNMPConnectionException: DEVICE_METRICS_STATES.NETWORK_FAILURE,
    URLError: DEVICE_METRICS_STATES.NETWORK_FAILURE,
    PanoptesPingException: DEVICE_METRICS_STATES.PING_FAILURE,
    PanoptesPingTimeoutException: DEVICE_METRICS_STATES.PING_FAILURE,
    PanoptesMetricsNullException: DEVICE_METRICS_STATES.MISSING_METRICS,
    PanoptesEnrichmentCacheError: DEVICE_METRICS_STATES.ENRICHMENT_FAILURE,
}

EXCEPTIONS_KEYS = list(exceptions_dict.keys())

_PING_STATES = [DEVICE_METRICS_STATES.TIMEOUT, DEVICE_METRICS_STATES.NETWORK_FAILURE]


class PanoptesPollingStatus(object):
    """
    PanoptesPollingStatus is the primary class for tracking metrics collection for devices in the Panoptes environment.

    Args:
        resource(PanoptesResource): Which PanoptesResource this PanoptesPollingStatus instance will refer to
        execute_frequency(int): How frequently to execute the polling on the resource
        logger(logging.Logger): log for output
        ping(bool): Ping the device after any metric collection failure
        metrics_group_type_name(str): The metrics group type name, defaults to 'status'
        metric_name(str): The metric name, defaults to 'status'
    """

    def __init__(self, resource, execute_frequency, logger, ping=True,
                 metrics_group_type_name=u'status', metric_name=u'status', context=None):
        """
        Initialize the device.

        Args:
            self: (todo): write your description
            resource: (str): write your description
            execute_frequency: (float): write your description
            logger: (todo): write your description
            ping: (int): write your description
            metrics_group_type_name: (str): write your description
            metric_name: (str): write your description
            context: (str): write your description
        """
        assert isinstance(resource, PanoptesResource), u'resource must be an instance of PanoptesResource'
        assert PanoptesValidators.valid_nonzero_integer(execute_frequency), u'execute_frequency must be integer > 0'
        assert PanoptesValidators.valid_logger(logger), u'logger must be an instance of logging.Logger'

        self._device_name = resource.resource_endpoint
        self._resource = resource
        self._device_type = u':'.join([resource.resource_class, resource.resource_subclass, resource.resource_type])
        self._device_status_metrics_group = PanoptesMetricsGroup(resource, metrics_group_type_name, execute_frequency)
        self._metric_name = metric_name
        self._logger = logger
        self._metric_statuses = dict()
        self._device_status = DEVICE_METRICS_STATES.SUCCESS
        self._ping = ping
        self._context = context

    @property
    def device_status_metrics_group(self):
        """
        Creates device_status_metrics_group

        Returns:
            PanoptesMetricsGroup: The PanoptesMetricsGroup for the status of this device
        """
        if self._ping and self.device_status in _PING_STATES:
            try:
                panoptes_ping = PanoptesPingConnectionFactory.get_ping_connection(resource=self._resource,
                                                                                  context=self._context)
                if panoptes_ping.packet_loss_pct == 100.0:
                    self._device_status = DEVICE_METRICS_STATES.PING_FAILURE
            except:
                self._device_status = DEVICE_METRICS_STATES.PING_FAILURE

        self._device_status_metrics_group.add_metric(PanoptesMetric(self._metric_name,
                                                                    self._device_status,
                                                                    PanoptesMetricType.GAUGE))
        return self._device_status_metrics_group

    @property
    def device_status(self):
        """
        Returns:
            int: The status of the "device" metric
        """
        return self._device_status

    @property
    def device_name(self):
        """
        Returns:
             str: The name of this device
        """
        return self._device_name

    @property
    def device_type(self):
        """
        Returns:
             str: The type of this device
        """
        return self._device_type

    @property
    def logger(self):
        """
        Returns:
             logging.Logger: The logger for this device
        """
        return self._logger

    def handle_success(self, k):
        """
        Update the _device_metrics_status dictionary when a given metric has been successfully obtained.

        Args:
            k(str): name of the metric to apply the success to
        """
        self.logger.debug(
                u'Successfully polled %s "%s" for %s' % (str(self._device_type), str(self._device_name), str(k)))
        if k in self._metric_statuses:
            if self._metric_statuses[k] != DEVICE_METRICS_STATES.SUCCESS:
                self._metric_statuses[k] = DEVICE_METRICS_STATES.PARTIAL_METRIC_FAILURE
        else:
            self._metric_statuses[k] = DEVICE_METRICS_STATES.SUCCESS
        self._set_device_status()

    def handle_exception(self, k, e):
        """
        Mutate the state of the _device_metrics_status dictionary.

        Args:
            k(str): name of the metric to apply the exception to
            e(Exception): exception which has occurred for the given k
        """
        self.logger.warn(u'Error while trying to poll "%s" (%s) for "%s": %s - %s' %
                         (str(self._device_name), str(self._device_type), k, repr(e), traceback.format_exc()))

        if k in self._metric_statuses:
            if self._metric_statuses[k] in \
                    [DEVICE_METRICS_STATES.SUCCESS, DEVICE_METRICS_STATES.PARTIAL_METRIC_FAILURE]:
                self._metric_statuses[k] = DEVICE_METRICS_STATES.PARTIAL_METRIC_FAILURE
                self._set_device_status()
                return

        if type(e) in EXCEPTIONS_KEYS:
            self._metric_statuses[k] = exceptions_dict[type(e)]
        else:
            found_exception = False
            for exception in inspect.getmro(type(e)):
                if exception in EXCEPTIONS_KEYS:
                    self._metric_statuses[k] = exceptions_dict[exception]
                    found_exception = True
                    break
            if not found_exception:
                self._metric_statuses[k] = DEVICE_METRICS_STATES.INTERNAL_FAILURE
        self._set_device_status()

    def _set_device_status(self):
        """
        Call when done with operations on polling status object to set overall
        device status based upon component metric statuses.
        """
        if len(self._metric_statuses) > 0:
            if all(status == DEVICE_METRICS_STATES.SUCCESS for status in list(self._metric_statuses.values())):
                self._device_status = DEVICE_METRICS_STATES.SUCCESS
            elif DEVICE_METRICS_STATES.SUCCESS in list(self._metric_statuses.values()):
                self._device_status = DEVICE_METRICS_STATES.PARTIAL_METRIC_FAILURE
            else:
                count = Counter(list(self._metric_statuses.values()))
                if len(count.most_common()) > 0:

                    # get the most common (1) status and it's count as a tuple, and grab just its name
                    self._device_status = count.most_common(1)[0][0]
                else:
                    self._device_status = DEVICE_METRICS_STATES.INTERNAL_FAILURE
