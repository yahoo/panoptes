"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
import time

from yahoo_panoptes.enrichment.enrichment_plugin import PanoptesEnrichmentPlugin, PanoptesEnrichmentPluginError
from yahoo_panoptes.enrichment.schema.generic.snmp import PanoptesGenericSNMPMetricsEnrichmentGroup
from yahoo_panoptes.framework.enrichment import PanoptesEnrichmentGroupSet
from yahoo_panoptes.plugins.helpers.snmp_connections import PanoptesSNMPConnectionFactory

_DEFAULT_SNMP_TIMEOUT = 5
_DEFAULT_SNMP_RETRIES = 1
_DEFAULT_SNMP_NON_REPEATERS = 0
_DEFAULT_SNMP_MAX_REPETITIONS = 25
_DEFAULT_METRICS_SCHEMA_NAMESPACE = u'metrics'


class PanoptesEnrichmentGenericSNMPPlugin(PanoptesEnrichmentPlugin):
    def __init__(self):
        self._plugin_context = None
        self._plugin_name = None
        self._device_resource = None
        self._logger = None
        self._plugin_conf = None
        self._execute_frequency = None
        self._enrichment_ttl = None
        self._enrichment_namespace = None
        self._snmp_timeout = None
        self._snmp_retries = None
        self._snmp_non_repeaters = None
        self._snmp_max_repetitions = None
        self._device_fqdn = None
        self._enrichment_group = None
        self._enrichment_group_set = None
        self._metrics_groups = None
        self._snmp_connection = None
        self._oids_map = None

        super(PanoptesEnrichmentGenericSNMPPlugin, self).__init__()

    def get_enrichment(self):
        return self.enrichment_group_set

    def run(self, context):
        """
        The main entry point to the plugin

        Args:
            context (PanoptesPluginContext): The Plugin Context passed by the Enrichment Plugin Agent

        Returns:
            PanoptesEnrichmentGroupSet: A enrichment group e set

        Raises:
            PanoptesEnrichmentPluginError: This exception is raised if any part of the enrichment process has errors
        """
        self._plugin_context = context
        self._device_resource = context.data
        self._logger = context.logger
        self._plugin_conf = context.config
        self._plugin_name = self._plugin_conf[u'Core'][u'name']

        self._device_fqdn = self._device_resource.resource_endpoint

        try:
            self._execute_frequency = int(self._plugin_conf[u'main'][u'execute_frequency'])
            self._enrichment_ttl = int(self._plugin_conf[u'main'][u'enrichment_ttl'])
            self._enrichment_namespace = self._plugin_conf.get(u'main', {}).get(u'enrichment_namespace',
                                                                                _DEFAULT_METRICS_SCHEMA_NAMESPACE)
            self._snmp_timeout = int(self._plugin_conf.get(u'snmp', {}).get(u'timeout', _DEFAULT_SNMP_TIMEOUT))
            self._snmp_retries = int(self._plugin_conf.get(u'snmp', {}).get(u'retries', _DEFAULT_SNMP_RETRIES))
            self._snmp_non_repeaters = int(self._plugin_conf.get(u'snmp', {}).get(u'non_repeaters',
                                                                                  _DEFAULT_SNMP_NON_REPEATERS))
            self._snmp_max_repetitions = int(self._plugin_conf.get(u'snmp', {}).get(u'max_repetitions',
                                                                                    _DEFAULT_SNMP_MAX_REPETITIONS))
        except Exception as e:
            raise PanoptesEnrichmentPluginError(
                u'[{}] Error while parsing configuration for device "{}": {}'.format(self._plugin_name,
                                                                                     self._device_fqdn,
                                                                                     repr(e)))

        self._logger.debug(u'[{}] For device "{}", '
                           u'going to use enrichment namespace "{}"'.format(self._plugin_name,
                                                                            self.device_fqdn,
                                                                            self._enrichment_namespace)
                           )

        self._enrichment_group = self.metrics_enrichment_class(enrichment_ttl=self._enrichment_ttl,
                                                               execute_frequency=self._execute_frequency)
        self._enrichment_group_set = PanoptesEnrichmentGroupSet(self._device_resource)
        self._metrics_groups = None

        try:
            self._snmp_connection = PanoptesSNMPConnectionFactory.get_snmp_connection(
                plugin_context=context, resource=self._device_resource,
                timeout=self._snmp_timeout, retries=self._snmp_retries)
        except Exception as e:
            raise PanoptesEnrichmentPluginError(
                u'[{}] Error while creating SNMP connection for the device "{}": {}'.format(self._plugin_name,
                                                                                            self._device_fqdn,
                                                                                            repr(e)))

        self._oids_map = None

        start_time = time.time()

        self._logger.info(u'[{}] Going to poll device "{}" for metrics enrichment'.format(self._plugin_name,
                                                                                          self._device_fqdn))

        device_results = self.get_enrichment()

        end_time = time.time()

        if device_results:
            self._logger.info(
                u'[{}] Done polling enrichment for device "{}" in {:.2f} seconds, {} elements'.format(
                    self._plugin_name, self._device_fqdn, end_time - start_time, len(device_results)))
        else:
            self._logger.warn(u'[{}] Error polling metrics enrichment for device "{}"'.format(self._plugin_name,
                                                                                              self._device_fqdn))

        return device_results

    @property
    def metrics_enrichment_class(self):
        PanoptesGenericSNMPMetricsEnrichmentGroup.METRICS_SCHEMA_NAMESPACE = self._enrichment_namespace
        return PanoptesGenericSNMPMetricsEnrichmentGroup

    @property
    def oids_map(self):
        return self._oids_map

    @property
    def metrics_groups(self):
        return self._metrics_groups

    @property
    def device_fqdn(self):
        return self._device_fqdn

    @property
    def enrichment_group(self):
        return self._enrichment_group

    @property
    def enrichment_group_set(self):
        return self._enrichment_group_set
