"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
import time
from importlib import import_module

from yahoo_panoptes.framework.plugins.panoptes_base_plugin import PanoptesBasePlugin, \
    PanoptesPluginRuntimeError, PanoptesPluginConfigurationError
from yahoo_panoptes.plugins.helpers.snmp_connections import PanoptesSNMPConnectionFactory
from yahoo_panoptes.framework.utilities.snmp.connection import PanoptesSNMPPluginConfiguration


class PanoptesSNMPBasePlugin(PanoptesBasePlugin):
    def __init__(self):
        super(PanoptesSNMPBasePlugin, self).__init__()
        self._plugin_context = None
        self._plugin_config = None
        self._logger = None
        self._snmp_configuration = None
        self._execute_frequency = None
        self._resource = None
        self._enrichment = None
        self._host = None

    @property
    def plugin_context(self):
        return self._plugin_context

    @property
    def plugin_config(self):
        return self._plugin_config

    @property
    def logger(self):
        return self._logger

    @property
    def resource(self):
        return self._resource

    @property
    def enrichment(self):
        return self._enrichment

    @property
    def execute_frequency(self):
        return self._execute_frequency

    @property
    def host(self):
        return self._host

    @property
    def snmp_configuration(self):
        return self._snmp_configuration

    @property
    def snmp_connection(self):
        return self._snmp_connection

    def _get_snmp_connection(self):
        snmp_connection_class = getattr(import_module(self.snmp_configuration.connection_factory_module),
                                        self.snmp_configuration.connection_factory_class)

        assert issubclass(snmp_connection_class,
                          PanoptesSNMPConnectionFactory), u'SNMP connection class must be a subclass of ' \
                                                          u'PanoptesSNMPConnectionFactory'

        self._snmp_connection = snmp_connection_class.get_snmp_connection(
            plugin_context=self._plugin_context,
            resource=self._plugin_context.data,
            timeout=self._snmp_configuration.timeout,
            retries=self._snmp_configuration.retries,
            port=self._snmp_configuration.port
        )

    def get_results(self):
        return

    def run(self, context):
        self._plugin_context = context
        self._plugin_config = context.config
        self._logger = context.logger
        self._execute_frequency = int(self._plugin_config[u'main'][u'execute_frequency'])
        self._resource = context.data
        self._enrichment = context.enrichment
        self._host = self._resource.resource_endpoint

        try:
            # Max Repetitions && Tests
            self._snmp_configuration = PanoptesSNMPPluginConfiguration(self._plugin_context)
        except Exception as e:
            raise PanoptesPluginConfigurationError(u'Error parsing SNMP configuration: {}'.format(repr(e)))

        try:
            self._get_snmp_connection()
        except Exception as e:
            raise PanoptesPluginRuntimeError(u'Error creating SNMP connection: {}'.format(repr(e)))

        start_time = time.time()

        self.logger.info(u'Going to poll device "{}:{}" for metrics'.format(self._host, self.snmp_configuration.port))

        results = self.get_results()

        end_time = time.time()

        if results:
            self.logger.info(
                    u'Done polling metrics for device "{}" in {:.2f} seconds, {} metrics groups'.format(
                            self._host, end_time - start_time, len(results)))
        else:
            self.logger.warn(u'Error polling metrics for device {}'.format(self._host))

        return results


class PanoptesSNMPBaseEnrichmentPlugin(PanoptesSNMPBasePlugin):
    def __init__(self):
        super(PanoptesSNMPBaseEnrichmentPlugin, self).__init__()
        self._enrichment_ttl = None

    @property
    def enrichment_ttl(self):
        return self._enrichment_ttl

    def run(self, context):
        self._enrichment_ttl = int(context.config[u'main'][u'enrichment_ttl'])
        return super(PanoptesSNMPBaseEnrichmentPlugin, self).run(context)
