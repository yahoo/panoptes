import time

from yahoo_panoptes.enrichment.enrichment_plugin import PanoptesEnrichmentPlugin, PanoptesEnrichmentPluginError
from yahoo_panoptes.enrichment.schema.generic.snmp import PanoptesGenericSNMPMetricsEnrichmentGroup
from yahoo_panoptes.framework.plugins import panoptes_base_plugin, base_snmp_plugin
from yahoo_panoptes.framework.enrichment import PanoptesEnrichmentGroupSet

_DEFAULT_SNMP_TIMEOUT = 5
_DEFAULT_SNMP_RETRIES = 1
_DEFAULT_SNMP_NON_REPEATERS = 0
_DEFAULT_SNMP_MAX_REPETITIONS = 25


class PanoptesEnrichmentGenericSNMPPlugin(base_snmp_plugin.PanoptesSNMPBaseEnrichmentPlugin, PanoptesEnrichmentPlugin):
    def __init__(self):
        self._plugin_conf = None
        self._execute_frequency = None
        self._enrichment_ttl = None
        self._snmp_timeout = None
        self._snmp_retries = None
        self._snmp_non_repeaters = None
        self._snmp_max_repetitions = None
        self._enrichment_group = None
        self._enrichment_group_set = None
        self._metrics_groups = None
        self._snmp_connection = None
        self._oids_map = None

        super(PanoptesEnrichmentGenericSNMPPlugin, self).__init__()

    def get_results(self):
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
        self._logger = context.logger
        self._plugin_conf = context.config
        self._resource = context.data

        try:
            self._enrichment_ttl = int(self._plugin_conf['main']['enrichment_ttl'])
            self._execute_frequency = int(self._plugin_conf['main']['execute_frequency'])
            self._snmp_timeout = int(self._plugin_conf.get('snmp', {}).get('timeout', _DEFAULT_SNMP_TIMEOUT))
            self._snmp_retries = int(self._plugin_conf.get('snmp', {}).get('retries', _DEFAULT_SNMP_RETRIES))
            self._snmp_non_repeaters = int(self._plugin_conf.get('snmp', {}).get('non_repeaters',
                                                                                 _DEFAULT_SNMP_NON_REPEATERS))
            self._snmp_max_repetitions = int(self._plugin_conf.get('snmp', {}).get('max_repetitions',
                                                                                   _DEFAULT_SNMP_MAX_REPETITIONS))
        except Exception as e:
            raise PanoptesEnrichmentPluginError(
                "Error while parsing configuration for device {}: {}".format(self._device_fqdn, repr(e)))

        self._enrichment_group = self.metrics_enrichment_class(enrichment_ttl=self._enrichment_ttl,
                                                               execute_frequency=self._execute_frequency)
        self._enrichment_group_set = PanoptesEnrichmentGroupSet(self._resource)
        self._metrics_groups = None

        return super(PanoptesEnrichmentGenericSNMPPlugin, self).run(context)

    @property
    def metrics_enrichment_class(self):
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
