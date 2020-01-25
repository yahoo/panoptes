from builtins import object
from time import time

from yahoo_panoptes.framework.enrichment import PanoptesEnrichmentSet, \
    PanoptesEnrichmentGroupSet, PanoptesEnrichmentMultiGroupSet
from yahoo_panoptes.enrichment.enrichment_plugin import PanoptesEnrichmentPlugin, PanoptesEnrichmentPluginError
from yahoo_panoptes.enrichment.schema.heartbeat import PanoptesHeartbeatEnrichmentGroup


class HeartbeatEnrichment(object):
    def __init__(self, plugin_context, device_resource, execute_frequency, enrichment_ttl):
        self._plugin_context = plugin_context
        self._logger = plugin_context.logger
        self._device_resource = device_resource
        self._device_fqdn = device_resource.resource_endpoint
        self._heartbeat_enrichment_group = \
            PanoptesHeartbeatEnrichmentGroup(execute_frequency=execute_frequency,
                                             enrichment_ttl=enrichment_ttl)
        self._heartbeat_enrichment_group_set = PanoptesEnrichmentGroupSet(device_resource)
        self._heartbeat_enrichment_multi_group_set = PanoptesEnrichmentMultiGroupSet()

    def get_enrichment(self):
        try:
            logger = self._logger

            heartbeat_enrichment_set = PanoptesEnrichmentSet(u'heartbeat')
            heartbeat_enrichment_set.add(u'timestamp', time())

            self._heartbeat_enrichment_group.add_enrichment_set(heartbeat_enrichment_set)

            self._heartbeat_enrichment_group_set.add_enrichment_group(self._heartbeat_enrichment_group)

            logger.debug(u'Heartbeat enrichment for host {} PanoptesEnrichmentGroupSet {}'.
                         format(self._device_fqdn, self._heartbeat_enrichment_group_set))

            self._heartbeat_enrichment_multi_group_set.add_enrichment_group_set(self._heartbeat_enrichment_group_set)

            return self._heartbeat_enrichment_multi_group_set
        except Exception as e:
            raise PanoptesEnrichmentPluginError(u'Failed to get heartbeat timestamp enrichment for the host "%s": %s' %
                                                (self._device_fqdn, repr(e)))


class PluginEnrichmentHeartbeat(PanoptesEnrichmentPlugin):
    def run(self, context):
        """
        The main entry point to the plugin

        Args:
            context (PanoptesPluginContext): The Plugin Context passed by the Plugin Agent

        Returns:
            PanoptesEnrichmentMultiGroupSet: A non-empty enrichment group set

        Raises:
            PanoptesEnrichmentPluginError: This exception is raised if any part of the metrics process has errors
        """
        conf = context.config
        logger = context.logger

        start_time = time()

        device_resource = context.data
        execute_frequency = int(conf[u'main'][u'execute_frequency'])
        enrichment_ttl = int(conf[u'main'][u'enrichment_ttl'])

        logger.info(u'Going to poll resource "%s" for heartbeat enrichment' % device_resource.resource_endpoint)

        device_polling = HeartbeatEnrichment(plugin_context=context, device_resource=device_resource,
                                             execute_frequency=execute_frequency, enrichment_ttl=enrichment_ttl)

        device_results = device_polling.get_enrichment()

        end_time = time()

        if device_results:
            logger.info(
                u'Done polling heartbeat enrichment for host "%s" in %.2f seconds, %s elements' % (
                    device_resource.resource_endpoint, end_time - start_time, len(device_results)))
        else:
            logger.warn(u'Error polling heartbeat enrichment for device %s' % device_resource.resource_endpoint)

        return device_results
