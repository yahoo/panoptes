"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements the Enrichment Plugin Agent which accepts plugin names as Celery Task parameters and executes
them. Results, if any, returned by each plugin are expected to be PanoptesEnrichmentGroupSet. If the returned Metrics
Set is not empty, each metric from the metric set is placed on a Kafka queue named 'metric'

This module is expected to be imported and executed though the Celery 'worker' command line tool
"""
import faulthandler
import sys

from celery import task

from ..framework import const
from ..framework.exceptions import PanoptesBaseException
from ..framework.context import PanoptesContext
from ..framework.celery_manager import PanoptesCeleryConfig, PanoptesCeleryInstance
from ..framework.resources import PanoptesResourceStore, PanoptesResourcesKeyValueStore
from ..framework.plugins.panoptes_base_plugin import PanoptesPluginInfo
from ..framework.plugins.runner import PanoptesPluginWithEnrichmentRunner
from ..framework.utilities.helpers import get_calling_module_name
from ..framework.utilities.key_value_store import PanoptesKeyValueStore
from ..framework.utilities.secrets import PanoptesSecretsStore
from ..framework.enrichment import PanoptesEnrichmentMultiGroupSet, \
    PanoptesEnrichmentCacheKeyValueStore, PanoptesEnrichmentGroupSet
from .enrichment_plugin import PanoptesEnrichmentPlugin

panoptes_context = None
panoptes_enrichment_task_context = None
celery = None


class PanoptesEnrichmentPluginAgentKeyValueStore(PanoptesKeyValueStore):
    """
    A custom Key/Value store for the Enrichment Plugin Agent which store data like a plugin's last execution time and \
    the last result production time
    """

    def __init__(self, panoptes_context):
        super(PanoptesEnrichmentPluginAgentKeyValueStore, self).__init__(
            panoptes_context, const.ENRICHMENT_PLUGIN_AGENT_KEY_VALUE_NAMESPACE)

        self._redis = panoptes_context.get_redis_connection(const.ENRICHMENT_REDIS_GROUP)


class PanoptesEnrichmentPluginKeyValueStore(PanoptesKeyValueStore):
    """
    A custom Key/Value store for the Panoptes Enrichment plugins

    Plugins executed by the Enrichment Plugin Agent get access to this store and can get/set values they need for their\
    operation
    """

    def __init__(self, panoptes_context):
        super(PanoptesEnrichmentPluginKeyValueStore, self).__init__(panoptes_context, const.PLUGINS_KEY_VALUE_NAMESPACE)


class PanoptesEnrichmentAgentContext(PanoptesContext):
    """
    This class implements a PanoptesContext with no clients - only used for setting up the Celery app
    """

    def __init__(self):
        super(PanoptesEnrichmentAgentContext, self).__init__(key_value_store_class_list=[],
                                                             create_message_producer=False,
                                                             create_zookeeper_client=False)


class PanoptesEnrichmentTaskContext(PanoptesContext):
    """
    This class implements a PanoptesContext with the following:
        - A Key/Value Store for Enrichment Plugin Agent
        - A Key/Value Store for Panoptes Plugins
        - A Asynchronous Message Producer
        - A Zookeeper Client
    """

    def __init__(self):
        super(PanoptesEnrichmentTaskContext, self).__init__(
                key_value_store_class_list=[PanoptesEnrichmentPluginAgentKeyValueStore,
                                            PanoptesEnrichmentPluginKeyValueStore,
                                            PanoptesEnrichmentCacheKeyValueStore,
                                            PanoptesSecretsStore,
                                            PanoptesResourcesKeyValueStore],
                create_message_producer=False, async_message_producer=False, create_zookeeper_client=True)


class PanoptesEnrichmentPluginAgentError(PanoptesBaseException):
    """
    The exception class for Enrichment Plugin Agent runtime errors
    """
    pass


@task
def enrichment_plugin_task(enrichment_plugin_name, resource_key):
    """
    The main method of the Enrichment Plugin Agent

    This method, called by Celery, loads and executes the specified plugin. The workflow is as follows:
        - Locate and load plugin through `yapsy's <http://yapsy.sourceforge.net/>`_ plugin manager
        - Evaluate if the plugin should be executed right now
        - If yes, then create a plugin context
        - Attempt to get a lock for the instance of the plugin (unique combination of the plugin name and configuration)
        - If the lock is acquired, then attempt to execute the plugin
        - If the plugin executes successfully, update the timestamp in the Enrichment Plugin Agent Key/Value store
        - If the plugin produced a non-zero result set, send the resource over the message bus
        - If sending over the message succeeds, update the timestamp in the Enrichment Plugin Agent Key/Value store

    Args:
        enrichment_plugin_name (str): The name of the plugin to be executed
        resource_key (dict): The dictionary that contains the resource key of the resource this enrichment plugin
        should collect info

    Returns:
        None

    """
    global panoptes_enrichment_task_context

    if panoptes_enrichment_task_context is None:
        try:
            panoptes_enrichment_task_context = PanoptesEnrichmentTaskContext()
        except Exception as e:
            sys.exit('Could not create a Panoptes Enrichment Task Context: %s' % (repr(e)))

    logger = panoptes_enrichment_task_context.logger

    try:
        resource = PanoptesResourceStore(panoptes_enrichment_task_context).get_resource(resource_key)
        plugin_runner = PanoptesPluginWithEnrichmentRunner(
            plugin_name=enrichment_plugin_name,
            plugin_type='enrichment',
            plugin_class=PanoptesEnrichmentPlugin,
            plugin_info_class=PanoptesPluginInfo,
            plugin_data=resource,
            panoptes_context=panoptes_enrichment_task_context,
            plugin_agent_kv_store_class=PanoptesEnrichmentPluginAgentKeyValueStore,
            plugin_kv_store_class=PanoptesEnrichmentPluginKeyValueStore,
            plugin_secrets_store_class=PanoptesSecretsStore,
            plugin_logger_name=const.ENRICHMENT_PLUGIN_AGENT_PLUGINS_CHILD_LOGGER_NAME,
            plugin_result_class=(PanoptesEnrichmentGroupSet, PanoptesEnrichmentMultiGroupSet),
            results_callback=_store_enrichment_data)
        plugin_runner.execute_plugin()
    except Exception as e:
        logger.error('[%s] Error executing plugin: %s' % (enrichment_plugin_name, str(e)))


def _store_enrichment_data(context, results, plugin):
    """
    Populate enrichment info on Redis kv store

    Args:
        context (PanoptesContext): The PanoptesContext being used by the Plugin Agent
        results (object): Enrichment data PanoptesEnrichmentGroupSet/PanoptesEnrichmentMultiGroupSet
        returned by the plugin
        plugin (PanoptesPluginInfo): The plugin object that produced this set of results

    Returns:
        None
    """
    logger = context.logger
    enrichment_key_value_store = context.get_kv_store(PanoptesEnrichmentCacheKeyValueStore)

    if isinstance(results, PanoptesEnrichmentGroupSet):
        resource = results.resource
        enrichment_groups = results.enrichment
        for enrichment_group in enrichment_groups:
            _update_enrichment_kv_store(logger, enrichment_key_value_store, resource, enrichment_group)
    elif isinstance(results, PanoptesEnrichmentMultiGroupSet):
        enrichment_group_sets = results.enrichment_group_sets
        for group_set in enrichment_group_sets:
            resource = group_set.resource
            for enrichment_group in group_set.enrichment:
                _update_enrichment_kv_store(logger, enrichment_key_value_store, resource, enrichment_group)


def _update_enrichment_kv_store(logger, enrichment_key_value_store, resource, enrichment_group):

    key = resource.resource_id + const.KV_NAMESPACE_DELIMITER + enrichment_group.namespace
    value = enrichment_group.serialize()

    logger.debug('Going to store enrichment info for resource id {} namespace {}'
                 .format(resource.resource_id, enrichment_group.namespace))

    try:
        enrichment_key_value_store.set(key, value, expire=enrichment_group.enrichment_ttl)
        logger.debug('Successfully populated enrichment info for resource id {} namespace {}'
                     .format(resource.resource_id, enrichment_group.namespace))
    except Exception as e:
        logger.error('Failed to store enrichment info for resource id {} namespace {}: {}'
                     .format(resource.resource_id, enrichment_group.namespace, e))


def start_enrichment_plugin_agent():
    """
    The entry point for the Enrichment Plugin Agent

    This method creates a Panoptes Context and the Celery Instance for the Enrichment Plugin Agent

    Returns:
        None
    """
    global panoptes_context, celery

    try:
        panoptes_context = PanoptesEnrichmentAgentContext()
    except Exception as e:
        sys.exit('Could not create a Panoptes Context: %s' % (str(e)))

    logger = panoptes_context.logger
    logger.info('Attempting to start Celery application')

    celery_config = PanoptesCeleryConfig(const.ENRICHMENT_PLUGIN_AGENT_CELERY_APP_NAME)

    try:
        celery = PanoptesCeleryInstance(panoptes_context, celery_config).celery
    except Exception as exp:
        sys.exit('Could not instantiate Celery application: %s' % str(exp))
    else:
        logger.info('Started Celery application: %s' % celery)


if get_calling_module_name() == const.CELERY_LOADER_MODULE:
    faulthandler.enable()
    start_enrichment_plugin_agent()
