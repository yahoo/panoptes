"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements the Discovery Plugin Agent which accepts plugin names as Celery Task parameters and executes
them. Results, if any, returned by each plugin are expected to be PanoptesResourceSets. If the returned Resource Set is
not empty, each resource from the resource set is placed on a Kafka queue named '<resource_site>-discovery'

This module is expected to be imported and executed though the Celery 'worker' command line tool
"""
import faulthandler
import json
import sys

from celery import task
from celery.signals import worker_shutdown

from ..framework import const
from ..framework.exceptions import PanoptesBaseException
from ..framework.context import PanoptesContext
from ..framework.celery_manager import PanoptesCeleryConfig, PanoptesCeleryInstance
from ..framework.resources import PanoptesResourceSet
from ..framework.plugins.panoptes_base_plugin import PanoptesPluginInfo
from ..framework.plugins.runner import PanoptesPluginWithEnrichmentRunner
from ..framework.utilities.helpers import get_calling_module_name
from ..framework.utilities.key_value_store import PanoptesKeyValueStore
from ..framework.utilities.secrets import PanoptesSecretsStore
from .panoptes_discovery_plugin import PanoptesDiscoveryPlugin

panoptes_context = None
panoptes_discovery_task_context = None
celery = None


class PanoptesDiscoveryPluginAgentKeyValueStore(PanoptesKeyValueStore):
    """
    A custom Key/Value store for the Discovery Plugin Agent which store data like a plugin's last execution time and \
    the last result production time
    """

    def __init__(self, panoptes_context):
        super(PanoptesDiscoveryPluginAgentKeyValueStore, self).\
            __init__(panoptes_context, const.DISCOVERY_PLUGIN_AGENT_KEY_VALUE_NAMESPACE)


class PanoptesDiscoveryPluginKeyValueStore(PanoptesKeyValueStore):
    """
    A custom Key/Value store for the Panoptes Discovery plugins

    Plugins executed by the Discovery Plugin Agent get access to this store and can get/set values they need for their\
    operation
    """

    def __init__(self, panoptes_context):
        super(PanoptesDiscoveryPluginKeyValueStore, self).__init__(panoptes_context, const.PLUGINS_KEY_VALUE_NAMESPACE)


class PanoptesDiscoveryAgentContext(PanoptesContext):
    """
    This class implements a PanoptesContext with no clients - only used for setting up the Celery app
    """

    def __init__(self):
        super(PanoptesDiscoveryAgentContext, self).__init__(key_value_store_class_list=[],
                                                            create_message_producer=False,
                                                            create_zookeeper_client=False)


class PanoptesDiscoveryTaskContext(PanoptesContext):
    """
    This class implements a PanoptesContext with the following:
        - A Key/Value Store for Discovery Plugin Agent
        - A Key/Value Store for Panoptes Plugins
        - A Asynchronous Message Producer
        - A Zookeeper Client
    """

    def __init__(self):
        super(PanoptesDiscoveryTaskContext, self).__init__(
            key_value_store_class_list=[PanoptesDiscoveryPluginAgentKeyValueStore,
                                        PanoptesDiscoveryPluginKeyValueStore,
                                        PanoptesSecretsStore],
            create_message_producer=True, async_message_producer=True, create_zookeeper_client=True)


class PanoptesDiscoveryPluginAgentError(PanoptesBaseException):
    """
    The exception class for Discovery Plugin Agent runtime errors
    """
    pass


@task
def discovery_plugin_task(discovery_plugin_name):
    """
    The main method of the Discovery Agent

    This method, called by Celery, loads and executes the specified plugin. The workflow is as follows:
        - Locate and load plugin through `yapsy's <http://yapsy.sourceforge.net/>`_ plugin manager
        - Evaluate if the plugin should be executed right now
        - If yes, then create a plugin context
        - Attempt to get a lock for the instance of the plugin (unique combination of the plugin name and configuration)
        - If the lock is acquired, then attempt to execute the plugin
        - If the plugin executes successfully, update the timestamp in the Discovery Plugin Agent Key/Value store
        - If the plugin produced a non-zero result set, send the resource over the message bus
        - If sending over the message succeeds, update the timestamp in the Discovery Plugin Agent Key/Value store

    Args:
        discovery_plugin_name (str): The name of the plugin to be executed

    Returns:
        PanoptesResourceSet

    """
    global panoptes_discovery_task_context

    if panoptes_discovery_task_context is None:
        try:
            panoptes_discovery_task_context = PanoptesDiscoveryTaskContext()
        except Exception as e:
            sys.exit('Could not create a Panoptes Discovery Task Context: %s' % (repr(e)))

    logger = panoptes_context.logger

    logger.debug('panoptes_context object: %s' % panoptes_context)

    try:
        plugin_runner = PanoptesPluginWithEnrichmentRunner(
            plugin_name=discovery_plugin_name,
            plugin_type='discovery',
            plugin_class=PanoptesDiscoveryPlugin,
            plugin_info_class=PanoptesPluginInfo,
            plugin_data=None,
            panoptes_context=panoptes_discovery_task_context,
            plugin_agent_kv_store_class=PanoptesDiscoveryPluginAgentKeyValueStore,
            plugin_kv_store_class=PanoptesDiscoveryPluginKeyValueStore,
            plugin_secrets_store_class=PanoptesSecretsStore,
            plugin_logger_name=const.DISCOVERY_PLUGIN_AGENT_PLUGINS_CHILD_LOGGER_NAME,
            plugin_result_class=PanoptesResourceSet,
            results_callback=__send_resource_set)
        plugin_runner.execute_plugin()
    except Exception as e:
        logger.error('[%s] Error executing plugin: %s' % (discovery_plugin_name, str(e)))


def __send_resource_set(context, results, plugin):
    """
    Emits each resource from the provided resource set to message bus

    Args:
        context (PanoptesContext): The PanoptesContext being used by the Plugin Agent
        results (PanoptesResourceSet): The ResourceSet returned by the plugin
        plugin (PanoptesPluginInfo): The plugin object that produced this set of results

    Returns:
        None
    """
    logger = context.logger
    producer = context.message_producer
    key = str(plugin.normalized_name) + ':' + str(plugin.version) + ':' + str(plugin.signature)
    resources_by_site = results.get_resources_by_site()
    logger.debug('Results: %s' % str(resources_by_site))
    logger.info('Found %d sites in results' % len(resources_by_site))
    for resource_site in resources_by_site.keys():
        topic = resource_site + '-' + 'resources'
        resource_list = dict(resources=[resource.raw for resource in resources_by_site[resource_site]])
        resource_list['resource_set_creation_timestamp'] = results.resource_set_creation_timestamp
        resource_list['resource_set_schema_version'] = results.resource_set_schema_version
        logger.debug('Going to send resource "%s" to message bus' % resource_list)
        try:
            producer.send_messages(topic, key, json.dumps(resource_list))
        except:
            raise
        logger.debug('Sent resource "%s" to message bus' % resource_list)


@worker_shutdown.connect()
def shutdown_signal_handler(sender, args=None, **kwargs):
    """
    This method handles the shutdown signal from Celery

    Args:
        sender (Celery): The Celery app which sent the shutdown signal
        args (dict):
        **kwargs (dict):

    Returns:
        None
    """
    global panoptes_context
    panoptes_context.logger.info('Discovery Plugin Agent Worker shutting down - going to stop message producer')
    try:
        panoptes_context.message_producer.stop()
    except Exception as e:
        panoptes_context.logger.error('Could not shutdown message producer: %s' % str(e))


def start_discovery_plugin_agent():
    """
    The entry point for the Discovery Plugin Agent

    This method creates a Panoptes Context and the Celery Instance for the Discovery Plugin Agent

    Returns:
        None
    """
    global panoptes_context, celery
    try:
        panoptes_context = PanoptesDiscoveryAgentContext()
    except Exception as e:
        sys.exit('Could not create a Panoptes Context: %s' % (str(e)))

    logger = panoptes_context.logger
    logger.info('Attempting to start Celery application')

    celery_config = PanoptesCeleryConfig(const.DISCOVERY_PLUGIN_AGENT_CELERY_APP_NAME)

    try:
        celery = PanoptesCeleryInstance(panoptes_context, celery_config).celery
    except Exception as exp:
        sys.exit('Could not instantiate Celery application: %s' % str(exp))
    else:
        logger.info('Started Celery application: %s' % celery)


if get_calling_module_name() == const.CELERY_LOADER_MODULE:
    faulthandler.enable()
    start_discovery_plugin_agent()
