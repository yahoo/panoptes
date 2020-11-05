"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

The module implements the Enrichment Plugin Scheduler which finds any enrichment Plugins in the configured path and then
installs/updates a Celery Beat schedule for each enrichment plugin and configuration combination.

If the list or the configuration of the plugins changes, the schedule is updated

This module is expected to be imported and executed though the Celery 'beat' command line tool

Internally, there are two threads that get setup: one is the main thread that runs the Celery Beat service. The other is
the thread started by the Enrichment Plugin Scheduler to detect and update plugin/configuration changes
"""
import faulthandler
import sys
import time
from resource import getrusage, RUSAGE_SELF
from datetime import datetime, timedelta

from celery.signals import beat_init

from yahoo_panoptes.framework import const
from yahoo_panoptes.framework.exceptions import PanoptesBaseException
from yahoo_panoptes.framework.context import PanoptesContext
from yahoo_panoptes.framework.resources import PanoptesResourcesKeyValueStore, PanoptesResourceCache
from yahoo_panoptes.framework.celery_manager import PanoptesCeleryConfig
from yahoo_panoptes.framework.plugins.helpers import expires, time_limit
from yahoo_panoptes.framework.plugins.manager import PanoptesPluginManager
from yahoo_panoptes.framework.plugins.panoptes_base_plugin import PanoptesPluginConfigurationError
from yahoo_panoptes.framework.plugins.scheduler import PanoptesPluginScheduler
from yahoo_panoptes.framework.utilities.helpers import get_calling_module_name, inspect_calling_module_for_name
from yahoo_panoptes.framework.utilities.key_value_store import PanoptesKeyValueStore
from yahoo_panoptes.enrichment.enrichment_plugin import PanoptesEnrichmentPlugin, PanoptesEnrichmentPluginInfo
from yahoo_panoptes.enrichment.enrichment_plugin_agent import PanoptesEnrichmentPluginAgentKeyValueStore

panoptes_context = None
enrichment_plugin_scheduler = None
celery = None
logger = None


class PanoptesEnrichmentPluginSchedulerError(PanoptesBaseException):
    """
    The exception class for Enrichment Plugin Scheduler errors
    """
    pass


class PanoptesEnrichmentSchedulerKeyValueStore(PanoptesKeyValueStore):
    """
    A custom Key/Value store for which uses the namespace demarcated for Enrichment Plugin Scheduler
    """

    def __init__(self, panoptes_context):
        """
        Initialize the underlying pipe.

        Args:
            self: (todo): write your description
            panoptes_context: (todo): write your description
        """
        super(PanoptesEnrichmentSchedulerKeyValueStore, self). \
            __init__(panoptes_context, const.ENRICHMENT_PLUGIN_SCHEDULER_KEY_VALUE_NAMESPACE)


class PanoptesEnrichmentPluginSchedulerContext(PanoptesContext):
    """
    This class implements a PanoptesContext with the following:
        A Key/Value Store for Enrichment Plugin Scheduler and Resource Manager
        A Zookeeper Client
    """

    def __init__(self):
        """
        Initialize the message store.

        Args:
            self: (todo): write your description
        """
        super(PanoptesEnrichmentPluginSchedulerContext, self).__init__(
            key_value_store_class_list=[PanoptesEnrichmentSchedulerKeyValueStore,
                                        PanoptesEnrichmentPluginAgentKeyValueStore,
                                        PanoptesResourcesKeyValueStore],
            create_message_producer=False, create_zookeeper_client=True)


class PanoptesCeleryEnrichmentAgentConfig(PanoptesCeleryConfig):
    task_routes = {const.ENRICHMENT_PLUGIN_AGENT_MODULE_NAME: {u'queue': const.ENRICHMENT_PLUGIN_AGENT_CELERY_APP_NAME}}

    def __init__(self):
        """
        Initialize the flask application.

        Args:
            self: (todo): write your description
        """
        super(PanoptesCeleryEnrichmentAgentConfig, self). \
            __init__(app_name=const.ENRICHMENT_PLUGIN_SCHEDULER_CELERY_APP_NAME)


def enrichment_plugin_scheduler_task(celery_beat_service, iteration_count=0):
    """
    This function is the workhorse of the Enrichment Plugin Scheduler module. It detects changes in plugins and their
    configuration and updates the Celery Beat schedule accordingly.

    Args:
        celery_beat_service (celery.beat.Service): The Celery Beat Service instance associated with this Plugin\
        Scheduler
        iteration_count (int): The number of times the scheduler task has been called. The count is tracked by the
        PanoptesTourOfDuty class inside of the PanoptesPluginScheduler class.
    Returns:
        None

    """
    logger.info(u'Timezone: %s' % str(celery_beat_service.app.timezone))
    start_time = time.time()

    try:
        resource_cache = PanoptesResourceCache(panoptes_context)
        resource_cache.setup_resource_cache()

    except Exception:
        logger.exception(u'Could not create resource cache, skipping cycle')
        return

    try:
        plugin_manager = PanoptesPluginManager(
            plugin_type=u'enrichment',
            plugin_class=PanoptesEnrichmentPlugin,
            plugin_info_class=PanoptesEnrichmentPluginInfo,
            panoptes_context=panoptes_context,
            kv_store_class=PanoptesEnrichmentPluginAgentKeyValueStore
        )
        plugins = plugin_manager.getPluginsOfCategory(category_name=u'enrichment')
        logger.info(u'Found %d plugins' % len(plugins))
    except:
        logger.exception('Error trying to load enrichment plugins, skipping cycle')
        return

    new_schedule = dict()

    for plugin in plugins:
        logger.info(u'Found plugin "%s", version %s at %s ' % (plugin.name, plugin.version, plugin.path))

        try:
            logger.info(u'Plugin "%s" has configuration: %s' % (plugin.name, plugin.config))
            logger.info(u'Plugin %s has plugin module time %s (UTC) and config mtime %s (UTC)' % (
                plugin.name, plugin.moduleMtime, plugin.configMtime))

            if plugin.execute_frequency <= 0:
                logger.info(u'Plugin %s has an invalid execution frequency (%d), skipping plugin' % (
                    plugin.name, plugin.execute_frequency))
                continue

            if not plugin.resource_filter:
                logger.info(u'Plugin "%s" does not have any resource filter specified, skipping plugin' %
                            plugin.name)
                continue
        except PanoptesPluginConfigurationError as e:
            logger.error(u'Error reading/parsing configuration for plugin "%s", skipping plugin. Error: %s' %
                         (plugin.name, repr(e)))

        try:
            resource_set = resource_cache.get_resources(plugin.resource_filter)
        except Exception as e:
            logger.info(u'Error in applying resource filter "%s" for plugin "%s", skipping plugin: %s' % (
                plugin.resource_filter, plugin.name, repr(e)))
            continue

        if len(resource_set) == 0:
            logger.info(
                u'No resources found for plugin "%s" after applying resource filter "%s", skipping plugin' % (
                    plugin.name, plugin.resource_filter))

        logger.info(u'Length of resource set {} for plugin {}'.format(len(resource_set), plugin.name))

        for resource in resource_set:
            logger.debug(u'Going to add task for plugin "%s" with execute frequency %d, args "%s", resources %s' % (
                plugin.name, plugin.execute_frequency, plugin.config, resource))

            plugin.data = resource

            task_name = u':'.join([plugin.normalized_name, plugin.signature, str(resource.resource_id)])

            new_schedule[task_name] = {
                u'task': const.ENRICHMENT_PLUGIN_AGENT_MODULE_NAME,
                u'schedule': timedelta(seconds=plugin.execute_frequency),
                u'last_run_at': datetime.utcfromtimestamp(plugin.last_executed),
                u'args': (plugin.name, resource.serialization_key),
                u'options': {
                    u'expires': expires(plugin),
                    u'time_limit': time_limit(plugin)
                }
            }

    resource_cache.close_resource_cache()

    logger.info(u'Going to unload plugin modules. Length of sys.modules before unloading modules: %d'
                % len(sys.modules))

    plugin_manager.unload_modules()
    logger.info(u'Unloaded plugin modules. Length of sys.modules after unloading modules: %d' % len(sys.modules))

    try:
        scheduler = celery_beat_service.scheduler
        scheduler.update(logger, new_schedule, called_by_panoptes=True)
        end_time = time.time()
        logger.info(u'Scheduled %d tasks in %.2fs' % (len(new_schedule), end_time - start_time))

    except:
        logger.exception(u'Error in updating schedule for Enrichment Plugins')

    logger.info(u'RSS memory: %dKB' % getrusage(RUSAGE_SELF).ru_maxrss)


def start_enrichment_plugin_scheduler():
    """
    The entry point for the Enrichment Plugin Scheduler

    This method creates a Panoptes Context and the Celery Instance for the Enrichment Plugin Scheduler

    Returns:
        None
    """
    global enrichment_plugin_scheduler, celery, logger, panoptes_context, resources_store

    try:
        panoptes_context = PanoptesEnrichmentPluginSchedulerContext()
    except Exception as e:
        sys.exit(u'Could not create a Panoptes Context: %s' % (repr(e)))

    try:
        celery_config = PanoptesCeleryEnrichmentAgentConfig()
    except Exception as e:
        sys.exit(u'Could not create a Celery Config object: %s' % repr(e))

    enrichment_plugin_scheduler = PanoptesPluginScheduler(
        panoptes_context=panoptes_context,
        plugin_type=u'enrichment',
        plugin_type_display_name=u'Enrichment',
        celery_config=celery_config,
        lock_timeout=const.ENRICHMENT_PLUGIN_SCHEDULER_LOCK_ACQUIRE_TIMEOUT,
        plugin_scheduler_task=enrichment_plugin_scheduler_task)

    logger = panoptes_context.logger
    celery = enrichment_plugin_scheduler.start()

    if not celery:
        sys.exit(u'Could not start Celery Beat Service')


@beat_init.connect
def celery_beat_service_started(sender=None, args=None, **kwargs):
    """
    This method is called after Celery Beat instantiates it's service

    Args:
        sender (celery.beat.Service): The Celery Beat Service which was started by Celery Beat
        args (dict): Arguments
        **kwargs (dict): Keyword Arguments

    Returns:
        None
    """
    global enrichment_plugin_scheduler
    sender.scheduler.panoptes_context = panoptes_context
    sender.scheduler.metadata_kv_store_class = PanoptesEnrichmentPluginAgentKeyValueStore
    sender.scheduler.task_prefix = const.ENRICHMENT_PLUGIN_SCHEDULER_CELERY_TASK_PREFIX

    try:
        enrichment_plugin_scheduler.run(sender, args, **kwargs)
    except Exception as e:
        sys.exit(u'Error while running plugin scheduler: %s' % repr(e))


"""
This wrapper is to ensure that the Enrichment Plugin Scheduler only executes when called from Celery - prevents against
execution when imported from other modules (like Sphinx) or called from the command line
"""
if get_calling_module_name() == const.CELERY_LOADER_MODULE or \
        inspect_calling_module_for_name('celery'):  # pragma: no cover
    faulthandler.enable()
    start_enrichment_plugin_scheduler()
