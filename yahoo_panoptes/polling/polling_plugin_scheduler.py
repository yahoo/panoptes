"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

The module implements the Polling Plugin Scheduler which finds any Discovery Plugins in the configured path and then
installs/updates a Celery Beat schedule for each discovery plugin and configuration combination.

If the list or the configuration of the plugins changes, the schedule is updated

This module is expected to be imported and executed though the Celery 'beat' command line tool

Internally, there are two threads that get setup: one is the main thread that runs the Celery Beat service. The other is
the thread started by the Polling Plugin Scheduler to detect and update plugin/configuration changes
"""
from builtins import str
import faulthandler
import sys
import time
from resource import getrusage, RUSAGE_SELF
from datetime import datetime, timedelta


from celery.signals import beat_init

from yahoo_panoptes.framework import const
from yahoo_panoptes.framework.exceptions import PanoptesBaseException
from yahoo_panoptes.framework.context import PanoptesContext
from yahoo_panoptes.framework.celery_manager import PanoptesCeleryConfig
from yahoo_panoptes.framework.resources import PanoptesResourcesKeyValueStore, PanoptesResourceCache
from yahoo_panoptes.framework.plugins.helpers import expires, time_limit
from yahoo_panoptes.framework.plugins.manager import PanoptesPluginManager
from yahoo_panoptes.framework.plugins.panoptes_base_plugin import PanoptesPluginConfigurationError
from yahoo_panoptes.framework.plugins.scheduler import PanoptesPluginScheduler
from yahoo_panoptes.framework.utilities.helpers import get_calling_module_name
from yahoo_panoptes.framework.utilities.key_value_store import PanoptesKeyValueStore
from yahoo_panoptes.polling.polling_plugin import PanoptesPollingPlugin, PanoptesPollingPluginInfo
from yahoo_panoptes.polling.polling_plugin_agent import PanoptesPollingPluginAgentKeyValueStore


panoptes_context = None
polling_plugin_scheduler = None
celery = None
logger = None


class PanoptesPollingPluginSchedulerError(PanoptesBaseException):
    """
    The exception class for Polling Plugin Scheduler errors
    """
    pass


class PanoptesPollingSchedulerKeyValueStore(PanoptesKeyValueStore):
    """
    A custom Key/Value store for which uses the namespace demarcated for Polling Plugin Scheduler
    """

    def __init__(self, panoptes_context):
        super(PanoptesPollingSchedulerKeyValueStore, self).__init__(panoptes_context,
                                                                    const.POLLING_PLUGIN_SCHEDULER_KEY_VALUE_NAMESPACE)


class PanoptesPollingPluginSchedulerContext(PanoptesContext):
    """
    This class implements a PanoptesContext with the following:
        A Key/Value Store for Polling Plugin Scheduler and Resource Manager
        A Zookeeper Client
    """

    def __init__(self):
        super(PanoptesPollingPluginSchedulerContext, self).__init__(
                key_value_store_class_list=[PanoptesPollingSchedulerKeyValueStore,
                                            PanoptesPollingPluginAgentKeyValueStore,
                                            PanoptesResourcesKeyValueStore],
                create_message_producer=False, create_zookeeper_client=True)


class PanoptesCeleryPollingAgentConfig(PanoptesCeleryConfig):
    task_routes = {const.POLLING_PLUGIN_AGENT_MODULE_NAME: {u'queue': const.POLLING_PLUGIN_AGENT_CELERY_APP_NAME}}

    def __init__(self):
        super(PanoptesCeleryPollingAgentConfig, self).__init__(app_name=const.POLLING_PLUGIN_SCHEDULER_CELERY_APP_NAME)


def polling_plugin_scheduler_task(celery_beat_service):
    """
    This function is the workhorse of the Polling Plugin Scheduler module. It detects changes in plugins and their
    configuration and updates the Celery Beat schedule accordingly.

    Args:
        celery_beat_service (celery.beat.Service): The Celery Beat Service instance associated with this Plugin\
        Scheduler

    Returns:
        None

    """
    start_time = time.time()

    try:
        resource_cache = PanoptesResourceCache(panoptes_context)
        resource_cache.setup_resource_cache()
    except:
        logger.exception(u'Could not create resource cache, skipping cycle')
        return

    try:
        plugin_manager = PanoptesPluginManager(
            plugin_type=u'polling',
            plugin_class=PanoptesPollingPlugin,
            plugin_info_class=PanoptesPollingPluginInfo,
            panoptes_context=panoptes_context,
            kv_store_class=PanoptesPollingPluginAgentKeyValueStore
        )
        plugins = plugin_manager.getPluginsOfCategory(category_name=u'polling')
        logger.info(u'Found %d plugins' % len(plugins))
    except:
        logger.exception(u'Error trying to load Polling plugins, skipping cycle')
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
                u'task': const.POLLING_PLUGIN_AGENT_MODULE_NAME,
                u'schedule': timedelta(seconds=plugin.execute_frequency),
                u'args': (plugin.name, resource.serialization_key),
                u'last_run_at': datetime.utcfromtimestamp(plugin.last_executed),
                u'options': {
                    u'expires': expires(plugin),
                    u'time_limit': time_limit(plugin)
                }
            }

    resource_cache.close_resource_cache()

    logger.info('Going to unload plugin modules. Length of sys.modules before unloading modules: %d' % len(sys.modules))
    plugin_manager.unload_modules()
    logger.info('Unloaded plugin modules. Length of sys.modules after unloading modules: %d' % len(sys.modules))

    try:
        scheduler = celery_beat_service.scheduler
        scheduler.update(logger, new_schedule)

        end_time = time.time()
        logger.info(u'Scheduled %d tasks in %.2fs' % (len(new_schedule), end_time - start_time))
        logger.info(u'RSS memory: %dKB' % getrusage(RUSAGE_SELF).ru_maxrss)

    except:
        logger.exception(u'Error in updating schedule for Polling Plugins')

    logger.info('RSS memory: %dKB' % getrusage(RUSAGE_SELF).ru_maxrss)


def start_polling_plugin_scheduler():
    """
    The entry point for the Polling Plugin Scheduler

    This method creates a Panoptes Context and the Celery Instance for the Polling Plugin Scheduler

    Returns:
        None
    """
    global polling_plugin_scheduler, celery, logger, panoptes_context

    try:
        panoptes_context = PanoptesPollingPluginSchedulerContext()
    except Exception as e:
        sys.exit(u'Could not create a Panoptes Context: %s' % (repr(e)))

    logger = panoptes_context.logger

    try:
        celery_config = PanoptesCeleryPollingAgentConfig()
    except Exception as e:
        sys.exit(u'Could not create a Celery Config object: %s' % repr(e))

    try:
        polling_plugin_scheduler = PanoptesPluginScheduler(
            panoptes_context=panoptes_context, plugin_type=u'polling',
            plugin_type_display_name=u'Polling',
            celery_config=celery_config,
            lock_timeout=const.POLLING_PLUGIN_SCHEDULER_LOCK_ACQUIRE_TIMEOUT,
            plugin_scheduler_task=polling_plugin_scheduler_task,
            plugin_subtype=panoptes_context.config_dict[u'polling'][u'plugins_subtype']
        )
    except Exception as e:
        sys.exit(u'Could not create a Plugin Scheduler object: %s' % repr(e))

    try:
        celery = polling_plugin_scheduler.start()
    except Exception as e:
        sys.exit(u'Could not start the Plugin Scheduler object: %s' % repr(e))

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
    global polling_plugin_scheduler
    sender.scheduler.panoptes_context = panoptes_context
    sender.scheduler.task_prefix = const.POLLING_PLUGIN_SCHEDULER_CELERY_TASK_PREFIX

    try:
        polling_plugin_scheduler.run(sender, args, **kwargs)
    except Exception as e:
        sys.exit(u'Error while running plugin scheduler: %s' % repr(e))


"""
This wrapper is to ensure that the Polling Plugin Scheduler only executes when called from Celery - prevents against
execution when imported from other modules (like Sphinx) or called from the command line
"""
if get_calling_module_name() == const.CELERY_LOADER_MODULE:  # pragma: no cover
    faulthandler.enable()
    start_polling_plugin_scheduler()
