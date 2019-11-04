"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

The module implements the Discovery Plugin Scheduler which finds any Discovery Plugins in the configured path and then
installs/updates a Celery Beat schedule for each discovery plugin and configuration combination.

If the list or the configuration of the plugins changes, the schedule is updated

This module is expected to be imported and executed though the Celery 'beat' command line tool

Internally, there are two threads that get setup: one is the main thread that runs the Celery Beat service. The other is
the thread started by the Discovery Plugin Scheduler to detect and update plugin/configuration changes
"""
import faulthandler
import sys
import time
from resource import getrusage, RUSAGE_SELF
from datetime import datetime, timedelta

from celery.signals import beat_init

from yahoo_panoptes.framework import const
from yahoo_panoptes.framework.context import PanoptesContext
from yahoo_panoptes.framework.celery_manager import PanoptesCeleryConfig
from yahoo_panoptes.framework.plugins.helpers import expires, time_limit
from yahoo_panoptes.framework.plugins.manager import PanoptesPluginManager
from yahoo_panoptes.framework.plugins.panoptes_base_plugin import PanoptesPluginConfigurationError, \
    PanoptesPluginInfo
from yahoo_panoptes.framework.plugins.scheduler import PanoptesPluginScheduler
from yahoo_panoptes.framework.utilities.helpers import get_calling_module_name
from yahoo_panoptes.framework.utilities.key_value_store import PanoptesKeyValueStore
from yahoo_panoptes.discovery.panoptes_discovery_plugin import PanoptesDiscoveryPlugin
from yahoo_panoptes.discovery.discovery_plugin_agent import PanoptesDiscoveryPluginAgentKeyValueStore

panoptes_context = None
discovery_plugin_scheduler = None
celery = None
logger = None


class PanoptesDiscoveryPluginSchedulerKeyValueStore(PanoptesKeyValueStore):
    """
    A custom Key/Value store for the Discovery Plugin Scheduler which uses the namespace demarcated for Discovery
    Plugin Scheduler
    """

    def __init__(self, context):
        super(PanoptesDiscoveryPluginSchedulerKeyValueStore, self).__init__(
            context, const.DISCOVERY_PLUGIN_SCHEDULER_KEY_VALUE_NAMESPACE)


class PanoptesDiscoveryPluginSchedulerContext(PanoptesContext):
    """
    This class implements a PanoptesContext with the following:
        A Key/Value Store for Discovery Plugin Scheduler
        A Zookeeper Client
    """

    def __init__(self):
        super(PanoptesDiscoveryPluginSchedulerContext, self).__init__(
            key_value_store_class_list=[PanoptesDiscoveryPluginSchedulerKeyValueStore,
                                        PanoptesDiscoveryPluginAgentKeyValueStore],
            create_message_producer=False, create_zookeeper_client=True)


class PanoptesCeleryDiscoveryAgentConfig(PanoptesCeleryConfig):
    task_routes = {const.DISCOVERY_PLUGIN_AGENT_MODULE_NAME: {u'queue': const.DISCOVERY_PLUGIN_AGENT_CELERY_APP_NAME}}

    def __init__(self):
        super(PanoptesCeleryDiscoveryAgentConfig, self).__init__(
            app_name=const.DISCOVERY_PLUGIN_SCHEDULER_CELERY_APP_NAME)


def discovery_plugin_scheduler_task(celery_beat_service):
    """
    This function is the workhorse of the Discovery Plugin Scheduler module. It detects changes in plugins and their
    configuration and updates the Celery Beat schedule accordingly.

    Args:
        celery_beat_service (celery.beat.Service): The Celery Beat Service object associated with this Plugin Scheduler

    Returns:
        None
    """

    start_time = time.time()

    try:
        plugin_manager = PanoptesPluginManager(
            plugin_type=u'discovery',
            plugin_class=PanoptesDiscoveryPlugin,
            plugin_info_class=PanoptesPluginInfo,
            panoptes_context=panoptes_context,
            kv_store_class=PanoptesDiscoveryPluginAgentKeyValueStore
        )
        plugins = plugin_manager.getPluginsOfCategory(category_name=u'discovery')
    except:
        logger.exception(u'Error trying to load Discovery plugins, skipping cycle')
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
        except PanoptesPluginConfigurationError as e:
            logger.error(u'Error reading/parsing configuration for plugin %s, skipping plugin. Error: %s' %
                         (plugin.name, repr(e)))

        logger.debug(u'Going to add task for plugin "%s" with execute frequency %d and args "%s"' % (
            plugin.name, plugin.execute_frequency, plugin.config))

        task_name = u':'.join([plugin.normalized_name, plugin.signature])

        new_schedule[task_name] = {
            u'task': const.DISCOVERY_PLUGIN_AGENT_MODULE_NAME,
            u'schedule': timedelta(seconds=plugin.execute_frequency),
            u'last_run_at': datetime.utcfromtimestamp(plugin.last_executed),
            u'args': (plugin.name,),
            u'options': {
                u'expires': expires(plugin),
                u'time_limit': time_limit(plugin)
            }
        }

    logger.info(u'Going to unload plugin modules. Length of sys.modules before unloading modules: %d'
                % len(sys.modules))

    plugin_manager.unload_modules()
    logger.info(u'Unloaded plugin modules. Length of sys.modules after unloading modules: %d' % len(sys.modules))

    try:
        scheduler = celery_beat_service.scheduler
        scheduler.update(logger, new_schedule)

        end_time = time.time()
        logger.info(u'Scheduled %d tasks in %.2fs' % (len(new_schedule), end_time - start_time))
    except:
        logger.exception(u'Error in updating schedule for Discovery Plugins')

    logger.info(u'RSS memory: %dKB' % getrusage(RUSAGE_SELF).ru_maxrss)


def start_discovery_plugin_scheduler():
    """
    The entry point for the Discovery Plugin Scheduler

    This method creates a Panoptes Context and the Celery Instance for the Discovery Plugin Scheduler

    Returns:
        None
    """
    global discovery_plugin_scheduler, celery, logger, panoptes_context

    try:
        panoptes_context = PanoptesDiscoveryPluginSchedulerContext()
    except Exception as e:
        sys.exit(u'Could not create a Panoptes Context: %s' % (repr(e)))

    try:
        celery_config = PanoptesCeleryDiscoveryAgentConfig()
    except Exception as e:
        sys.exit(u'Could not create a Celery Config object: %s' % repr(e))

    discovery_plugin_scheduler = PanoptesPluginScheduler(panoptes_context, u'discovery', u'Discovery',
                                                         celery_config,
                                                         const.DISCOVERY_PLUGIN_SCHEDULER_LOCK_ACQUIRE_TIMEOUT,
                                                         discovery_plugin_scheduler_task)
    logger = panoptes_context.logger
    celery = discovery_plugin_scheduler.start()

    if not celery:
        sys.exit(u'Could not start Celery Beat Service')


@beat_init.connect
def celery_beat_service_started(sender=None, args=None, **kwargs):
    """
    This method is called after Celery Beat instantiates its service

    Args:
        sender (celery.beat.Service): The Celery Beat Service which was started by Celery Beat
        args (dict): Arguments
        **kwargs (dict): Keyword Arguments

    Returns:
        None
    """
    sender.scheduler.panoptes_context = panoptes_context
    sender.scheduler.task_prefix = const.DISCOVERY_PLUGIN_SCHEDULER_CELERY_TASK_PREFIX
    discovery_plugin_scheduler.run(sender, args, **kwargs)


# This wrapper is to ensure that the Discovery Plugin Scheduler only executes when called from Celery - prevents against
# execution when imported from other modules (like Sphinx) or called from the command line

if get_calling_module_name() == const.CELERY_LOADER_MODULE:  # pragma: no cover
    faulthandler.enable()
    start_discovery_plugin_scheduler()
