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
from datetime import timedelta

from celery.signals import beat_init

from ..framework import const
from ..framework.context import PanoptesContext
from ..framework.celery_manager import PanoptesCeleryConfig
from ..framework.plugins.helpers import expires, time_limit
from ..framework.plugins.manager import PanoptesPluginManager
from ..framework.plugins.panoptes_base_plugin import PanoptesPluginConfigurationError, \
    PanoptesPluginInfo
from ..framework.plugins.scheduler import PanoptesPluginScheduler
from ..framework.utilities.helpers import get_calling_module_name
from ..framework.utilities.key_value_store import PanoptesKeyValueStore
from .panoptes_discovery_plugin import PanoptesDiscoveryPlugin

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
                key_value_store_class_list=[PanoptesDiscoveryPluginSchedulerKeyValueStore],
                create_message_producer=False, create_zookeeper_client=True)


class PanoptesCeleryDiscoveryAgentConfig(PanoptesCeleryConfig):
    task_routes = {const.DISCOVERY_PLUGIN_AGENT_MODULE_NAME: {'queue': const.DISCOVERY_PLUGIN_AGENT_CELERY_APP_NAME}}

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
        plugin_manager = PanoptesPluginManager(plugin_type='discovery',
                                               plugin_class=PanoptesDiscoveryPlugin,
                                               plugin_info_class=PanoptesPluginInfo,
                                               panoptes_context=panoptes_context,
                                               kv_store_class=PanoptesDiscoveryPluginSchedulerKeyValueStore)
        plugins = plugin_manager.getPluginsOfCategory(category_name='discovery')
    except:
        logger.exception('Error trying to load Discovery plugins, skipping cycle')
        return

    new_schedule = dict()

    for plugin in plugins:
        logger.info('Found plugin "%s", version %s at %s ' % (plugin.name, plugin.version, plugin.path))

        try:
            logger.info('Plugin "%s" has configuration: %s' % (plugin.name, plugin.config))
            logger.info('Plugin %s has plugin module time %s (UTC) and config mtime %s (UTC)' % (
                plugin.name, plugin.moduleMtime, plugin.configMtime))

            if plugin.execute_frequency <= 0:
                logger.info('Plugin %s has an invalid execution frequency (%d), skipping plugin' % (
                    plugin.name, plugin.execute_frequency))
                continue
        except PanoptesPluginConfigurationError as e:
            logger.error('Error reading/parsing configuration for plugin %s, skipping plugin. Error: %s' %
                         (plugin.name, repr(e)))

        logger.debug('Going to add task for plugin "%s" with execute frequency %d and args "%s"' % (
            plugin.name, plugin.execute_frequency, plugin.config))
        task_name = ':'.join([plugin.normalized_name, plugin.signature])
        new_schedule[task_name] = {
            'task': const.DISCOVERY_PLUGIN_AGENT_MODULE_NAME,
            'schedule': timedelta(seconds=plugin.execute_frequency),
            'args': (plugin.name,),
            'options': {
                'expires': expires(plugin),
                'time_limit': time_limit(plugin)
            }
        }

    end_time = time.time()

    try:
        scheduler = celery_beat_service.scheduler
        scheduler.update(logger, new_schedule)
        logger.info('Scheduled %d tasks in %.2fs' % (len(new_schedule), end_time - start_time))
    except:
        logger.exception('Error in updating schedule for Polling Plugins')


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
        sys.exit('Could not create a Panoptes Context: %s' % (repr(e)))

    try:
        celery_config = PanoptesCeleryDiscoveryAgentConfig()
    except Exception as e:
        sys.exit('Could not create a Celery Config object: %s' % repr(e))

    discovery_plugin_scheduler = PanoptesPluginScheduler(panoptes_context, 'discovery', 'Discovery',
                                                         celery_config,
                                                         const.DISCOVERY_PLUGIN_SCHEDULER_LOCK_ACQUIRE_TIMEOUT,
                                                         discovery_plugin_scheduler_task)
    logger = panoptes_context.logger
    celery = discovery_plugin_scheduler.start()

    if not celery:
        sys.exit('Could not start Celery Beat Service')


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
    sender.scheduler.panoptes_context = panoptes_context
    sender.scheduler.task_prefix = const.DISCOVERY_PLUGIN_SCHEDULER_CELERY_TASK_PREFIX
    discovery_plugin_scheduler.run(sender, args, **kwargs)


# This wrapper is to ensure that the Discovery Plugin Scheduler only executes when called from Celery - prevents against
# execution when imported from other modules (like Sphinx) or called from the command line

if get_calling_module_name() == const.CELERY_LOADER_MODULE:
    faulthandler.enable()
    start_discovery_plugin_scheduler()
