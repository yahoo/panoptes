"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements the polling Plugin Agent which accepts plugin names as Celery Task parameters and executes
them. Results, if any, returned by each plugin are expected to be PanoptesMetricSets. If the returned Metrics Set is
not empty, each metric from the metric set is placed on a Kafka queue named 'metric'

This module is expected to be imported and executed though the Celery 'worker' command line tool
"""
import faulthandler
import sys

from celery import task
from celery.signals import worker_process_shutdown

from ..framework import const
from ..framework.context import PanoptesContext
from ..framework.exceptions import PanoptesBaseException
from ..framework.celery_manager import PanoptesCeleryConfig, PanoptesCeleryInstance
from ..framework.metrics import PanoptesMetricType, PanoptesMetric, PanoptesMetricsGroup, \
    PanoptesMetricsGroupSet
from ..framework.resources import PanoptesResourceStore, PanoptesResourcesKeyValueStore
from ..framework.plugins.panoptes_base_plugin import PanoptesPluginInfo
from ..framework.plugins.runner import PanoptesPluginWithEnrichmentRunner
from ..framework.utilities.helpers import get_calling_module_name
from ..framework.utilities.key_value_store import PanoptesKeyValueStore
from ..framework.utilities.secrets import PanoptesSecretsStore
from .polling_plugin import PanoptesPollingPlugin

panoptes_context = None
panoptes_polling_task_context = None
celery = None


class PanoptesMetricsKeyValueStore(PanoptesKeyValueStore):
    """Class to create key value store for metrics processing (particularly data to calculate gauges)

    Args:
        panoptes_context (PanoptesContext): The metrics panoptes context
    """
    redis_group = const.METRICS_REDIS_GROUP

    def __init__(self, panoptes_context):
        super(PanoptesMetricsKeyValueStore, self).__init__(panoptes_context, const.METRICS_KEY_VALUE_NAMESPACE)


class PanoptesPollingPluginAgentKeyValueStore(PanoptesKeyValueStore):
    """
    A custom Key/Value store for the Polling Plugin Agent which store data like a plugin's last execution time and \
    the last result production time
    """

    def __init__(self, panoptes_context):
        super(PanoptesPollingPluginAgentKeyValueStore, self).__init__(panoptes_context,
                                                                      const.POLLING_PLUGIN_AGENT_KEY_VALUE_NAMESPACE)


class PanoptesPollingPluginKeyValueStore(PanoptesKeyValueStore):
    """
    A custom Key/Value store for the Panoptes Polling plugins

    Plugins executed by the Polling Plugin Agent get access to this store and can get/set values they need for their\
    operation
    """

    def __init__(self, panoptes_context):
        super(PanoptesPollingPluginKeyValueStore, self).__init__(panoptes_context, const.PLUGINS_KEY_VALUE_NAMESPACE)


class PanoptesPollingAgentContext(PanoptesContext):
    """
    This class implements a PanoptesContext with no clients - only used for setting up the Celery app
    """

    def __init__(self):
        super(PanoptesPollingAgentContext, self).__init__(key_value_store_class_list=[],
                                                          create_message_producer=False,
                                                          create_zookeeper_client=False)


class PanoptesPollingTaskContext(PanoptesContext):
    """
    This class implements a PanoptesContext with the following:
        - A Key/Value Store for Polling Plugin Agent
        - A Key/Value Store for Panoptes Plugins
        - A Asynchronous Message Producer
        - A Zookeeper Client
    """

    def __init__(self):
        super(PanoptesPollingTaskContext, self).__init__(
                key_value_store_class_list=[PanoptesPollingPluginAgentKeyValueStore,
                                            PanoptesPollingPluginKeyValueStore,
                                            PanoptesSecretsStore,
                                            PanoptesResourcesKeyValueStore,
                                            PanoptesMetricsKeyValueStore],
                create_message_producer=True, async_message_producer=True, create_zookeeper_client=True)


class PanoptesPollingPluginAgentError(PanoptesBaseException):
    """
    The exception class for Polling Plugin Agent runtime errors
    """
    pass


@task
def polling_plugin_task(polling_plugin_name, resource_key):
    """
    The main method of the Polling Plugin Agent

    This method, called by Celery, loads and executes the specified plugin. The workflow is as follows:
        - Locate and load plugin through `yapsy's <http://yapsy.sourceforge.net/>`_ plugin manager
        - Evaluate if the plugin should be executed right now
        - If yes, then create a plugin context
        - Attempt to get a lock for the instance of the plugin (unique combination of the plugin name and configuration)
        - If the lock is acquired, then attempt to execute the plugin
        - If the plugin executes successfully, update the timestamp in the Polling Plugin Agent Key/Value store
        - If the plugin produced a non-zero result set, send the resource over the message bus
        - If sending over the message succeeds, update the timestamp in the Polling Plugin Agent Key/Value store

    Args:
        polling_plugin_name (str): The name of the plugin to be executed
        resource_key (dict): The dictionary that contains the resource key of the resource this polling plugin should \
        monitor

    Returns:
        None
    """
    global panoptes_polling_task_context

    if panoptes_polling_task_context is None:
        try:
            panoptes_polling_task_context = PanoptesPollingTaskContext()
        except Exception as e:
            sys.exit('Could not create a Panoptes Polling Task Context: %s' % (repr(e)))

    logger = panoptes_polling_task_context.logger

    try:
        resource = PanoptesResourceStore(panoptes_polling_task_context).get_resource(resource_key)
        plugin_runner = PanoptesPluginWithEnrichmentRunner(
                plugin_name=polling_plugin_name,
                plugin_type='polling',
                plugin_class=PanoptesPollingPlugin,
                plugin_info_class=PanoptesPluginInfo,
                plugin_data=resource,
                panoptes_context=panoptes_polling_task_context,
                plugin_agent_kv_store_class=PanoptesPollingPluginAgentKeyValueStore,
                plugin_kv_store_class=PanoptesPollingPluginKeyValueStore,
                plugin_secrets_store_class=PanoptesSecretsStore,
                plugin_logger_name=const.POLLING_PLUGIN_AGENT_PLUGINS_CHILD_LOGGER_NAME,
                plugin_result_class=PanoptesMetricsGroupSet,
                results_callback=_process_metrics_group_set
        )
        plugin_runner.execute_plugin()
    except Exception as e:
        logger.error('[%s] Error executing plugin: %s' % (polling_plugin_name, str(e)))


def _make_key(metrics_group):
    """
    Returns a key that is based on the resource id, metrics group type and dimension names and values

    Args:
        metrics_group (PanoptesMetricsGroup): The PanoptesMetricsGroup to generate the key for

    Returns:
        str: The KV_STORE_DELIMITER delimited key
    """
    dimensions = const.KV_STORE_DELIMITER.join(
            [const.KV_STORE_DELIMITER.join([dimension.name, dimension.value]) for dimension in
             metrics_group.dimensions])

    return const.KV_STORE_DELIMITER.join([metrics_group.resource.resource_id,
                                          metrics_group.group_type,
                                          dimensions])


def _split_and_strip(values, delimiter=','):
    return [value.strip() for value in values.split(delimiter)]


def _transformation_rate(context, metrics_group, inputs):
    kv_store = context.get_kv_store(PanoptesMetricsKeyValueStore)
    logger = context.logger

    output_metrics_group = metrics_group.copy()

    for metric in metrics_group.metrics:
        if metric.metric_name in inputs:
            key = const.KV_STORE_DELIMITER.join([_make_key(metrics_group), metric.metric_name])

            try:
                new_stored_value = const.KV_STORE_DELIMITER.join([str(metric.metric_value),
                                                                  str(metric.metric_timestamp)])
                stored_value = kv_store.getset(key, new_stored_value,
                                               const.METRICS_KV_STORE_TTL_MULTIPLE * metrics_group.interval)
            except Exception as e:
                context.logger.error(
                        'Error trying to fetch/store/convert for key "%s": %s, skipping conversion' % (key, repr(e)))
                continue

            if stored_value is None:
                logger.debug('Could not find existing value for key "%s", skipping conversion' % key)
                continue

            logger.debug('Calculating rate for %s' % key)

            value, timestamp = stored_value.split(const.KV_STORE_DELIMITER)
            time_difference = metric.metric_timestamp - float(timestamp)

            if time_difference < 0:
                logger.debug('Time difference is negative for key "%s": (%.2f), skipping conversion' %
                             (key, time_difference))
                continue
            elif time_difference == 0:
                logger.debug('Time difference is zero for key "%s", skipping conversion' % key)
                continue
            elif time_difference > (metrics_group.interval * const.METRICS_KV_STORE_TTL_MULTIPLE):
                logger.debug('Time difference is greater than TTL multiple for key "%s": (%.2f), skipping conversion' %
                             (key, time_difference))
                continue

            confidence = round(metrics_group.interval / time_difference, 2)
            if confidence < const.METRICS_CONFIDENCE_THRESHOLD:
                logger.warn('Confidence for key "%s" is %.2f, which is below the threshold of %.2f' %
                            (key, confidence, const.METRICS_CONFIDENCE_THRESHOLD))

            value = float(value)
            counter_difference = metric.metric_value - value

            if counter_difference >= 0:
                rate = int(counter_difference / time_difference)
                logger.debug('Rate for %s is %d' % (key, rate))

                try:
                    output_metrics_group.add_metric(PanoptesMetric(metric.metric_name, rate, PanoptesMetricType.GAUGE))
                except KeyError:
                    logger.warn('Metric %s already present as gauge in %s, skipping' % (metric.metric_name,
                                                                                        metrics_group.type))
            else:
                logger.debug('New counter value (%.2f) is less than current counter (%.2f) for key "%s": (%.2f), '
                             'skipping conversion' % (metric.metric_value, value, key, counter_difference))

    return output_metrics_group


def _process_transforms(context, transforms, metrics_group_set):
    """
    {
     "member": [{"rate": ["real_bytes_in",
                          "real_bytes_out",
                          "real_packets_in",
                          "real_packets_out",
                          "real_total_connections"]}]
    }

    Args:
        context (PanoptesContext): The PanoptesContext being used by the Plugin Agent
        transforms (dict): The transformations to apply
        metrics_group_set (PanoptesMetricsGroupSet): The metrics group set on which to apply the transformations

    Returns:
        PanoptesMetricsGroupSet: The processed/transformed metrics group set
    """
    callbacks = {'rate': _transformation_rate}

    logger = context.logger
    lookup = dict()
    output_metrics_group_set = PanoptesMetricsGroupSet()

    logger.debug('Going to process transforms: %s' % transforms)

    for key in transforms:
        transform_type, transform_metrics_group_type, transform_inputs = transforms[key].split(':')
        transform_inputs = _split_and_strip(transform_inputs)
        if transform_metrics_group_type not in lookup:
            lookup[transform_metrics_group_type] = list()

            lookup[transform_metrics_group_type].append((transform_type, transform_inputs))

    logger.debug('Transform lookups: %s' % lookup)

    for metrics_group in metrics_group_set:
        if metrics_group.group_type not in lookup:
            output_metrics_group_set.add(metrics_group)
            continue

        resource_serialization_key = metrics_group.resource.serialization_key

        for transform in lookup[metrics_group.group_type]:
            logger.debug(
                    'For resource %s, trying to process transform %s' % (resource_serialization_key, transform))
            transform_type, transform_inputs = transform

            if transform_type not in callbacks:
                logger.warn('For resource %s, no implementation for transform type "%s" found, skipping' % (
                    resource_serialization_key, transform))
                continue

            try:
                output_metrics_group = callbacks[transform_type](context, metrics_group, transform_inputs)
                if output_metrics_group is not None:
                    output_metrics_group_set.add(output_metrics_group)
            except Exception as e:
                logger.error('For resource %s, error while trying to transform metrics group "%s": %s, skipping' % (
                    resource_serialization_key, metrics_group.group_type, repr(e)))

    return output_metrics_group_set


def _send_metrics_group_set(context, metrics_group_set, topic_suffixes):
    """
    Emits the provided metrics group set to the message bus

    Args:
        context (PanoptesContext): The PanoptesContext being used by the Plugin Agent
        topic_suffixes (list): The list of suffixes to add to the topic to emit the metrics to
        metrics_group_set (PanoptesMetricsGroupSet): The metrics group set to emit

    Returns:
        None
    """
    producer = context.message_producer

    for metrics_group in metrics_group_set:
        key = const.METRICS_TOPIC_KEY_DELIMITER.join([metrics_group.resource.resource_class,
                                                      metrics_group.resource.resource_subclass,
                                                      metrics_group.resource.resource_type
                                                      ])
        partitioning_key = _make_key(metrics_group)

        for topic_suffix in set(topic_suffixes):
            topic = const.METRICS_TOPIC_NAME_DELIMITER.join([str(metrics_group.resource.resource_site), topic_suffix])
            context.logger.debug(
                    'Going to send metric group "%s" to topic "%s" with key "%s" and partitioning key "%s" ' % (
                        metrics_group.json, topic, key, partitioning_key))

            producer.send_messages(topic=topic, key=key, messages=metrics_group.json, partitioning_key=partitioning_key)

            context.logger.debug(
                    'Sent metric group "%s" to topic "%s" with key "%s" and partitioning key "%s" ' % (
                        metrics_group.json, topic, key, partitioning_key))


def _process_metrics_group_set(context, results, plugin):
    """
    Processes each metrics group in the result set - emits to the message bus, applies transforms and emits the \
    transformed results to the 'processed' topic on the message bus

    Args:
        context (PanoptesContext): The PanoptesContext being used by the Plugin Agent
        results (PanoptesMetricsGroupSet): The PanoptesMetricsGroupSet returned by the plugin
        plugin (PanoptesPollingPlugin): The Panoptes plugin object for the plugin that returned this dataset

    Returns:
        None
    """
    raw_metrics_topics_suffixes = [const.METRICS_RAW_TOPIC_SUFFIX]
    processed_metrics_topics_suffixes = [const.METRICS_PROCESSED_TOPIC_SUFFIX]

    if 'topics' in plugin.config:
        if 'raw' in plugin.config['topics']:
            raw_metrics_topics_suffixes.extend(_split_and_strip(plugin.config['topics']['raw']))
        if 'processed' in plugin.config['topics']:
            processed_metrics_topics_suffixes.extend(_split_and_strip(plugin.config['topics']['processed']))

    # Send to the 'raw' metrics topic
    _send_metrics_group_set(context, results, raw_metrics_topics_suffixes)

    # Applies transforms
    if 'transforms' in plugin.config:
        results = _process_transforms(context, plugin.config['transforms'], results)
        # Send to the 'processed' topic. Note - if no transforms have been applied,
        # then is essentially a copy of the 'raw' metrics
        _send_metrics_group_set(context, results, processed_metrics_topics_suffixes)


@worker_process_shutdown.connect()
def shutdown_signal_handler(sender, args=None, **kwargs):
    """
    This method handles the shutdown signal from Celery

    Args:
        sender (Celery): The Celery app which sent the shutdown signal
        args (dict): Unused
        **kwargs (dict): Unused

    Returns:
        None
    """
    if panoptes_polling_task_context is not None:
        print('Polling Plugin Agent shutting down - going to stop message producer')
        try:
            panoptes_polling_task_context.message_producer.stop()
        except Exception as e:
            print('Could not shutdown message producer: %s' % repr(e))


def start_polling_plugin_agent():
    """
    The entry point for the Polling Plugin Agent

    This method creates a Panoptes Context and the Celery Instance for the Polling Plugin Agent

    Returns:
        None
    """
    global panoptes_context, celery

    try:
        panoptes_context = PanoptesPollingAgentContext()
    except Exception as e:
        sys.exit('Could not create a Panoptes Context: %s' % (str(e)))

    logger = panoptes_context.logger
    logger.info('Attempting to start Celery application')

    celery_config = PanoptesCeleryConfig(const.POLLING_PLUGIN_AGENT_CELERY_APP_NAME)

    try:
        celery = PanoptesCeleryInstance(panoptes_context, celery_config).celery
    except Exception as exp:
        sys.exit('Could not instantiate Celery application: %s' % str(exp))
    else:
        logger.info('Started Celery application: %s' % celery)


if get_calling_module_name() == const.CELERY_LOADER_MODULE:
    faulthandler.enable()
    start_polling_plugin_agent()
