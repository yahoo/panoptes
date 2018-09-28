"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements a Resource Manager which reads the discovered resources and add/deletes/updates them in the
Panoptes Resource Store (currently Redis)
"""
import signal
import sys
import time

from ..framework import const
from ..framework.context import PanoptesContext
from ..framework.resources import PanoptesResource, PanoptesResourceSet, PanoptesResourceStore, \
    PanoptesResourcesKeyValueStore
from ..framework.utilities.consumer import PanoptesResourcesConsumer
from ..framework.utilities.helpers import get_client_id

panoptes_context = logger = consumer = resource_store = None


class PanoptesResourceManagerContext(PanoptesContext):
    def __init__(self):
        super(PanoptesResourceManagerContext, self).__init__(
                key_value_store_class_list=[PanoptesResourcesKeyValueStore],
                create_message_producer=False, async_message_producer=False, create_zookeeper_client=True)


def _resource_set_to_dictionary(resource_set):
    """
    Takes a PanoptesResourceSet and returns a dictionary keyed by serialization key of each resource. This is hack since
    Python has no easy way to get objects from sets

    @TODO: This is hack and should be replaced

    Args:
        resource_set (PanoptesResourceSet): The PanoptesResourceSet to convert

    Returns:
        dict: A dictionary which is : {<resource serialization key>: <resource object>]
    """
    return {resource.serialization_key: resource for resource in resource_set}


def handle_resources(plugin_signature, resources):
    resource_site = str(resources['resources'][0]['resource_site'])

    plugin_name = str(plugin_signature.split(':')[0])

    logger.info('For plugin "%s" and site "%s", going to get current set' % (plugin_name, resource_site))

    try:
        current_resource_set = resource_store.get_resources(site=resource_site, plugin_name=plugin_name)
    except:
        logger.exception('Error trying to get current resources for plugin "%s"' % plugin_signature)
        return False

    logger.info('For plugin "%s" and site "%s" current set has %d resources' % (
        plugin_signature, resource_site, len(current_resource_set)))

    new_resource_set = PanoptesResourceSet()

    for resource in resources['resources']:
        new_resource = PanoptesResource.resource_from_dict(resource)
        new_resource_set.add(new_resource)

    current_resource_dict = _resource_set_to_dictionary(current_resource_set)
    new_resource_dict = _resource_set_to_dictionary(new_resource_set)

    logger.info('For plugin "%s" and site "%s", new set has %d resources' % (
        plugin_signature, resource_site, len(new_resource_set)))

    resources_to_delete = current_resource_set.resources.difference(new_resource_set)

    logger.info('For plugin "%s" and site "%s", deleting %d resources' % (
        plugin_signature, resource_site, len(resources_to_delete)))

    for resource in resources_to_delete:
        current_timestamp = current_resource_dict[resource.serialization_key].resource_creation_timestamp
        new_timestamp = resources['resource_set_creation_timestamp']
        if current_timestamp > new_timestamp:
            logger.info('For plugin "%s" and site "%s", resource "%s" has timestamp (%0.2f UTC) greater than of '
                        'new set (%0.2f UTC), skipping deletion' % (
                            plugin_signature, resource_site, str(resource), current_timestamp, new_timestamp))

        else:
            logger.info('For plugin "%s" and site "%s", going to delete resource "%s"' % (
                plugin_signature, resource_site, resource))
            try:
                resource_store.delete_resource(plugin_signature, resource)
            except:
                logger.exception('Error trying to delete resource')
                return False

    resources_to_add = new_resource_set.resources.difference(current_resource_set)

    logger.info('For plugin "%s" and site "%s", adding %d resources' %
                (plugin_signature, resource_site, len(resources_to_add)))

    for resource in resources_to_add:
        logger.debug('Going to add resource "%s"' % resource)
        try:
            resource_store.add_resource(plugin_signature, resource)
        except:
            logger.exception('Error trying to add resource')
            return False

    resources_to_update = current_resource_set.resources.intersection(new_resource_set)

    logger.info('For plugin "%s" and site "%s", updating %d resources' % (
        plugin_signature, resource_site, len(resources_to_update)))

    start_time = time.time()

    resources_skipped = 0
    resources_updated = 0

    for resource in resources_to_update:
        current_timestamp = current_resource_dict[resource.serialization_key].resource_creation_timestamp
        new_timestamp = new_resource_dict[resource.serialization_key].resource_creation_timestamp
        if current_timestamp > new_timestamp:
            logger.info('For plugin "%s" and site "%s", resource "%s" has timestamp (%0.2f UTC) greater than of '
                        'new set (%0.2f UTC), skipping update' % (
                            plugin_signature, resource_site, str(resource), current_timestamp, new_timestamp))
            resources_skipped += 1
        else:
            try:
                new_resource = new_resource_dict[resource.serialization_key]
                logger.debug('Going to update resource "%s"' % new_resource)
                resource_store.add_resource(plugin_signature, new_resource)
                resources_updated += 1
            except:
                logger.exception('Error trying to add resource')
                return False

    end_time = time.time()

    logger.info('For plugin "%s" and site "%s", added/updated %d resources in %0.2f seconds, skipped %d resources' % (
        plugin_signature, resource_site, resources_updated, round(end_time - start_time, 2), resources_skipped))

    return True


def _signal_handler(signal_number, _):
    print('Caught %s, shutting down Resource Manager' % const.SIGNALS_TO_NAMES_DICT[signal_number])

    try:
        if consumer:
            print('Going to shutdown Kafka consumer')
            consumer.stop_consumer()
    except:
        print('Error trying to stop Kafka consumer, shutting down anyway')

    print('Shutdown complete, exiting')
    sys.exit(0)


def _install_signal_handlers():
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGHUP, _signal_handler)


def start():
    global panoptes_context, logger, resource_store, consumer

    try:
        panoptes_context = PanoptesResourceManagerContext()
    except Exception as e:
        sys.exit('Could not create a Panoptes Context: %s' % (str(e)))

    logger = panoptes_context.logger
    _install_signal_handlers()
    client_id = get_client_id(const.RESOURCE_MANAGER_CLIENT_ID_PREFIX)
    resource_store = PanoptesResourceStore(panoptes_context)

    consumer = PanoptesResourcesConsumer(panoptes_context=panoptes_context,
                                         keys=None,
                                         client_id=client_id,
                                         group=const.RESOURCE_MANAGER_KAFKA_GROUP_ID,
                                         poll_timeout=const.RESOURCE_MANAGER_KAFKA_POLL_TIMEOUT,
                                         callback=handle_resources,
                                         max_partition_fetch_bytes=const.RESOURCE_MANAGER_MAX_PARTITION_FETCH_BYTES)

    consumer.start_consumer()


if __name__ == '__main__':
    start()
