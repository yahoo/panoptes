"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

Discovery plugin to create PanoptesResources from JSON config files.
"""
import json
import os

from yahoo_panoptes.discovery.panoptes_discovery_plugin import PanoptesDiscoveryPlugin, PanoptesDiscoveryPluginError
from yahoo_panoptes.framework.plugins.context import PanoptesPluginContext
from yahoo_panoptes.framework.resources import PanoptesResource, PanoptesResourceSet


class PluginDiscoveryJSONFile(PanoptesDiscoveryPlugin):
    """
    Standalone discovery plugin to populate PanoptesResources from a JSON file.
    """
    def run(self, context):
        """
        The main entry point to the plugin

        Args:
            context (PanoptesPluginContext): The Plugin Context passed by the Plugin Agent

        Returns:
            PanoptesResourceSet: A non-empty resource set

        Raises:
            PanoptesDiscoveryPluginError: This exception is raised if any part of the lookup process has errors
        """

        assert context and isinstance(context, PanoptesPluginContext), u'context must be a PanoptesPluginContext'

        conf = context.config
        logger = context.logger
        config_file = None

        try:
            config_file = conf[u'main'][u'config_file']
            if not config_file.startswith(os.path.sep):
                config_file = os.path.join(os.path.dirname((os.path.realpath(__file__))), config_file)
            with open(config_file) as f:
                resource_specs = json.load(f)
        except Exception as e:
            raise PanoptesDiscoveryPluginError(
                u'Error while attempting to parse JSON from file {}: {}'.format(config_file, repr(e))
            )

        resources = PanoptesResourceSet()
        num_successes = 0
        num_failures = 0

        for resource_spec in resource_specs:
            try:
                resource = PanoptesResource.resource_from_dict(resource_spec)
                resources.add(resource)
                num_successes += 1
                logger.debug(u'Added resource {} from JSON file {}'.format(resource, config_file))
            except Exception as e:
                logger.debug(u'Error while attempting to create a PanoptesResource from file {}: {}'.format(
                    config_file, repr(e)))
                num_failures += 1
                continue

        if num_successes > 0:
            logger.info(u'Tried to read {} resources from {}, {} failed'.format(num_successes + num_failures,
                                                                                config_file,
                                                                                num_failures))
        else:
            logger.error(u'Error while attempting to create PanoptesResources from {}.'.format(config_file))
            raise PanoptesDiscoveryPluginError(
                    u'Error during lookup for PanoptesResource from file {}.'.format(config_file))

        return resources
