"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module defines a generic polling plugin class for use in testing.
"""
from yahoo_panoptes.enrichment.enrichment_plugin import PanoptesEnrichmentPlugin
from yahoo_panoptes.framework.metrics import PanoptesMetric, PanoptesMetricType, PanoptesMetricsGroup
from yahoo_panoptes.framework.resources import PanoptesResource, PanoptesResourceSet


_TEST_INTERVAL = 60


class PanoptesTestPollingPlugin(PanoptesEnrichmentPlugin):
    name = "Test Enrichment Plugin"
    resource = PanoptesResource(resource_site='test_site',
                                resource_class='test_class',
                                resource_subclass='test_subclass',
                                resource_type='test_type',
                                resource_id='test_resource_id',
                                resource_endpoint='test_resource_endpoint',
                                resource_plugin='test_resource_plugin',
                                resource_creation_timestamp=0)

    def run(self, context):
        """
        Returns the context manager.

        Args:
            self: (todo): write your description
            context: (dict): write your description
        """

        panoptes_resource_set = PanoptesResourceSet()
        panoptes_resource_set.add(self.resource)

        return panoptes_resource_set
