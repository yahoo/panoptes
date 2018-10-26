"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module defines a generic polling plugin class for use in testing.
"""

from yahoo_panoptes.polling.polling_plugin import PanoptesPollingPlugin
from yahoo_panoptes.framework.metrics import PanoptesMetric, PanoptesMetricType, PanoptesMetricsGroup, \
    PanoptesMetricsGroupSet
from yahoo_panoptes.framework.resources import PanoptesResource


_TEST_INTERVAL = 60


class PanoptesTestPollingPlugin(PanoptesPollingPlugin):
    name = "Panoptes Test Polling Plugin"
    panoptes_resource = PanoptesResource(resource_site='test', resource_class='test',
                                         resource_subclass='test',
                                         resource_type='test', resource_id='test', resource_endpoint='test',
                                         resource_plugin='test')

    def run(self, context):
        metric_group_set = PanoptesMetricsGroupSet()
        metric1 = PanoptesMetric("test", 0.0, PanoptesMetricType.GAUGE)
        metric_group = PanoptesMetricsGroup(self.panoptes_resource, "Test", _TEST_INTERVAL)
        metric_group.add_metric(metric1)
        metric_group_set.add(metric_group)
        return metric_group_set
