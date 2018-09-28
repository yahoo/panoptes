"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""

import json
import time
import unittest

from mock import *

from test_helpers import ordered
from yahoo_panoptes.framework.metrics import PanoptesMetricsGroup, PanoptesMetricDimension, \
    PanoptesMetricsGroupSet, PanoptesMetric, PanoptesMetricType, METRICS_TIMESTAMP_PRECISION
from yahoo_panoptes.framework.resources import PanoptesResource

mock_time = MagicMock()
mock_time.return_value = round(time.time(), METRICS_TIMESTAMP_PRECISION)


class TestResources(unittest.TestCase):
    def setUp(self):
        self.__panoptes_resource = PanoptesResource(resource_site='test', resource_class='test',
                                                    resource_subclass='test',
                                                    resource_type='test', resource_id='test', resource_endpoint='test',
                                                    resource_plugin='test')
        self.__panoptes_resource.add_metadata('test', 'test')

    def test_panoptes_metric(self):
        with self.assertRaises(AssertionError):
            PanoptesMetric(None, 0, PanoptesMetricType.GAUGE)

        with self.assertRaises(ValueError):
            PanoptesMetric('1', 0, PanoptesMetricType.GAUGE)

        with self.assertRaises(AssertionError):
            PanoptesMetric('test_metric', None, PanoptesMetricType.GAUGE)

        with self.assertRaises(AssertionError):
            PanoptesMetric('test_metric', 0, None)

        with self.assertRaises(AssertionError):
            PanoptesMetric('test_metric', True, PanoptesMetricType.GAUGE)

        metric1 = PanoptesMetric('test_metric', 0, PanoptesMetricType.GAUGE)

        self.assertEqual(metric1.metric_name, 'test_metric')
        self.assertEqual(metric1.metric_value, 0)
        self.assertEqual(metric1.metric_type, PanoptesMetricType.GAUGE)

        self.assertNotEqual(metric1, None)

        metric2 = PanoptesMetric('test_metric', 0, PanoptesMetricType.GAUGE)
        self.assertEqual(metric1, metric2)

        metric2 = PanoptesMetric('test_metric', 1, PanoptesMetricType.GAUGE)
        self.assertNotEqual(metric1, metric2)

        metric2 = PanoptesMetric('test_metric', 1, PanoptesMetricType.COUNTER)
        self.assertNotEqual(metric1, metric2)

    @patch('yahoo_panoptes.framework.metrics.time', mock_time)
    def test_panoptes_metric_json_and_repr(self):
        metric = PanoptesMetric('test_metric', 0, PanoptesMetricType.GAUGE, mock_time.return_value)
        serialized = json.loads(metric.json)
        expected = {"metric_creation_timestamp": mock_time.return_value,
                    "metric_name": "test_metric",
                    "metric_type": "gauge",
                    "metric_value": 0}

        self.assertEqual(ordered(serialized), ordered(expected))

    def testMetricsGroup(self):
        now = round(time.time(), METRICS_TIMESTAMP_PRECISION)
        metrics_group = PanoptesMetricsGroup(self.__panoptes_resource, 'test', 120)
        self.assertEqual(metrics_group.group_type, 'test')
        self.assertEqual(metrics_group.interval, 120)
        self.assertEqual(metrics_group.schema_version, '0.2')
        self.assertGreaterEqual(metrics_group.creation_timestamp, now)

        dimension_one = PanoptesMetricDimension('if_alias', 'bar')
        dimension_two = PanoptesMetricDimension('if_alias', 'foo')

        metrics_group.add_dimension(dimension_one)

        with self.assertRaises(KeyError):
            metrics_group.add_dimension(dimension_two)

        self.assertEqual(len(metrics_group.dimensions), 1)
        self.assertEqual(metrics_group.contains_dimension_by_name('if_alias'), True)
        self.assertEqual(metrics_group.contains_dimension_by_name('baz'), False)
        metrics_group.delete_dimension_by_name('if_alias')
        self.assertEqual(metrics_group.contains_dimension_by_name('if_alias'), False)
        self.assertEqual(len(metrics_group.dimensions), 0)
        self.assertEqual(metrics_group.get_dimension_by_name('foo'), None)

        metrics_group.add_dimension(dimension_two)
        dimension_three = PanoptesMetricDimension('if_alias', 'bar')
        metrics_group.upsert_dimension(dimension_three)
        self.assertEqual(len(metrics_group.dimensions), 1)
        self.assertEqual(metrics_group.get_dimension_by_name('if_alias').value, 'bar')
        dimension_four = PanoptesMetricDimension('if_name', 'eth0')
        metrics_group.upsert_dimension(dimension_four)
        self.assertEqual(len(metrics_group.dimensions), 2)

        with self.assertRaises(AssertionError):
            metrics_group.add_metric(None)

        metric = PanoptesMetric('test_metric', 0, PanoptesMetricType.GAUGE)
        metrics_group.add_metric(metric)
        to_json = metrics_group.json
        metrics = PanoptesMetricsGroup.flatten_metrics(json.loads(to_json)['metrics'])
        self.assertEquals(metrics['gauge']['test_metric']['value'], 0)

        metrics_group_two = PanoptesMetricsGroup(self.__panoptes_resource, 'test', 120)
        metrics_group_two.add_dimension(dimension_two)
        metrics_group_two.upsert_dimension(dimension_three)
        metrics_group_two.upsert_dimension(dimension_four)
        metrics_group_two.add_metric(metric)

        self.assertEqual(metrics_group, metrics_group_two)

    def test_metrics_group_hash(self):
        now = round(time.time(), METRICS_TIMESTAMP_PRECISION)
        metrics_group = PanoptesMetricsGroup(self.__panoptes_resource, 'test', 120)
        metrics_group_two = PanoptesMetricsGroup(self.__panoptes_resource, 'test', 120)

        dimension = PanoptesMetricDimension('if_alias', 'bar')
        metric = PanoptesMetric('test_metric', 0, PanoptesMetricType.GAUGE, metric_creation_timestamp=now)
        metric_diff_timestamp = PanoptesMetric('test_metric', 0, PanoptesMetricType.GAUGE,
                                               metric_creation_timestamp=now + 0.01)

        metrics_group.add_dimension(dimension)
        metrics_group_two.add_dimension(dimension)

        self.assertEqual(metrics_group.__hash__(), metrics_group_two.__hash__())

        metrics_group.add_metric(metric)
        metrics_group_two.add_metric(metric_diff_timestamp)

        self.assertEqual(metrics_group.__hash__(), metrics_group_two.__hash__())

        metrics_group_set = PanoptesMetricsGroupSet()
        metrics_group_set.add(metrics_group)
        metrics_group_set.add(metrics_group_two)

        assert len(metrics_group_set) == 1


if __name__ == '__main__':
    unittest.main()
