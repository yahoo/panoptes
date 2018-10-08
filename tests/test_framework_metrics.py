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
    PanoptesMetricsGroupSet, PanoptesMetric, PanoptesMetricSet, PanoptesMetricType, PanoptesMetricsGroupEncoder, \
    METRICS_TIMESTAMP_PRECISION
from yahoo_panoptes.framework.resources import PanoptesResource

mock_time = MagicMock()
mock_time.return_value = round(1538082314.09, METRICS_TIMESTAMP_PRECISION)


class TestMetrics(unittest.TestCase):
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

        metric1 = PanoptesMetric('test_metric', 0, PanoptesMetricType.GAUGE,
                                 metric_creation_timestamp=mock_time.return_value)

        self.assertEqual(metric1.metric_name, 'test_metric')
        self.assertEqual(metric1.metric_value, 0)
        self.assertEqual(metric1.metric_timestamp, mock_time.return_value)
        self.assertEqual(metric1.metric_type, PanoptesMetricType.GAUGE)
        self.assertEqual(repr(metric1),
                         "{{'metric_creation_timestamp': {}, 'metric_type': 'gauge', 'metric_name': 'test_metric', "
                         "'metric_value': 0}}".format(mock_time.return_value))

        self.assertNotEqual(metric1, None)

        # Check PanoptesMetric.__eq__
        assert metric1 == PanoptesMetric('test_metric', 0, PanoptesMetricType.GAUGE)
        with self.assertRaises(AssertionError):
            assert metric1 == PanoptesMetricDimension("test", "value")
        with self.assertRaises(AssertionError):
            assert metric1 == PanoptesMetric('different_name', 0, PanoptesMetricType.GAUGE)
        with self.assertRaises(AssertionError):
            assert metric1 == PanoptesMetric('test_metric', 1, PanoptesMetricType.GAUGE)
        with self.assertRaises(AssertionError):
            assert metric1 == PanoptesMetric('test_metric', 0, PanoptesMetricType.COUNTER)

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

        with patch('yahoo_panoptes.framework.metrics.time', mock_time):
            metrics_group = PanoptesMetricsGroup(self.__panoptes_resource, 'test', 120)

            dimension_one = PanoptesMetricDimension('if_alias', 'bar')
            dimension_two = PanoptesMetricDimension('if_alias', 'foo')

            metrics_group.add_dimension(dimension_one)

            with self.assertRaises(KeyError):
                metrics_group.add_dimension(dimension_two)

            #  Test basic dimension operations
            self.assertEqual(len(metrics_group.dimensions), 1)
            self.assertTrue(metrics_group.contains_dimension_by_name('if_alias'))
            self.assertFalse(metrics_group.contains_dimension_by_name('baz'))
            self.assertEqual(metrics_group.get_dimension_by_name('if_alias').value, 'bar')
            metrics_group.delete_dimension_by_name('if_alias')
            self.assertFalse(metrics_group.contains_dimension_by_name('if_alias'))
            self.assertEqual(len(metrics_group.dimensions), 0)
            self.assertEqual(metrics_group.get_dimension_by_name('foo'), None)

            metrics_group.add_dimension(dimension_two)
            dimension_three = PanoptesMetricDimension('if_alias', 'test')
            metrics_group.upsert_dimension(dimension_three)
            self.assertEqual(len(metrics_group.dimensions), 1)
            self.assertEqual(metrics_group.get_dimension_by_name('if_alias').value, 'test')
            dimension_four = PanoptesMetricDimension('if_name', 'eth0')
            metrics_group.upsert_dimension(dimension_four)
            self.assertEqual(len(metrics_group.dimensions), 2)

            #  Test basic metric operations
            with self.assertRaises(AssertionError):
                metrics_group.add_metric(None)

            metric = PanoptesMetric('test_metric', 0, PanoptesMetricType.GAUGE)
            metrics_group.add_metric(metric)
            with self.assertRaises(KeyError):
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

            # Check PanoptesMetricsGroup.__eq__
            panoptes_resource_two = PanoptesResource(resource_site='test2', resource_class='test2',
                                                     resource_subclass='test2',
                                                     resource_type='test2', resource_id='test2',
                                                     resource_endpoint='test2',
                                                     resource_plugin='test2')

            metrics_group_two = PanoptesMetricsGroup(panoptes_resource_two, 'test', 120)
            metrics_group_three = PanoptesMetricsGroup(self.__panoptes_resource, 'test', 120)
            with self.assertRaises(AssertionError):
                assert metrics_group_two == metrics_group_three

            metrics_group_three = metrics_group.copy()

            with self.assertRaises(AssertionError):
                assert metrics_group == dimension_one
            assert metrics_group == metrics_group_three

            metrics_group_three.delete_dimension_by_name("if_name")
            with self.assertRaises(AssertionError):
                assert metrics_group == metrics_group_three
            metrics_group_three.upsert_dimension(dimension_four)
            assert metrics_group == metrics_group_three

            metric_two = PanoptesMetric('test_metric_2', 1, PanoptesMetricType.GAUGE)
            metrics_group_three.add_metric(metric_two)
            with self.assertRaises(AssertionError):
                assert metrics_group == metrics_group_three

            #  Test PanoptesMetricsGroup.__repr__
            _METRICS_GROUP_REPR = "{{'metrics_group_interval': 120, " \
                                  "'resource': plugin|test|site|test|class|test|subclass|test|type|test|id|test|" \
                                  "endpoint|test, 'dimensions': set([{{'dimension_name': 'if_alias', " \
                                  "'dimension_value': 'test'}}, " \
                                  "{{'dimension_name': 'if_name', 'dimension_value': 'eth0'}}]), " \
                                  "'metrics_group_type': 'test', " \
                                  "'metrics': set([{{'metric_creation_timestamp': {}, " \
                                  "'metric_type': 'gauge', 'metric_name': 'test_metric', 'metric_value': 0}}]), " \
                                  "'metrics_group_creation_timestamp': {}, " \
                                  "'metrics_group_schema_version': '0.2'}}".format(mock_time.return_value,
                                                                                   mock_time.return_value)
            self.assertEqual(repr(metrics_group), _METRICS_GROUP_REPR)

            dimensions_as_dicts = [{'dimension_name': dimension.name,
                                    'dimension_value': dimension.value} for dimension in metrics_group.dimensions]
            self.assertEqual(PanoptesMetricsGroup.flatten_dimensions(dimensions_as_dicts),
                             {'if_alias': 'test', 'if_name': 'eth0'})

    def test_panoptes_metrics_group_encoder(self):
        test_dict = dict()
        encoder = PanoptesMetricsGroupEncoder()

        mock_default = Mock(json.JSONEncoder.default)
        with patch('yahoo_panoptes.framework.metrics.json.JSONEncoder.default', mock_default):
            encoder.default(test_dict)
            mock_default.assert_called_once()

    def test_panoptes_metric_dimension(self):
        with self.assertRaises(ValueError):
            PanoptesMetricDimension('contain$_invalid_character$', 'bar')
        with self.assertRaises(ValueError):
            PanoptesMetricDimension('foo', 'contains_pipe|')

        dimension_one = PanoptesMetricDimension('if_alias', 'bar')

        self.assertEqual(dimension_one.json, '{"dimension_name": "if_alias", "dimension_value": "bar"}')
        self.assertEqual(repr(dimension_one), "{'dimension_name': 'if_alias', 'dimension_value': 'bar'}")

        metric_one = PanoptesMetric('test_metric', 0, PanoptesMetricType.GAUGE,
                                    metric_creation_timestamp=mock_time.return_value)

        with self.assertRaises(AssertionError):
            assert dimension_one == metric_one

        dimension_two = PanoptesMetricDimension('if_alias', 'foo')
        with self.assertRaises(AssertionError):
            assert dimension_one == dimension_two

        dimension_three = PanoptesMetricDimension('if_alias', 'bar')
        assert dimension_one == dimension_three

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

    @patch('yahoo_panoptes.framework.metrics.time', mock_time)
    def test_panoptes_metrics_group_set(self):
        """Tests basic PanoptesMetricsGroupSet operations"""
        metrics_group_set = PanoptesMetricsGroupSet()
        metrics_group = PanoptesMetricsGroup(self.__panoptes_resource, 'test', 120)
        metrics_group_two = PanoptesMetricsGroup(self.__panoptes_resource, 'test', 120)
        metrics_group_set.add(metrics_group)
        metrics_group_set.add(metrics_group_two)
        assert len(metrics_group_set) == 1
        self.assertIn(metrics_group, metrics_group_set.metrics_groups)

        metrics_group_set.remove(metrics_group_two)
        assert len(metrics_group_set) == 0

        metrics_group_set.add(metrics_group)
        metrics_group_three = PanoptesMetricsGroup(self.__panoptes_resource, 'test3', 120)
        metrics_group_three.add_metric(PanoptesMetric("test3", 0.0, PanoptesMetricType.GAUGE))
        metrics_group_set.add(metrics_group_three)
        assert len(metrics_group_set) == 2

        metrics_group_set_two = PanoptesMetricsGroupSet()
        metrics_group_four = PanoptesMetricsGroup(self.__panoptes_resource, 'test', 120)
        metrics_group_four.add_metric(PanoptesMetric("test4", 0.0, PanoptesMetricType.GAUGE))
        metrics_group_set_two.add(metrics_group_four)
        assert len(metrics_group_set_two) == 1

        #  Test PanoptesMetricsGroupSet.__add__
        metrics_group_set_union = metrics_group_set + metrics_group_set_two
        assert len(metrics_group_set_union) == 3

        with self.assertRaises(AssertionError):
            metrics_group_set.remove(self.__panoptes_resource)

        with self.assertRaises(TypeError):
            metrics_group_set + metrics_group

        #  Test PanoptesMetricsGroupSet.__iter__ & 'next'
        metrics_group_count = 0
        metrics_group_set_union_interator = iter(metrics_group_set_union)
        for _ in metrics_group_set_union:
            self.assertIn(metrics_group_set_union_interator.next(), metrics_group_set_union.metrics_groups)
            metrics_group_count += 1
        assert len(metrics_group_set_union) == metrics_group_count
        with self.assertRaises(Exception):
            metrics_group_set_union_interator.next()

        #  Test PanoptesMetricsGroupSet.__repr__
        _METRICS_GROUP_SET_REPR = "set([{{'metrics_group_interval': 120, " \
                                  "'resource': plugin|test|site|test|class|test|subclass|test|type|test|id|test|" \
                                  "endpoint|test, 'dimensions': set([]), 'metrics_group_type': 'test', " \
                                  "'metrics': set([]), 'metrics_group_creation_timestamp': {}, " \
                                  "'metrics_group_schema_version': '0.2'}}, {{'metrics_group_interval': 120, " \
                                  "'resource': plugin|test|site|test|class|test|subclass|test|type|test|id|test|" \
                                  "endpoint|test, 'dimensions': set([]), 'metrics_group_type': 'test3', " \
                                  "'metrics': set([{{'metric_creation_timestamp': {}, " \
                                  "'metric_type': 'gauge', " \
                                  "'metric_name': 'test3', 'metric_value': 0.0}}]), " \
                                  "'metrics_group_creation_timestamp': {}, " \
                                  "'metrics_group_schema_version': '0.2'}}])".format(mock_time.return_value,
                                                                                     mock_time.return_value,
                                                                                     mock_time.return_value)
        self.assertEqual(repr(metrics_group_set), _METRICS_GROUP_SET_REPR)

    def test_panoptes_metric_set(self):
        metric_set = PanoptesMetricSet()
        metric1 = PanoptesMetric('test_metric', 0, PanoptesMetricType.GAUGE,
                                 metric_creation_timestamp=mock_time.return_value)
        metric2 = PanoptesMetric('test_metric', 0, PanoptesMetricType.GAUGE,
                                 metric_creation_timestamp=mock_time.return_value)

        metric_set.add(metric1)
        metric_set.add(metric2)
        assert len(metric_set) == 1

        self.assertIn(metric1, metric_set.metrics)

        #  Test PanoptesMetricSet.__repr__
        _METRIC_SET_REPR = "set([{{'metric_creation_timestamp': {}, 'metric_type': 'gauge', " \
                           "'metric_name': 'test_metric', 'metric_value': 0}}])".format(mock_time.return_value)
        self.assertEqual(repr(metric_set), _METRIC_SET_REPR)

        with self.assertRaises(Exception):
            metric_set.remove(self.__panoptes_resource)

        metric_set.remove(metric1)
        assert len(metric_set) == 0

        #  Test PanoptesMetricSet.__iter__ and 'next'
        metric_count = 0
        metric_set_iterator = iter(metric_set)
        for _ in metric_set:
            self.assertIn(metric_set_iterator.next(), metric_set.metrics)
            metric_count += 1
        assert len(metric_set) == metric_count
        with self.assertRaises(Exception):
            metric_set_iterator.next()

if __name__ == '__main__':
    unittest.main()
