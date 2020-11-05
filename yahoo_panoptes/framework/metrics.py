"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module defines metrics and their related abstractions
"""
import json
import re
import threading
from time import time

from six import string_types

from yahoo_panoptes.framework.exceptions import PanoptesBaseException
from yahoo_panoptes.framework.resources import PanoptesResource
from yahoo_panoptes.framework.validators import PanoptesValidators

_VALID_KEY = re.compile(r"^[^\d\W]\w*\Z")


METRICS_TIMESTAMP_PRECISION = 3
METRICS_GROUP_SCHEMA_VERSION = u'0.2'


class PanoptesMetricsException(PanoptesBaseException):
    pass


class PanoptesMetricsNullException(PanoptesMetricsException):
    pass


class PanoptesMetricType(object):
    GAUGE, COUNTER = list(range(2))


METRIC_TYPE_NAMES = dict((getattr(PanoptesMetricType, n), n) for n in dir(PanoptesMetricType) if u'_' not in n)


class PanoptesMetricValidators(object):
    @classmethod
    def valid_panoptes_resource(cls, resource):
        """
        Return a resource resource object return a : class : a resource.

        Args:
            cls: (todo): write your description
            resource: (todo): write your description
        """
        return resource and isinstance(resource, PanoptesResource)

    @classmethod
    def valid_panoptes_metric_type(cls, metric_type):
        """
        Validate a metrictype for a metric_type.

        Args:
            cls: (todo): write your description
            metric_type: (str): write your description
        """
        return metric_type is not None and metric_type in METRIC_TYPE_NAMES

    @classmethod
    def valid_panoptes_metric(cls, metric):
        """
        Validate a metric object.

        Args:
            cls: (todo): write your description
            metric: (str): write your description
        """
        return metric and isinstance(metric, PanoptesMetric)

    @classmethod
    def valid_panoptes_metric_dimension(cls, metric_dimension):
        """
        Checks if metric dimension is a metric.

        Args:
            cls: (todo): write your description
            metric_dimension: (int): write your description
        """
        return metric_dimension and isinstance(metric_dimension, PanoptesMetricDimension)

    @classmethod
    def valid_panoptes_metrics_group(cls, metrics_group):
        """
        Returns a : class : metricgroupgroupgroupgroupgroupgroupgroupgroup instance.

        Args:
            cls: (todo): write your description
            metrics_group: (todo): write your description
        """
        return metrics_group and isinstance(metrics_group, PanoptesMetricsGroup)


class PanoptesMetric(object):
    """
    Representation of a metric monitored by a plugin

    A metric has a name and a corresponding value. It may also have associated dimension names and values

    Args:
        metric_name(str): The name of the metric
        metric_value(float): The value of the metric
        metric_type(int): The type of the metric - valid values are attributes of the PanoptesMetricType class
    """

    def __init__(self, metric_name, metric_value, metric_type, metric_creation_timestamp=None):
        """
        Initialize a metric.

        Args:
            self: (todo): write your description
            metric_name: (str): write your description
            metric_value: (todo): write your description
            metric_type: (str): write your description
            metric_creation_timestamp: (str): write your description
        """
        assert PanoptesValidators.valid_nonempty_string(metric_name), 'metric_name must be a non-empty str'
        assert PanoptesValidators.valid_number(metric_value), 'metric_value must be number'
        assert PanoptesMetricValidators.valid_panoptes_metric_type(
                metric_type), u'metric_type must be an attribute of PanoptesMetricType'
        assert (metric_creation_timestamp is None) or PanoptesValidators.valid_number(metric_creation_timestamp), \
            u'metric_creation_timestamp should be None or a number'

        if not _VALID_KEY.match(metric_name):
            raise ValueError(
                    u'metric name "%s" has to match pattern: (letter|"_") (letter | digit | "_")*' % metric_name)

        self.__data = dict()
        self.__data[u'metric_creation_timestamp'] = round(metric_creation_timestamp, METRICS_TIMESTAMP_PRECISION) if \
            metric_creation_timestamp is not None else round(time(), METRICS_TIMESTAMP_PRECISION)
        self.__data[u'metric_name'] = metric_name
        self.__data[u'metric_value'] = metric_value
        self.__metric_type_raw = metric_type
        self.__data[u'metric_type'] = METRIC_TYPE_NAMES[metric_type].lower()

    @property
    def metric_name(self):
        """
        The name of the metric

        Returns:
            str: The name of the metric
        """
        return self.__data[u'metric_name']

    @property
    def metric_value(self):
        """
        The value of the metric

        Returns:
            float: The value of the metric
        """
        return self.__data[u'metric_value']

    @property
    def metric_timestamp(self):
        """
        The creation timestamp of the metric

        Returns:
            float: The creation timestamp of the metric
        """
        return round(self.__data[u'metric_creation_timestamp'], METRICS_TIMESTAMP_PRECISION)

    @property
    def metric_type(self):
        """
        Return the metric_type.

        Args:
            self: (todo): write your description
        """
        return self.__metric_type_raw

    @property
    def json(self):
        """
        The JSON representation of the metric

        Returns:
            str: The JSON representation of the metric
        """
        return json.dumps(self.__data, sort_keys=True)

    def __repr__(self):
        """
        Return a human - readable representation of this metric.

        Args:
            self: (todo): write your description
        """
        return u'PanoptesMetric[' + str(self.metric_name) + u'|' + str(self.metric_value) + u'|' + \
               METRIC_TYPE_NAMES[self.metric_type] + u'|' + str(self.metric_timestamp) + u']'

    def __hash__(self):
        """
        Returns the hash of the field.

        Args:
            self: (todo): write your description
        """
        return hash(self.__data[u'metric_name'] + str(self.__data[u'metric_value']))

    def __str__(self):
        """
        Return a string representation of this metric.

        Args:
            self: (todo): write your description
        """
        return str(self.metric_name) + u'|' + str(self.metric_value) + u'|' + str(self.metric_type)

    def __lt__(self, other):
        """
        Determine if the given metric is a metric.

        Args:
            self: (todo): write your description
            other: (todo): write your description
        """
        if not isinstance(other, PanoptesMetric):
            return False
        return self.metric_name < other.metric_name

    def __eq__(self, other):
        """
        Returns true if the metric is equal value.

        Args:
            self: (todo): write your description
            other: (todo): write your description
        """
        if not isinstance(other, PanoptesMetric):
            return False

        return self.metric_name == other.metric_name and \
            self.metric_value == other.metric_value and \
            self.metric_type == other.metric_type


class PanoptesMetricDimension(object):
    def __init__(self, name, value):
        """
        Initialize a new instance.

        Args:
            self: (todo): write your description
            name: (str): write your description
            value: (todo): write your description
        """
        assert name and isinstance(name, string_types), (
            u'dimension name must be non-empty str or unicode, is type %s' % type(name))
        assert value and isinstance(value, string_types), (
            u'dimension value for dimension "%s" must be non-empty str or unicode, is type %s' % (name, type(value)))

        if not _VALID_KEY.match(name):
            raise ValueError(
                    u'dimension name "%s" has to match pattern: (letter|"_") (letter | digit | "_")*' % name)

        if u'|' in value:
            raise ValueError(u'dimension value "%s" cannot contain |' % value)

        self.__data = dict()

        self.__data[u'dimension_name'] = name
        self.__data[u'dimension_value'] = value

    @property
    def name(self):
        """
        The name of the data.

        Args:
            self: (todo): write your description
        """
        return self.__data[u'dimension_name']

    @property
    def value(self):
        """
        Return the value of the field.

        Args:
            self: (todo): write your description
        """
        return self.__data[u'dimension_value']

    @property
    def json(self):
        """
        : return : str

        Args:
            self: (todo): write your description
        """
        return json.dumps(self.__data)

    def __repr__(self):
        """
        Return a human - readable representation of this object.

        Args:
            self: (todo): write your description
        """
        return u'PanoptesMetricDimension[{}|{}]'.format(self.name, str(self.value))

    def __hash__(self):
        """
        Returns the hash of the name.

        Args:
            self: (todo): write your description
        """
        return hash(self.name + self.value)

    def __str__(self):
        """
        Return a human - readable string representation.

        Args:
            self: (todo): write your description
        """
        return str(self.name) + u'|' + str(self.value)

    def __lt__(self, other):
        """
        Determine if two metric objects are the same.

        Args:
            self: (todo): write your description
            other: (todo): write your description
        """
        if not isinstance(other, PanoptesMetricDimension):
            return False
        return self.name < other.name

    def __eq__(self, other):
        """
        Determine if two values are equal.

        Args:
            self: (todo): write your description
            other: (todo): write your description
        """
        if not isinstance(other, PanoptesMetricDimension):
            return False
        return self.name == other.name and self.value == other.value


class PanoptesMetricsGroupEncoder(json.JSONEncoder):
    # https://github.com/PyCQA/pylint/issues/414
    def default(self, o):  # pylint: disable=E0202
        """
        Default encoder to json.

        Args:
            self: (todo): write your description
            o: (todo): write your description
        """
        if isinstance(o, set):
            return list(o)
        if isinstance(o, PanoptesResource):
            return o.__dict__[u'_PanoptesResource__data']
        if isinstance(o, PanoptesMetric):
            return o.__dict__[u'_PanoptesMetric__data']
        if isinstance(o, PanoptesMetricDimension):
            return o.__dict__[u'_PanoptesMetricDimension__data']
        return json.JSONEncoder.default(self, o)


class PanoptesMetricsGroup(object):
    def __init__(self, resource, group_type, interval):
        """
        Initialize a metric group.

        Args:
            self: (todo): write your description
            resource: (str): write your description
            group_type: (str): write your description
            interval: (int): write your description
        """
        assert PanoptesMetricValidators.valid_panoptes_resource(
                resource), u'resource must be an instance of PanoptesResource'
        assert PanoptesValidators.valid_nonempty_string(
                group_type), u'group_type must be a non-empty string'
        assert PanoptesValidators.valid_nonzero_integer(
                interval), u'interval must a integer greater than zero'

        self.__data = dict()
        self.__metrics_index = {metric_type: list() for metric_type in METRIC_TYPE_NAMES}
        self.__data[u'metrics_group_type'] = group_type
        self.__data[u'metrics_group_interval'] = interval
        self.__data[u'metrics_group_creation_timestamp'] = round(time(), METRICS_TIMESTAMP_PRECISION)
        self.__data[u'metrics_group_schema_version'] = METRICS_GROUP_SCHEMA_VERSION
        self.__data[u'resource'] = resource
        self.__data[u'metrics'] = set()
        self.__data[u'dimensions'] = set()
        self._data_lock = threading.Lock()

    def copy(self):
        """
        Returns a copy of this group.

        Args:
            self: (todo): write your description
        """
        copied_metrics_group = PanoptesMetricsGroup(self.resource, self.group_type, self.interval)
        for metric in self.metrics:
            copied_metrics_group.add_metric(metric)
        for dimension in self.dimensions:
            copied_metrics_group.add_dimension(dimension)

        return copied_metrics_group

    def add_metric(self, metric):
        """
        Add a metric to the metric group.

        Args:
            self: (todo): write your description
            metric: (str): write your description
        """
        assert PanoptesMetricValidators.valid_panoptes_metric(metric), u'metric must be an instance of PanoptesMetric'

        if metric.metric_name in self.__metrics_index[metric.metric_type]:
            raise KeyError(u'Metric name "%s" (type "%s") for metrics group type "%s" already populated' %
                           (metric.metric_name, METRIC_TYPE_NAMES[metric.metric_type], self.group_type))
        self.__data[u'metrics'].add(metric)
        self.__metrics_index[metric.metric_type].append(metric.metric_name)

    def add_dimension(self, dimension):
        """
        Adds a metric to the metric.

        Args:
            self: (todo): write your description
            dimension: (int): write your description
        """
        assert PanoptesMetricValidators.valid_panoptes_metric_dimension(dimension), u'dimension must be instance ' \
                                                                                    u'of PanoptesMetricDimension'
        with self._data_lock:
            if self.contains_dimension_by_name(dimension.name):
                raise KeyError(u'Dimension name %s already populated. '
                               u'Please use upsert_dimension if you need to update dimensions' % dimension.name)
            else:
                self.__data[u'dimensions'].add(dimension)

    def get_dimension_by_name(self, dimension_name):
        """
        Args : class :.

        Args:
            self: (todo): write your description
            dimension_name: (str): write your description
        """
        assert dimension_name and isinstance(dimension_name, string_types), (
            u'dimension name must be non-empty str or unicode, is type %s' % type(dimension_name))
        dimension = [x for x in self.__data[u'dimensions'] if x.name == dimension_name]
        if not dimension:
            return None
        else:
            return dimension[0]

    def contains_dimension_by_name(self, dimension_name):
        """
        Returns true if the given dimension contains the given dimension_name.

        Args:
            self: (todo): write your description
            dimension_name: (str): write your description
        """
        assert dimension_name and isinstance(dimension_name, string_types), (
            u'dimension name must be non-empty str or unicode, is type %s' % type(dimension_name))
        return dimension_name in [x.name for x in self.__data[u'dimensions']]

    def delete_dimension_by_name(self, dimension_name):
        """
        Deletes a dimension by name.

        Args:
            self: (todo): write your description
            dimension_name: (str): write your description
        """
        assert dimension_name and isinstance(dimension_name, string_types), (
            u'dimension name must be non-empty str or unicode, is type %s' % type(dimension_name))
        with self._data_lock:
            if self.contains_dimension_by_name(dimension_name):
                dimension = self.get_dimension_by_name(dimension_name)
                self.__data[u'dimensions'].remove(dimension)

    def upsert_dimension(self, dimension):
        """
        Assigns or updates a dimension.

        Args:
            self: (todo): write your description
            dimension: (int): write your description
        """
        assert PanoptesMetricValidators.valid_panoptes_metric_dimension(
                dimension), u'dimension must be instance of PanoptesMetricDimension'

        if self.contains_dimension_by_name(dimension.name):
            self.delete_dimension_by_name(dimension.name)
        self.__data[u'dimensions'].add(dimension)

    @staticmethod
    def flatten_dimensions(dimensions):
        """Changes list of dimensions to dict

        Args:
            dimensions(list): List of dictionaries containing name and value for each dimension

        Returns:
            dict: Key is dimension_name, Value is dimension_value
        """
        return {dimension[u'dimension_name']: dimension[u'dimension_value'] for dimension in dimensions}

    @staticmethod
    def flatten_metrics(metrics):
        """Changes list of metrics to nested dict

        Args:
            metrics(list): List of dictionaries containing name, value, type

        Returns:
            dict: Keys are counter, gauge, which then contain a dictionary of the metrics name paired with
            values and timestamps for each name.
        """
        metrics_dict = {u'counter': {}, u'gauge': {}}

        for metric in metrics:
            metrics_dict[metric[u'metric_type']][metric[u'metric_name']] = \
                {u'value': metric[u'metric_value'], u'timestamp': metric[u'metric_creation_timestamp']}

        return metrics_dict

    @property
    def resource(self):
        """
        Returns the resource.

        Args:
            self: (todo): write your description
        """
        return self.__data[u'resource']

    @property
    def metrics(self):
        """
        Return a list of metric names.

        Args:
            self: (todo): write your description
        """
        return sorted(self.__data[u'metrics'])

    @property
    def dimensions(self):
        """
        Return the dimensions of all dimensions.

        Args:
            self: (todo): write your description
        """
        return sorted(self.__data[u'dimensions'])

    @property
    def group_type(self):
        """
        The group type.

        Args:
            self: (todo): write your description
        """
        return self.__data[u'metrics_group_type']

    @property
    def interval(self):
        """
        Return the interval of the interval.

        Args:
            self: (todo): write your description
        """
        return self.__data[u'metrics_group_interval']

    @property
    def schema_version(self):
        """
        Return the schema version.

        Args:
            self: (todo): write your description
        """
        return self.__data[u'metrics_group_schema_version']

    @property
    def creation_timestamp(self):
        """
        Creation creation timestamp.

        Args:
            self: (todo): write your description
        """
        return self.__data[u'metrics_group_creation_timestamp']

    @property
    def json(self):
        """
        Serialize the data in json.

        Args:
            self: (todo): write your description
        """
        return json.dumps(self.__data, cls=PanoptesMetricsGroupEncoder)

    def __repr__(self):
        """
        Return a representation of this metric representation.

        Args:
            self: (todo): write your description
        """
        return u'PanoptesMetricsGroup[' \
            u'resource:' + repr(self.resource) + u',' \
            u'interval:' + str(self.interval) + u',' \
            u'schema_version:' + self.schema_version + u',' \
            u'group_type:' + self.group_type + u',' \
            u'creation_timestamp:' + str(self.creation_timestamp) + u',' \
            u'dimensions:[' + u','.join([repr(dimension) for dimension in sorted(self.dimensions)]) + u'],' \
            u'metrics:[' + u','.join([repr(metric) for metric in sorted(self.metrics)]) + u']]'

    def __hash__(self):
        """
        Returns a hash string for the metric.

        Args:
            self: (todo): write your description
        """
        metrics_string = str()
        dimensions_string = str()

        for metric in frozenset(self.metrics):
            metrics_string += str(metric)

        for dimension in frozenset(self.dimensions):
            dimensions_string += str(dimension)

        return hash(str(self.resource) + metrics_string + dimensions_string)

    def __lt__(self, other):
        """
        Determine if the given other is a grouptype.

        Args:
            self: (todo): write your description
            other: (todo): write your description
        """
        if not isinstance(other, PanoptesMetricsGroup):
            return False
        return self.group_type < other.group_type

    def __eq__(self, other):
        """
        Returns true if other is a metric is a metric.

        Args:
            self: (todo): write your description
            other: (todo): write your description
        """
        if not isinstance(other, PanoptesMetricsGroup):
            return False
        return self.resource == other.resource and self.metrics == other.metrics and self.dimensions == other.dimensions


class PanoptesMetricsGroupSet(object):
    def __init__(self):
        """
        Initialize the metrics.

        Args:
            self: (todo): write your description
        """
        self._metrics_groups = set()

    def add(self, metrics_group):
        """
        Adds metrics to the metric group.

        Args:
            self: (todo): write your description
            metrics_group: (int): write your description
        """
        assert PanoptesMetricValidators.valid_panoptes_metrics_group(
                metrics_group), u'metrics_group must be an instance of PanoptesMetricsGroup'
        self._metrics_groups.add(metrics_group)

    def remove(self, metrics_group):
        """
        Remove a metric group.

        Args:
            self: (todo): write your description
            metrics_group: (todo): write your description
        """
        assert PanoptesMetricValidators.valid_panoptes_metrics_group(
                metrics_group), u'metrics_group must be an instance of PanoptesMetricsGroup'
        self._metrics_groups.remove(metrics_group)

    @property
    def metrics_groups(self):
        """
        Returns the : class : class :.

        Args:
            self: (todo): write your description
        """
        return self._metrics_groups

    def __add__(self, other):
        """
        Returns a new metricsgroupset to this group.

        Args:
            self: (todo): write your description
            other: (todo): write your description
        """
        if not other or not isinstance(other, PanoptesMetricsGroupSet):
            raise TypeError(u'Unsupported type for addition: {}'.format(type(other)))

        new_metrics_group_set = PanoptesMetricsGroupSet()
        list(map(new_metrics_group_set.add, self.metrics_groups))
        list(map(new_metrics_group_set.add, other.metrics_groups))

        return new_metrics_group_set

    def __iter__(self):
        """
        Returns an iterator over the iterables.

        Args:
            self: (todo): write your description
        """
        return iter(self._metrics_groups)

    def __next__(self):
        """
        Returns the next result.

        Args:
            self: (todo): write your description
        """
        return next(iter(self._metrics_groups))

    def __len__(self):
        """
        Returns the length of the data group.

        Args:
            self: (todo): write your description
        """
        return len(self._metrics_groups)

    def __repr__(self):
        """
        Return a human - readable representation of this group.

        Args:
            self: (todo): write your description
        """
        return u'PanoptesMetricsGroupSet[' + \
            u','.join([repr(metric_group) for metric_group in sorted(self._metrics_groups)]) + \
            u']'


class PanoptesMetricSet(object):
    """
    An (un-ordered) set of PanoptesMetrics
    """

    def __init__(self):
        """
        Initialize the metrics

        Args:
            self: (todo): write your description
        """
        self.__metrics = set()

    def add(self, metric):
        """
        Add a metric to the set

        Args:
            metric (PanoptesMetric): The metric to add

        Returns:
            None

        """
        assert isinstance(metric, PanoptesMetric), 'metric must be an instance of PanoptesMetric'
        self.__metrics.add(metric)

    def remove(self, metric):
        """
        Remove a metric from the set

        Args:
            metric (PanoptesMetric): The metric to remove

        Returns:
            None

        """
        assert isinstance(metric, PanoptesMetric), 'metric must be an instance of PanoptesMetric'
        self.__metrics.remove(metric)

    @property
    def metrics(self):
        """
        Return the list of metrics in this set

        Returns:
            list: The set of metrics

        """
        return self.__metrics

    def __iter__(self):
        """
        Returns an iterator over the iterables.

        Args:
            self: (todo): write your description
        """
        return iter(self.__metrics)

    def __next__(self):
        """
        Returns the next metric in the set

        Returns:
            PanoptesMetric: The next metric in the set
        """
        return next(iter(self.__metrics))

    def __repr__(self):
        """
        Return a human - readable representation of this metric.

        Args:
            self: (todo): write your description
        """
        return u'PanoptesMetricSet[' + \
               u','.join([repr(metric) for metric in self.__metrics]) + \
               u']'

    def __len__(self):
        """
        Returns the length of the metric.

        Args:
            self: (todo): write your description
        """
        return len(self.__metrics)
