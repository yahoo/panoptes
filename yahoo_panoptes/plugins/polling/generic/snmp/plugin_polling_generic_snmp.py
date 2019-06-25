"""
This module implements a generic SNMP Panoptes plugin that can consume enrichments for a range of device types in order
to poll those same devices.
"""
import json
import re
import time

from yahoo_panoptes.framework import metrics, enrichment, exceptions
from yahoo_panoptes.framework.plugins import panoptes_base_plugin, base_snmp_plugin
from yahoo_panoptes.polling import polling_plugin
from yahoo_panoptes.plugins.polling.utilities import polling_status

_NON_REPEATERS = 0
_MAX_REPETITIONS = 25

_TABLE_PATTERN = re.compile(r'(\w+)(?=\.|\[|$)')

_TYPE_MAPPING = {
    "Integer": int,
    "Integer32": int,
    "UInteger32": int,
    "Octet String": str,
    "Object Identifier": str,
    "Bit String": str,
    "IpAddress": str,
    "Counter32": int,
    "Counter64": long,
    "Gauge32": int,
    "TimeTicks": long,
    "Opaque": str,
    "NsapAddress": str,
    "integer": int,  # User-defined types
    "int": int,
    "float": float,
    "double": float,
    "string": str,
    "str": str,
    "long": long
}

_V1_STRING_LITERALS = ['data', 'ctrl', 'dram']

_METRIC_TYPE_MAP = {
    "gauge": metrics.PanoptesMetricType.GAUGE,
    "counter": metrics.PanoptesMetricType.COUNTER
}


def _identity(x):
    """Perform a simple identity function."""
    return x


class PanoptesMetricDimensionException(exceptions.PanoptesBaseException):
    pass


class PanoptesMetricDimensionNullException(PanoptesMetricDimensionException):
    pass


class PanoptesEnrichmentFileEmptyError(panoptes_base_plugin.PanoptesPluginConfigurationError):
    pass


class PluginPollingGenericSNMPMetrics(base_snmp_plugin.PanoptesSNMPBasePlugin, polling_plugin.PanoptesPollingPlugin):
    """
    This plugin takes an enrichment defining what metric groups should be output and what oids should be polled, and
    evaluates them according to the Generic SNMP Polling DSL.
    """
    def __init__(self):
        """See base class."""
        super(PluginPollingGenericSNMPMetrics, self).__init__()

        self._metrics = metrics.PanoptesMetricsGroupSet()
        self._config = None
        self._namespace = None
        self._device_model = None
        self._polling_status = None
        self._polling_status_metric_name = None
        self._enrichment_schema_version = None
        self._oid_maps = None
        self._snmpget_oid_map = None

    def _get_metrics_groups_with_oid(self, oid_name):
        """
        Given an oid_name, returns a set of the names of all metrics groups which use that oid.
        Args:
            oid_name: The name of the oid to query as defined in the oid map.
        Returns:
            A set of the names (as defined in the metrics_groups map) of all metrics groups that reference that oid.
        """
        metrics_groups = set()
        for metrics_group_map in self._config["metrics_groups"]:
            for metric_value in metrics_group_map["metrics"].values():
                if isinstance(metric_value["value"], basestring):
                    if oid_name in metric_value["value"]:
                        metrics_groups.add(metrics_group_map["group_name"])
            for dimension_value in metrics_group_map["dimensions"].values():
                if isinstance(dimension_value["value"], basestring):
                    if oid_name in dimension_value["value"]:
                        metrics_groups.add(metrics_group_map["group_name"])
        return metrics_groups

    def _handle_exceptions_for_oid(self, oid_name, error):
        """
        Given a failed oid and a an error caught when that oid failed, updates the polling status for every
            metrics group which relies on that oid.
        Args:
            oid_name: The name of the oid that failed.
            error: The error instance raised when the oid failed.
        Returns:
            None
        """
        failed_metrics_groups = self._get_metrics_groups_with_oid(oid_name)

        for failed_group in failed_metrics_groups:
            self._polling_status.handle_exception(failed_group, error)

    def _handle_successes_for_oid(self, oid_name):
        """
        Updates the polling status when provided oid is successfully queried.
        Args:
            oid_name: The name of the oid that succeeded.
        Returns:
            None
        """
        successful_metrics_groups = self._get_metrics_groups_with_oid(oid_name)

        for successful_group in successful_metrics_groups:
            self._polling_status.handle_success(successful_group)

    def _get_snmp_polling_var(self, var, default):
        """
        Gets the value of the provided snmp polling variable, or assigns it to the default value provided.
            SNMP polling variables such as non_repeaters and max_repetitions should be resolved in the following order:
                JSON config
                Plugin config
                Defaults
        Args:
            var: The snmp polling variable to assign
            default: The default value to assign to var if var is not found in the json or plugin configs.
        Returns:
            The value to assign to var.
        """
        if self._config.get('snmp'):
            if var in self._config['snmp']:
                return self._config['snmp'].get(var)

        if 'snmp' in self._plugin_context.config:
            if var in self._plugin_context.config['snmp']:
                return self._plugin_context.config['snmp'].get(var)

        return default

    def _build_map(self, oid_name):
        """
        Builds the oids_map for the provided oid_name using the specified method.
        Args:
            oid_name: The oid for which to build the map using either snmp bulk_walk or get
        Returns:
            None
        """
        try:
            if self._config["oids"][oid_name]["method"] == "bulk_walk":
                self._build_map_by_bulk_walk(oid_name)
            elif self._config["oids"][oid_name]["method"] == "get":
                self._build_map_by_get(oid_name)
        except Exception as e:
            self._logger.warn('Exception when trying to poll device "%s" for "%s": %s' %
                              (self._host, oid_name, repr(e)))

    def _build_map_by_bulk_walk(self, oid_name):
        """
        Builds the oids_map for the provided oid_name using snmp bulk walk.
        Args:
            oid_name: The oid for which to build the map using snmp bulk walk.
        Returns:
            None
        """
        self._oid_maps[oid_name] = {}
        device_metrics_map = dict()
        stats = None
        try:
            if self._config["oids"][oid_name]["method"] == "bulk_walk":
                stats = self._snmp_connection.bulk_walk(oid=self._config["oids"][oid_name]["oid"],
                                                        non_repeaters=self._get_snmp_polling_var(
                                                            "non_repeaters", _NON_REPEATERS),
                                                        max_repetitions=self._get_snmp_polling_var(
                                                            "max_repetitions", _MAX_REPETITIONS))
        except Exception as e:
            self._polling_status.handle_exception("device", e)
            self._handle_exceptions_for_oid(oid_name, e)
            return

        if stats:
            for ent in stats:
                index = ent.index
                if "index_transform" in self._config["oids"][oid_name]:
                    if ent.index in self._config["oids"][oid_name]["index_transform"]:
                        index = self._config["oids"][oid_name]["index_transform"][ent.index]
                device_metrics_map[index] = ent.value
            self._oid_maps[oid_name] = device_metrics_map
            self._handle_successes_for_oid(oid_name)
        else:
            panoptes_metrics_exception = metrics.PanoptesMetricsNullException()
            self._handle_exceptions_for_oid(oid_name, panoptes_metrics_exception)

    def _build_map_by_get(self, oid_name):
        """
        Builds the oids_map for the provided oid_name using snmp get.
        Args:
            oid_name: The oid for which to build the map using snmp get.
        Returns:
            None
        """
        stat = None
        try:
            if self._config["oids"][oid_name]["method"] == "get":
                stat = self._snmp_connection.get(oid=self._config["oids"][oid_name]["oid"])
        except Exception as e:
            self._polling_status.handle_exception("device", e)
            self._handle_exceptions_for_oid(oid_name, e)
            return
        finally:
            # populate snmpget_oid_map keys, as it is relied on in _parse_expression
            self._snmpget_oid_map[oid_name] = None

        if stat:
            self._snmpget_oid_map[oid_name] = stat.value
            self._handle_successes_for_oid(oid_name)
        else:
            panoptes_metrics_exception = metrics.PanoptesMetricsNullException()
            self._handle_exceptions_for_oid(oid_name, panoptes_metrics_exception)

    def _get_config(self):
        """Get the enrichment specs for the plugin either from file or from key value store."""
        if self._enrichment and self._plugin_context.config['enrichment'].get('file'):
            raise enrichment.PanoptesEnrichmentCacheError("Enrichment defined in both config and via Key-Value store.")

        if self._enrichment:
            self._config = self._enrichment.get_enrichment_value('self', self._namespace, self._host)
        else:
            self._read_enrichment()

    def _process_config(self):
        """Normalize the enrichment configuration collected in _get_config."""
        processed_metrics_groups = list()

        for metrics_group_map in self._config["metrics_groups"]:
            processed_metrics_group_map = metrics_group_map
            for targets_type in ["metrics", "dimensions"]:
                for target, target_map in metrics_group_map[targets_type].items():
                    target_map = self._process_shorthand(target_map)
                    target_map = self._add_defaults(targets_type, target_map)
                    processed_metrics_group_map[targets_type][target] = target_map

                    if "ignore_empty_dimensions" not in processed_metrics_group_map:
                        processed_metrics_group_map["ignore_empty_dimensions"] = False

            processed_metrics_groups.append(processed_metrics_group_map)

        self._config["metrics_groups"] = processed_metrics_groups

    def _get_oids(self):
        """Collect and populate the oid maps according to the methods specified in the enrichment configuration."""
        self._oid_maps = dict()
        self._snmpget_oid_map = dict()
        for oid_name in self._config["oids"].keys():
            if self._config["oids"][oid_name]["method"] == "static":
                self._oid_maps[oid_name] = self._config["oids"][oid_name]["values"]
            elif self._config["oids"][oid_name]["method"] in ["bulk_walk", "get"]:
                self._build_map(oid_name)
            else:
                raise ValueError('self._config["oids"][oid_name]["method"] for oid_name: %s is not "static", '
                                 '"bulk_walk", or "get". It is %s' % (oid_name,
                                                                      self._config["oids"][oid_name]["method"]))

    def _parse_expression(self, raw_expression):
        """
        Translate the provided expression into python-executable code for this plugin.
        Args:
            raw_expression (str): The non-executable string to be translated
        Returns:
            The raw expression parsed into python-executable code that references the relevant oid_maps and/or indices
            therein.
        """
        tokens = str(raw_expression).split()
        parsed_expression = ""
        for token in tokens:
            match = _TABLE_PATTERN.search(token)
            if match:
                source_table = match.group(1)
                if source_table in self._oid_maps:
                    token = token.replace(source_table, 'self._oid_maps["' + source_table + '"]')
            if token in self._snmpget_oid_map.keys():
                token = token.replace(token, 'self._snmpget_oid_map["' + token + '"]')
            token = token.replace('.$index', '[index]')
            token = token.replace('$index', 'index')
            parsed_expression += token + " "

        return parsed_expression.rstrip()

    def _get_first_table_reference(self, value):
        """Given a string "value", return the first oid map (table) referenced in value."""
        match = _TABLE_PATTERN.search(value)
        if match:
            source_table = match.group(1)
            if source_table in self._oid_maps:
                return source_table

    def _get_indices_from_table(self, reference_table):
        if reference_table in self._oid_maps:
            return [x for x in self._oid_maps[reference_table].keys()]

    def _get_indices(self, target_map):
        """
        Get a list of the indices to be used for querying the oids specified in target_map.
        Args:
            target_map: The dimensions map or metrics map containing the oids to be queried.
        Returns:
            The list of indices to be used in querying the oids specified in target_map.
        """
        indices = []
        if "indices" in target_map:
            indices = target_map['indices']
        elif "indices_from" in target_map:
            indices = self._get_indices_from_table(target_map['indices_from'])
        else:  # Use the first table's indices
            source_table = self._get_first_table_reference(str(target_map['value']))
            if source_table:
                if self._oid_maps[source_table]:
                    indices = [x for x in self._oid_maps[source_table].keys()]
        return indices

    def _has_indices(self, target_map):
        """
        Returns if target_map is a 'top level' map or indexed map
        Args:
            target_map: the map to evaluate
        Returns:
            Is target_map "top level" or indexed?
        """
        if "indices" in target_map or "indices_from" in target_map:
            return True

        if "$index" not in str(target_map['value']):
            return False

        source_table = self._get_first_table_reference(str(target_map['value']))
        if source_table:
            if source_table in self._oid_maps:
                return True

        return False

    def _process_shorthand(self, value):
        """
        Parses format for metrics and dimensions with default values.
        Args:
            value: Either a dict representing the target_map, or the intended value for "value" in the target_map
        Returns:
            dict: target_map with updated values for metrics/dimensions
        """
        target_map = dict()

        if isinstance(value, dict):
            target_map = value
        else:
            target_map['value'] = value

        if 'type' not in target_map:
            if isinstance(value, int):
                target_map['type'] = 'integer'
            elif isinstance(value, float):
                target_map['type'] = 'float'
            elif isinstance(value, long):
                target_map['type'] = 'long'

        return target_map

    @staticmethod
    def _add_defaults_to_metric_map(metric_map):
        """
        Populate metric_map with default values if not already provided.
        Args:
            metric_map: the metric_map to which to assign default values
        Returns:
            The updated metric_map.
        """
        if 'type' not in metric_map:
            metric_map['type'] = 'integer'
        if 'metric_type' not in metric_map:
            metric_map['metric_type'] = 'gauge'

        return metric_map

    @staticmethod
    def _add_defaults_to_dimension_map(dimension_map):
        """
        Populate dimension_map with default values if not already provided.
        Args:
            dimension_map: the dimension_map to which to assign default values
        Returns:
            The updated dimension_map.
        """
        if 'type' not in dimension_map:
            dimension_map['type'] = 'string'
        return dimension_map

    def _add_defaults(self, targets_type, target_map):
        """
        Add defaults to the provided metrics map or dimensions map as appropriate.
        Args:
            targets_type: "metrics" or "dimensions"
            target_map: The metrics map or dimensions map to which to add default values.
        Returns:
            The updated target map.
        """
        if targets_type == "metrics":
            target_map = self._add_defaults_to_metric_map(target_map)
        elif targets_type == "dimensions":
            target_map = self._add_defaults_to_dimension_map(target_map)
        else:
            self._logger.warn('Error on "%s" (%s) in namespace "%s": '
                              '"target" must be of type "metrics" or "dimensions" but instead is of type "%s"' %
                              (self._host, self._device_model, self._namespace, targets_type))
            raise Exception('Error on "%s" (%s) in namespace "%s": '
                            '"target" must be of type "metrics" or "dimensions" but instead is of type "%s"' %
                            (self._host, self._device_model, self._namespace, targets_type))

        return target_map

    def _process_metrics_or_dimensions(self, targets_type, metrics_group_map):
        """
        Process the metrics or dimensions passed in for the metrics_group_map provided.
        Args:
            targets_type: 'metrics' or 'dimensions'
            metrics_group_map: The metrics_group_map defining the metrics group that should be built.
        Returns:
            The targets_map (either a dimensions map or metrics map), metrics_type_map, and
            "top_level" dimensions or metrics map.
        """
        targets_map = dict()
        top_level_targets_map = dict()
        metrics_type_map = dict()

        for target, target_map in metrics_group_map[targets_type].items():
            transform = _identity
            indices = self._get_indices(target_map)

            if "transform" in target_map:
                try:
                    transform = eval(target_map['transform'])
                except Exception as e:
                    self._logger.warn('Error on "%s" (%s) in namespace "%s" while evaluating '
                                      '"transform": %s: %s' %
                                      (self._host, self._device_model, self._namespace,
                                       target_map['transform'], repr(e)))
                    continue

            parsed_expression = self._parse_expression(target_map['value'])

            if targets_type == "metrics":
                # For non-indexed metrics, insert at top level of metrics_type_map
                metrics_type_map[target] = _METRIC_TYPE_MAP[target_map["metric_type"]]

            if self._has_indices(target_map):
                for index in indices:
                    try:
                        value = eval(parsed_expression)  # make sure ints are processed correctly

                        if index not in targets_map:
                            targets_map[index] = dict()
                        targets_map[index][target] = transform(_TYPE_MAPPING[target_map["type"]](value))
                    except Exception as e:
                        self._logger.warn('Error on "%s" (%s) in namespace "%s" while processing '
                                          'index "%s" for expression "%s": %s' %
                                          (self._host, self._device_model, self._namespace, index,
                                           parsed_expression, repr(e)))
                        continue
            else:
                try:
                    value = eval(parsed_expression)
                except Exception as e:
                    self._logger.warn('Error on "%s" (%s) in namespace "%s" while processing '
                                      'for expression "%s": %s' %
                                      (self._host, self._device_model, self._namespace,
                                       parsed_expression, repr(e)))
                    continue

                if not value:
                    self._logger.warn('While on "%s" (%s) in namespace "%s" while processing expression "%s",'
                                      ' value: %s was None' % (self._host, self._device_model, self._namespace,
                                                               parsed_expression, value))
                    continue
                top_level_targets_map[target] = transform(_TYPE_MAPPING[target_map["type"]](value))

        return targets_map, metrics_type_map, top_level_targets_map

    def _add_dimension_to_metrics_group_if_not_empty(self, dimension, value, metrics_group, dimension_was_empty):
        """
        Given a dimension and value for a metrics group, add to metrics group if value is not empty. Return the
        metrics_group and updated dimension_was_empty boolean.
        Args:
            dimension: The name of the dimension to add if not empty
            value: The value of the dimension to add if non-empty.
            metrics_group: The PanoptesMetricsGroup to which to add the dimension if non-empty.
            dimension_was_empty: Boolean representing whether the dimension value was empty.
        Returns:
            The updated metrics_group and the boolean denoting whether the dimension was empty.
        """
        if value != "":
            metrics_group.add_dimension(metrics.PanoptesMetricDimension(dimension, value))
        else:
            dimension_was_empty = True

        return metrics_group, dimension_was_empty

    def _add_top_level_metrics_and_dimensions_to_metrics_group(self, top_level_metrics_map, top_level_dimensions_map,
                                                               metrics_type_map, metrics_group, dimension_was_empty):
        """
        Add all top-level metrics and dimensions in the provided maps to the provided metrics_group.
        Args:
            top_level_metrics_map: The top level metrics map with values to add to the provided metrics_group.
            top_level_dimensions_map: The top level dimensions map with values to add to the provided metrics_group.
            metrics_type_map: The map containing the type values for each metric.
            metrics_group: The metrics_group to which to add metrics and dimensions.
            dimension_was_empty: Boolean representing whether an empty dimension has been found.
        Returns:
             The updated metrics_group and dimension_was_empty boolean.
        """
        for metric in top_level_metrics_map:
            metrics_group.add_metric(
                metrics.PanoptesMetric(metric, top_level_metrics_map[metric], metrics_type_map[metric]))
        # "top_level" metrics don't have indices, so only non-indexed dimensions can be added
        for dimension, value in top_level_dimensions_map.items():
            metrics_group, dimension_was_empty = self._add_dimension_to_metrics_group_if_not_empty(dimension,
                                                                                                   value,
                                                                                                   metrics_group,
                                                                                                   dimension_was_empty)

        return metrics_group, dimension_was_empty

    def _add_metrics_group_if_allowed(self, metrics_group, metrics_group_map, metrics_group_name, dimension_was_empty):
        """
        Add populated, provided metrics_group to the plugin's PanoptesMetricsGroupSet if no dimension was empty or if
        'ignore_empty_dimensions flag is True. Otherwise, handle a PanoptesMetricDimensionNullException for the
        metrics group in the polling status.
        Args:
            metrics_group: The metrics_group to add to self._metrics if allowed.
            metrics_group_map: The metrics_group_map for which to check "ignore_empty_dimensions" for the
                provided metrics_group.
            metrics_group_name: The name of the metrics_group provided.
            dimension_was_empty: Was an empty dimension found?
        """
        if metrics_group_map['ignore_empty_dimensions']:
            self._metrics.add(metrics_group)
        elif not dimension_was_empty:
            self._metrics.add(metrics_group)
        else:
            self._polling_status.handle_exception(metrics_group_name, PanoptesMetricDimensionNullException())

    def _process_metrics(self):
        """
        If values for the provided oids in the enrichment configuration have been found, then form and populate metrics
        groups and add to the plugin's PanoptesMetricsGroupSet.
        """
        if not self._oid_maps and not self._snmpget_oid_map:
            raise ValueError("self._oid_maps and self._snmpget_oid_map are empty or None.")

        for metrics_group_map in self._config["metrics_groups"]:
            dimension_was_empty = False
            metrics_group_name = metrics_group_map["group_name"]
            metrics_map, metrics_type_map, top_level_metrics_map = self._process_metrics_or_dimensions(
                targets_type="metrics", metrics_group_map=metrics_group_map)

            dimensions_map, _, top_level_dimensions_map = self._process_metrics_or_dimensions(
                targets_type="dimensions", metrics_group_map=metrics_group_map)

            if len(metrics_map) > 0:
                for index in metrics_map:
                    metrics_group = metrics.PanoptesMetricsGroup(self._resource, metrics_group_name,
                                                                 self._execute_frequency)
                    for metric, value in metrics_map[index].items():
                        if metric in metrics_type_map:
                            metrics_group.add_metric(
                                metrics.PanoptesMetric(metric, value, metrics_type_map[metric]))
                    if index in dimensions_map:
                        for dimension, value in dimensions_map[index].items():
                            metrics_group, dimension_was_empty = \
                                self._add_dimension_to_metrics_group_if_not_empty(dimension,
                                                                                  value,
                                                                                  metrics_group,
                                                                                  dimension_was_empty)
                    for dimension, value in top_level_dimensions_map.items():
                        metrics_group, dimension_was_empty = self._add_dimension_to_metrics_group_if_not_empty(
                            dimension,
                            value,
                            metrics_group,
                            dimension_was_empty)

                    # "top level" metrics
                    if len(top_level_metrics_map) > 0:
                        metrics_group, dimension_was_empty = \
                            self._add_top_level_metrics_and_dimensions_to_metrics_group(
                                top_level_metrics_map,
                                top_level_dimensions_map,
                                metrics_type_map,
                                metrics_group,
                                dimension_was_empty)

                    self._add_metrics_group_if_allowed(metrics_group, metrics_group_map, metrics_group_name,
                                                       dimension_was_empty)

                metrics_group = metrics.PanoptesMetricsGroup(self._resource, metrics_group_name,
                                                             self._execute_frequency)
                if len(top_level_metrics_map) > 0:
                    metrics_group, dimension_was_empty = \
                        self._add_top_level_metrics_and_dimensions_to_metrics_group(top_level_metrics_map,
                                                                                    top_level_dimensions_map,
                                                                                    metrics_type_map,
                                                                                    metrics_group,
                                                                                    dimension_was_empty)
                    if len(metrics_group.metrics) > 0:
                        self._add_metrics_group_if_allowed(metrics_group, metrics_group_map, metrics_group_name,
                                                           dimension_was_empty)

            else:  # Add only "top level" metrics for a given metrics group
                # "top level" metrics
                metrics_group = metrics.PanoptesMetricsGroup(self._resource, metrics_group_name,
                                                             self._execute_frequency)

                if len(top_level_metrics_map) > 0:
                    metrics_group, dimension_was_empty = \
                        self._add_top_level_metrics_and_dimensions_to_metrics_group(top_level_metrics_map,
                                                                                    top_level_dimensions_map,
                                                                                    metrics_type_map,
                                                                                    metrics_group,
                                                                                    dimension_was_empty)

                self._add_metrics_group_if_allowed(metrics_group, metrics_group_map, metrics_group_name,
                                                   dimension_was_empty)

    def _read_enrichment(self):
        """Get enrichment from file or from key-value store as configured."""
        try:
            enrichment_file = self._plugin_context.config['enrichment']['file']
        except:
            raise PanoptesEnrichmentFileEmptyError("Enrichment file not specified in configuration file.")

        try:
            with open(enrichment_file) as f:
                self._config = json.load(f)
        except Exception as e:
            raise panoptes_base_plugin.PanoptesPluginConfigurationError("Failure trying to read JSON from file %s: %s" %
                                                                        (enrichment_file, repr(e)))

    def get_results(self):
        """See base class."""
        self._polling_status = polling_status.PanoptesPollingStatus(resource=self._resource,
                                                                    execute_frequency=self._execute_frequency,
                                                                    logger=self._logger,
                                                                    metric_name=self._polling_status_metric_name)
        try:
            self._get_config()
            self._process_config()

            start_time = time.time()
            self._get_oids()
            end_time = time.time()

            self._logger.info('SNMP calls for device %s completed in %.2f seconds' % (
                self.host, end_time - start_time))

            self._process_metrics()
        except Exception as e:
            self._polling_status.handle_exception('device', e)
        finally:
            self._metrics.add(self._polling_status.device_status_metrics_group)
            return self._metrics

    def run(self, context):
        self._resource = context.data
        self._device_model = self._resource.resource_metadata.get('model', 'unknown')
        if 'enrichment_schema_version' in context.config['main']:
            self._enrichment_schema_version = context.config['main']['enrichment_schema_version']
        else:
            self._enrichment_schema_version = '0.1'
        self._namespace = context.config['main']['namespace']
        self._polling_status_metric_name = context.config['main']['polling_status_metric_name']

        return super(PluginPollingGenericSNMPMetrics, self).run(context)
