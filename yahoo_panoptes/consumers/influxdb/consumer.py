"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
from __future__ import print_function

from builtins import range
from builtins import object
import sys
import argparse
import os
import signal
import time
import traceback
import re
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError

from yahoo_panoptes.framework import const
from yahoo_panoptes.framework.context import PanoptesContext
from yahoo_panoptes.framework.resources import PanoptesResourcesKeyValueStore
from yahoo_panoptes.framework.utilities.consumer import PanoptesConsumer, make_topic_names_for_all_sites, \
    get_consumer_type_from_name
from yahoo_panoptes.framework.utilities.helpers import parse_config_file, get_client_id


METRICS_TYPE_SUPPORTED = [u'gauge', u'counter']
DEFAULT_CONFIG_FILE = u'/home/panoptes/conf/influxdb_consumer.ini'


class PanoptesInfluxDBConsumerContext(PanoptesContext):  # pragma: no cover
    """
    This class implements a PanoptesContext without any KV stores, producers or ZK client
    """
    def __init__(self):
        super(PanoptesInfluxDBConsumerContext, self).__init__(
                key_value_store_class_list=[PanoptesResourcesKeyValueStore],
                create_message_producer=False, async_message_producer=False, create_zookeeper_client=False)


class PanoptesInfluxDBConnection(InfluxDBClient):
    """
    Class to create InfluxDB client connection
    """
    def __init__(self, host, port, database, retries, timeout, pool_size):
        super(PanoptesInfluxDBConnection, self).__init__(
            host=host, port=port, database=database, retries=retries, timeout=timeout, pool_size=pool_size)


class PanoptesInfluxDBDefaultTransformer(object):
    """
    This class implements Panoptes Metrics Group to InfluxDB line protocol points transformation
    """
    def __init__(self, metrics_group):
        self._resource = metrics_group[u'resource']
        self._metrics_group = metrics_group

    @property
    def resource(self):
        """
        Returns:
        dict: The resource for the incoming metrics group
        """
        return self._resource

    @property
    def metrics_group(self):
        """
        Returns:
        dict: The incoming metrics group
        """
        return self._metrics_group

    @property
    def measurement(self):
        """
        This function provides a default implementation of the measurement name for InfluxDB.

        Users should override this if they want to change the application name

        Returns:
            str: The measurement name for InfluxDB
        """
        measurement = self._metrics_group[u'metrics_group_type']
        influx_measurement_string_regex = re.compile(r'[^0-9a-zA-Z_]+')

        return influx_measurement_string_regex.sub(u'_', measurement)

    @property
    def timestamp(self):
        """
        This function provides a default implementation of the timestamp - copies in the
        metrics_group_creation_timestamp

        Users should override this if they want to change the timestamp

        Returns:
            float: The Unix epoch timestamp in seconds for the points being emitted
        """
        return int(self._metrics_group[u'metrics_group_creation_timestamp'])

    @property
    def tag_set(self):
        """
        This function provides a default implementation of the tags needed by InfluxDB. It 'flattens' the incoming
        dimensions into comma separated name/value tag sets. It also adds three 'special' dimensions (resource_endpoint,
        resource_site, resource_class, resource_subclass, resource_type and host) taken from the incoming resource
        information

        Users should override this if they want to change the dimensions

        Returns:
            str: Comma separated name/value tags
        """
        tags = {d[u'dimension_name']: d[u'dimension_value'] for d in self.metrics_group[u'dimensions']}

        tags[u'resource_class'] = self.resource[u'resource_class']
        tags[u'resource_subclass'] = self.resource[u'resource_subclass']
        tags[u'resource_type'] = self.resource[u'resource_type']
        tags[u'resource_endpoint'] = self.resource[u'resource_endpoint']
        tags[u'resource_site'] = self.resource[u'resource_site']

        tag_set = u','.join(u"{!s}={!s}".format(key, self._escape_influx_special_char(val))
                            for (key, val) in sorted(tags.items()))

        return tag_set

    @property
    def field_set(self):
        """
        This function provides a default implementation of the fields needed by InfluxDB. It 'flattens' the incoming
        metrics into comma separated name/value field sets.

        Users should override this if they want to change the metrics

        Returns:
            str: comma separated name/value fields
        """
        fields = {d[u'metric_name'] + u'__' + d[u'metric_type']: d[u'metric_value'] for d in
                  self.metrics_group[u'metrics'] if d[u'metric_type'] in METRICS_TYPE_SUPPORTED}

        field_set = u','.join(u"{!s}={!r}".format(key, val) for (key, val) in sorted(fields.items()))

        return field_set

    @staticmethod
    def _escape_influx_special_char(string):
        """
        Method to escape InfluxDB points line protocol special characters

        Args:
            string: InfluxDB field or tag string

        Returns:
            str: field or tag string with special chars escaped
        """
        influx_special_char = re.compile(r'([,=\s])')
        return influx_special_char.sub(r'\\\1', string)

    def translate_to_influxdb_points(self):
        """
        Method to construct InfluxDB line protocol formatted string

        Returns:
            str: InfluxDB line protocol formatted string
            (measurement,tag1=text1,tag2=text2 field1=val1,field2=val2 timestamp)
        """

        return u'{},{} {} {}'.format(self.measurement, self.tag_set, self.field_set, self.timestamp)


class PanoptesInfluxDBConsumer(object):
    CONFIG_SPEC_FILE = os.path.dirname(os.path.realpath(__file__)) + u'/influxdb_consumer_configspec.ini'

    def __init__(self, config_file):
        """
        This is the base class for a Panoptes to InfluxDB consumer. It reads metrics from Kafka and sends them to
        InfluxDB write api.

        This class defines default implementations of converting Panoptes Metric Group to InfluxDB line protocol
        string format.

        measurement,tag1=text1,tag2=text2 field1=val1,field2=val2 timestamp

        It is expected that users would inherit and extend these methods if they wish to intercept
        the data in any way before sending to InfluxDB.

        The emission logic has fault tolerance and retries built in.

        Args:
            config_file (str): The name of config file

        Attributes:
            CONFIG_SPEC_FILE: The configspec file to use with configobj - please don't change this till you know what
            you're doing
        """
        try:
            self._config = parse_config_file(config_file, self.CONFIG_SPEC_FILE)
        except Exception as e:
            sys.exit(u'Error parsing configuration file: {}'.format(repr(e)))

        try:
            self._panoptes_context = PanoptesInfluxDBConsumerContext()
        except Exception as e:
            sys.exit(u'Could not create a InfluxDB Context: {}'.format(repr(e)))

        self._logger = self._panoptes_context.logger
        self._install_signal_handlers()
        self.influxdb_connection = None

        self._initialize_influxdb_connection()

        topics = make_topic_names_for_all_sites(self._panoptes_context, self._config[u'kafka'][u'queue'])
        client_id = get_client_id(prefix=self._config[u'kafka'][u'group_id'])

        self._consumer = PanoptesConsumer(self._panoptes_context,
                                          consumer_type=get_consumer_type_from_name(self._config[u'kafka'][u'queue']),
                                          topics=topics,
                                          keys=None,
                                          client_id=client_id,
                                          group=self._config[u'kafka'][u'group_id'],
                                          poll_timeout=self._config[u'kafka'][u'poll_timeout'],
                                          session_timeout=self._config[u'kafka'][u'session_timeout'],
                                          max_poll_records=self._config[u'kafka'][u'max_poll_records'],
                                          callback=self._process_message,
                                          validate=False)

        self.influxdb_points = set()
        self._last_emitted = 0
        self.influxdb_points_batch_size = 0

        self._consumer.start_consumer()

    def _initialize_influxdb_connection(self):
        """
        Method to initialize InfluxDB connection and creates database.

        Returns:
            None
        """
        logger = self._logger

        self.influxdb_connection = PanoptesInfluxDBConnection(
            host=self._config[u'influxdb'][u'host'],
            port=self._config[u'influxdb'][u'port'],
            database=self._config[u'influxdb'][u'database'],
            retries=self._config[u'influxdb'][u'write_api_connect_retries'],
            timeout=self._config[u'influxdb'][u'write_api_connect_timeout'],
            pool_size=self._config[u'influxdb'][u'write_api_connection_pool_size']
        )

        try:
            if self.influxdb_connection.ping():
                logger.info(u'Successfully initialized InfluxDB API connection')
        except:
            logger.error(u'Error trying to initialize InfluxDB API connection, exiting.')
            sys.exit(1)

        database_list = [db_entry[u'name'] for db_entry in self.influxdb_connection.get_list_database()]

        if self._config[u'influxdb'][u'database'] in database_list:
            logger.info(u'Influxdb database {!r} already created..skipping'
                        .format(self._config[u'influxdb'][u'database']))
        else:
            try:
                logger.info(u'Creating InfluxDB database {!r}'.format(self._config[u'influxdb'][u'database']))
                self.influxdb_connection.create_database(self._config[u'influxdb'][u'database'])
            except Exception as e:
                logger.error(u'Failed while creating InfluxDB database {!r}: {}'.
                             format(self._config[u'influxdb'][u'database'], repr(e)))
                sys.exit(1)

    @classmethod
    def factory(cls):
        """
        This function parses command line arguments and creates a InfluxDB consumer object with the config file provided
        on the command line

        Returns:
            PanoptesInfluxDBConsumer: InfluxDB consumer object with the config file provided
        """
        parser = argparse.ArgumentParser(description=u'Consume metrics from Panoptes and send them to InfluxDB')

        parser.add_argument(u'--config',
                            help=u'Configuration file to use for the consumer. Default: {}'.format(DEFAULT_CONFIG_FILE),
                            default=DEFAULT_CONFIG_FILE)
        try:
            # Using parse_known_args is a hack to get the tests to work under nose
            # https://stackoverflow.com/questions/28976912/how-to-use-nosetests-in-python-while-also-passing
            # -accepting-arguments-for-argpar
            args = parser.parse_known_args()
        except Exception as e:
            sys.exit(u'Error parsing command line options or configuration file: {}'.format(repr(e)))

        try:
            return cls(args[0].config)
        except Exception as e:
            sys.exit(u'Error trying to instantiate class: {}'.format(repr(e)))

    def _clear_metrics(self, current_time):
        self.influxdb_points = set()
        self.influxdb_points_batch_size = 0
        self._last_emitted = current_time

    def _send_one_by_one(self):
        """
        This function attempts to send metrics to InfluxDB one by one, instead of a batch. It skips any
        metrics that fail.
        If it fails to emit *all* metric, it will return False - to retry the points - this is done based on the
        assumption that all emission fails, it is probably a transient InfluxDB write api unavailability

        Returns:
            bool: True if it was able to emit some metrics to InfluxDB, false otherwise
        """
        logger = self._logger
        points_skipped = 0

        logger.warn(u'Client error trying to send {} points to InfluxDB api, going to send each point individually'.
                    format(len(self.influxdb_points)))

        for point in self.influxdb_points:
            try:
                self.influxdb_connection.write_points([point], time_precision=u's', protocol=u'line')
                logger.info(u'Successfully sent a point of {} bytes'.format(sys.getsizeof(point)))
            except Exception as e:
                points_skipped += 1
                logger.error(u'Failed while trying to send point: {}'.format(repr(e)))

        if points_skipped == len(self.influxdb_points):
            logger.error(u'Unable to emit any metric to InfluxDB api, will retry')
            return False
        else:
            logger.info(u'Successfully sent {} points to InfluxDB api and failed {} points'.
                        format(len(self.influxdb_points) - points_skipped, points_skipped))
            self._clear_metrics(self.current_time)
            return True

    def _send_to_influxdb(self, point):
        """
        This method attempts to send points to InfluxDB write api in batch and retries incase of failures.
        It calls _send_one_by_one method when it gets response code 400 (unable to parse).

        Args:
            point(str): InfluxDB points string

        Returns:
            bool: True if it was able to emit some metrics to YAMAS2, false otherwise
        """
        logger = self._logger

        self.influxdb_points.add(point)
        self.influxdb_points_batch_size = len(self.influxdb_points)

        time_over_emit_interval = round(self.current_time - self._last_emitted)

        if self.influxdb_points_batch_size >= self._config[u'influxdb'][u'write_api_batch_size'] \
                or time_over_emit_interval >= self._config[u'influxdb'][u'write_api_max_emit_interval']:

            logger.debug(u'Going to send {} bytes to InfluxDB api ({} points, {}s over emit interval)'
                         .format(sys.getsizeof(self.influxdb_points),
                                 len(self.influxdb_points), time_over_emit_interval))

            for retry in range(0, self._config[u'influxdb'][u'write_api_commit_retries']):
                try:
                    self.influxdb_connection.write_points(list(self.influxdb_points), time_precision=u's',
                                                          protocol=u'line',
                                                          batch_size=self._config[u'influxdb'][u'write_api_batch_size'])
                    logger.debug(u'Successfully bulk sent {} points to InfluxDB API'.format(len(self.influxdb_points)))
                    self._clear_metrics(self.current_time)
                    break
                except InfluxDBClientError as e:
                    logger.exception(u'Failed while trying to send {} bytes ({} points)'.
                                     format(sys.getsizeof(self.influxdb_points),
                                            len(self.influxdb_points)))

                    if e.code == 400:
                        if self._send_one_by_one():
                            break
                        else:
                            continue
                except Exception as e:
                    logger.exception(u'Failed while trying to send {} bytes ({} points): {}'.
                                     format(sys.getsizeof(self.influxdb_points), len(self.influxdb_points), repr(e)))
                    continue

            # Return False to Kafka consumer once we have points above write_api_batch_size in buffer and
            # not able to send *any* of them
            if len(self.influxdb_points) > self._config[u'influxdb'][u'write_api_batch_size']:
                logger.warn(u'Retries failed, will try again after backoff interval {}s'.
                            format(self._config[u'influxdb'][u'write_api_fail_backoff_interval']))
                time.sleep(self._config[u'influxdb'][u'write_api_fail_backoff_interval'])
                return False

        return True

    def _process_message(self, key, metrics_group):
        """
        This method is the workhorse of the InfluxDB consumer class. It receives incoming metrics groups from Kafka,
        translates them and emits them to InfluxDB write api

        Args:
            key (str): The key for incoming the message
            metrics_group (dict): The dictionary representation of a metrics group

        Returns:
            bool: True
        """
        logger = self._logger
        self._key = key
        self._metrics_group = metrics_group
        self.current_time = time.time()
        status = False

        try:
            influxdb_point = PanoptesInfluxDBDefaultTransformer(metrics_group).translate_to_influxdb_points()
        except Exception as e:
            logger.error(u'Failed while transforming influxDB points from Panoptes metrics group {}: {}'.
                         format(metrics_group, repr(e)))
            return True

        if influxdb_point:
            status = self._send_to_influxdb(influxdb_point)

        return status

    def _signal_handler(self, signal_number, _):  # pragma: no cover
        print(u'Caught {}, shutting down InfluxDB Consumer'.format(const.SIGNALS_TO_NAMES_DICT[signal_number]))
        print(u'Going to shutdown Kafka consumer')
        try:
            self._consumer.stop_consumer()
        except Exception as e:
            print(u'Error trying to stop Kafka consumer, shutting down anyway: {}'.format(repr(e)))
        print(u'Shutdown complete, exiting')
        sys.exit(0)

    def _install_signal_handlers(self):
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGHUP, self._signal_handler)


def start():  # pragma: no cover
    PanoptesInfluxDBConsumer.factory()
