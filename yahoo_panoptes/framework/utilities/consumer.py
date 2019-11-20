"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
from __future__ import division
from builtins import range
from past.utils import old_div
from builtins import object
import json
import sys
import time

from json_schema_validator.errors import ValidationError
from json_schema_validator.schema import Schema
from json_schema_validator.validator import Validator
import kafka
from kafka.common import OffsetAndMetadata

from yahoo_panoptes.framework.context import PanoptesContext
from yahoo_panoptes.framework.validators import PanoptesValidators


class PanoptesConsumerTypes(object):
    """
    This class contains the supported consumer types as attributes
    """
    METRICS, RESOURCES, PROCESSED = list(range(3))


CONSUMER_TYPE_NAMES = dict((getattr(PanoptesConsumerTypes, n), n.lower())
                           for n in dir(PanoptesConsumerTypes) if u'_' not in n)


def make_topic_names_for_all_sites(panoptes_context, topic_suffix):
    delimiter = panoptes_context.config_dict[u'kafka'][u'topic_name_delimiter']
    return [delimiter.join([site, topic_suffix]) for site in panoptes_context.config_object.sites]


def get_consumer_type_from_name(name):
    return getattr(PanoptesConsumerTypes, name.upper())


class PanoptesConsumerRecordValidator(object):
    """
    This class implements validators for various consumer types
    """

    _metrics_schema = Schema(
            {
                u"$schema": u"http://json-schema.org/draft-04/schema#",
                u"type": u"object",
                u"properties": {
                    u"metrics": {
                        u"type": u"array",
                        u"items": [{
                            u"type": u"object",
                            u"properties": {
                                u"metric_name": {u"type": u"string"},
                                u"metric_value": {u"type": u"number"},
                                u"metric_type": {u"type": u"string", u"enum": [u"gauge", u"counter"]},
                                u"metric_creation_timestamp": {u"type": u"number"}
                            },
                            u"required": [u"metric_name", u"metric_value", u"metric_type", u"metric_creation_timestamp"]
                        }],
                        u"minItems": 1
                    },
                    u"dimensions": {
                        u"type": u"array",
                        u"items": [{
                            u"type": u"object",
                            u"properties": {
                                u"dimension_name": {u"type": u"string"},
                                u"dimension_value": {u"type": u"string"}
                            },
                            u"required": [u"dimension_name", u"dimension_value"]
                        }]
                    },
                    u"resource": {
                        u"type": u"object",
                        u"properties": {
                            u"resource_site": {u"type": u"string"},
                            u"resource_class": {u"type": u"string"},
                            u"resource_subclass": {u"type": u"string"},
                            u"resource_type": {u"type": u"string"},
                            u"resource_id": {u"type": u"string"}
                        },
                        u"required": [u"resource_site", u"resource_class", u"resource_subclass", u"resource_type",
                                      u"resource_id"]
                    },
                    u"metrics_group_type": {u"type": u"string"},
                    u"metrics_group_interval": {u"type": u"number"},
                    u"metrics_group_creation_timestamp": {u"type": u"number"},
                    u"metrics_group_schema_version": {u"type": u"string", u"enum": [u"0.2"]}
                },
                u"required": [u"metrics", u"dimensions", u"resource", u"metrics_group_type", u"metrics_group_interval",
                              u"metrics_group_creation_timestamp", u"metrics_group_schema_version"]
            }
    )

    _resource_schema = Schema(
            {
                u"$schema": u"http://json-schema.org/draft-04/schema#",
                u"type": u"object",
                u"properties": {
                    u"resources": {
                        u"type": u"array",
                        u"items": [{
                            u"type": u"object",
                            u"properties": {
                                u"resource_site": {u"type": u"string"},
                                u"resource_class": {u"type": u"string"},
                                u"resource_subclass": {u"type": u"string"},
                                u"resource_type": {u"type": u"string"},
                                u"resource_id": {u"type": u"string"},
                                u"resource_endpoint": {u"type": u"string"},
                                u"resource_creation_timestamp": {u"type": u"number"},
                                u"resource_metadata": {
                                    u"type": u"object",
                                    u"patternProperties": {
                                        u"^[a-zA-Z0-9][a-zA-Z0-9]*$": {u"type": u"string"}
                                    }
                                }
                            },
                            u"required": [u"resource_site", u"resource_class", u"resource_subclass", u"resource_type",
                                          u"resource_id", u"resource_endpoint", u"resource_creation_timestamp"]
                        }],
                        u"minItems": 1
                    },
                    u"resource_set_creation_timestamp": {u"type": u"number"},
                    u"resource_set_schema_version": {u"type": u"string", u"enum": [u"0.1"]}
                },
                u"required": [u"resources", u"resource_set_creation_timestamp", u"resource_set_schema_version"]
            }
    )

    @staticmethod
    def validate_metrics(consumer_record):
        """
        This method validates that the passed consumer record  is a valid metrics group record

        Args:
            consumer_record (str): A valid JSON string representation of the consumer record

        Returns:
            bool: True if validation passes, False otherwise
        """
        try:
            return Validator.validate(PanoptesConsumerRecordValidator._metrics_schema, consumer_record)
        except ValidationError:
            return False

    @staticmethod
    def validate_resources(consumer_record):
        """
        This method validates that the passed consumer record  is a valid resources record

        Args:
            consumer_record (str): A valid JSON string representation of the consumer record

        Returns:
            bool: True if validation passes, False otherwise
        """
        try:
            return Validator.validate(PanoptesConsumerRecordValidator._resource_schema, consumer_record)
        except ValidationError:
            return False

    @staticmethod
    def validate(consumer_type, consumer_record):
        """
        This method is the 'entry point' for this class and routes the validation to type specific methods

        Args:
            consumer_type (int): A valid consumer type
            consumer_record (str): A valid JSON string representation of the consumer record
        """
        if consumer_type == PanoptesConsumerTypes.METRICS:
            return PanoptesConsumerRecordValidator.validate_metrics(consumer_record)
        elif consumer_type == PanoptesConsumerTypes.RESOURCES:
            return PanoptesConsumerRecordValidator.validate_resources(consumer_record)
        elif consumer_type == PanoptesConsumerTypes.PROCESSED:
            return PanoptesConsumerRecordValidator.validate_metrics(consumer_record)
        else:
            return False


class PanoptesConsumer(object):
    def __init__(self, panoptes_context, consumer_type, topics, client_id, group, keys, poll_timeout, callback,
                 validate=False, session_timeout=60, max_poll_records=500, max_partition_fetch_bytes=1048576):
        """
        This class implements a helper/wrapper for writing Panoptes Consumers

        The consumer object produced by this class joins the group provided in the arguments, subscribes to the relevant
        topics (which are combination of the site name and consumer type), filters records by the provided keys, checks
        validity of the records by converting them to JSON and validating against consumer type specific schemas and
        then calls the callback function provided for each record.

        The consumer object also takes care of advancing the committed marker for each topic and partition - if the
        callback fails or returns false, the marker is not advanced.

        Args:
            panoptes_context (PanoptesContext): The PanoptesContext to be used by the consumer
            consumer_type (int): A valid consumer type
            topics (list): A valid consumer type
            client_id (str): A non-empty string that uniquely identifies the consumer instance - this is used for \
            logging by and group administration by Kafka
            group (str): The Kafka group this consumer should be a part of
            keys (list, None): A comma separated list of keys the consumer should filter against
            poll_timeout (int): A non-negative integer which is the interval (in seconds) the consumer should sleep if \
            no records are available on the Kafka bus
            callback (callable): A callable to which each processed, validated consumer records (deserialized JSON \
            object) should be passed. The callable should return false if it cannot process the record due to
            temporary \
            issues - it would be redelivered to the callback in that case
            validate (bool): Whether each incoming record should be validated. Defaults to False
            session_timeout (int): A non-negative integer which is the interval (in seconds) after which the Kafka \
            Group Management system should consider the client disconnected
            max_poll_records (int): The maximum number of records to fetch in one poll cycle

        Returns:
            None
        """
        assert isinstance(panoptes_context, PanoptesContext), u'panoptes_context must be an instance of PanoptesContext'
        assert consumer_type in CONSUMER_TYPE_NAMES, u'consumer_type must be an valid attribute from ' \
                                                     u'PanoptesConsumerTypes'
        assert PanoptesValidators.valid_nonempty_iterable_of_strings(topics), u''
        assert PanoptesValidators.valid_nonempty_string(client_id), u'client_id must be a non-empty string'
        assert keys is None or PanoptesValidators.valid_nonempty_iterable_of_strings(keys), \
            u'keys must be None or a list of non-empty strings'
        assert PanoptesValidators.valid_positive_integer(poll_timeout), u'poll_timeout must be an integer'
        assert hasattr(callback, u'__call__'), u'callback must be a callable'
        assert isinstance(validate, bool), u'validate must be a boolean'
        assert PanoptesValidators.valid_nonzero_integer(session_timeout), u'session_timeout must be an integer '\
                                                                          u'greater than zero'
        assert PanoptesValidators.valid_nonzero_integer(max_poll_records), u'max_poll_records must be an integer '\
                                                                           u'greater than zero'

        self._panoptes_context = panoptes_context
        self._consumer_type = consumer_type
        self._topics = topics
        self._client_id = client_id
        self._keys = keys
        self._group = group
        self._poll_timeout = poll_timeout * 1000
        self._session_timeout = session_timeout * 1000
        self._request_timeout = self._session_timeout * 3
        self._heartbeat_interval = old_div(self._session_timeout, 3)
        self._max_poll_records = max_poll_records
        self._max_partition_fetch_bytes = max_partition_fetch_bytes
        self._callback = callback
        self._validate = validate

        self._consumer = None
        self._last_polled = 0
        self._asked_to_stop = False

    @property
    def panoptes_context(self):
        """
        The Panoptes Context associated with this consumer

        Returns:
            PanoptesContext:  The Panoptes Context associated with this consumer
        """
        return self._panoptes_context

    @property
    def client_id(self):
        """
        The Client Id associated with this consumer

        Returns:
            str: The Client Id associated with this consumer
        """
        return self._client_id

    @property
    def group(self):
        """
        The Kafka group associated with this consumer

        Returns:
            str: The Kafka group associated with this consumer
        """
        return self._group

    @property
    def poll_timeout(self):
        """
        The Kafka poll timeout (in seconds) set for this consumer

        Returns:
            int: The Kafka poll timeout (in seconds) set for this consumer
        """
        return self._poll_timeout

    @property
    def consumer_type(self):
        """
        The type of consumer

        Returns:
            int: The type of consumer this object is
        """
        return self._consumer_type

    @property
    def keys(self):
        """
        The comma separated list of keys associated with this consumer

        Returns:
            str: he comma separated list of keys associated with this consumer
        """
        return self._keys

    def asked_to_stop(self):
        """

        Returns:
            boolean: True if the consumer has received SIGINT from daemon tools
=        """
        return self._asked_to_stop

    def start_consumer(self):
        """
        This method is the workhorse of this class - it starts the Kafka consumer and calls the callback function for
        each valid record
        """
        logger = self.panoptes_context.logger
        config = self.panoptes_context.config_object
        last_batch_size = 0
        logger.info(u'Trying to start Kafka Consumer with brokers: "%s", topics: "%s", group: "%s"' % (
            config.kafka_brokers, self._topics, self.group))

        try:
            consumer = kafka.KafkaConsumer(bootstrap_servers=config.kafka_brokers,
                                           client_id=self.client_id,
                                           group_id=self.group,
                                           enable_auto_commit=False,
                                           session_timeout_ms=self._session_timeout,
                                           request_timeout_ms=self._request_timeout,
                                           heartbeat_interval_ms=self._heartbeat_interval,
                                           max_poll_records=self._max_poll_records,
                                           max_partition_fetch_bytes=self._max_partition_fetch_bytes)
            consumer.subscribe(topics=self._topics)
            logger.info(u'Consumer subscribed to: %s' % consumer.subscription())
            self._consumer = consumer
        except Exception as e:
            sys.exit(u'Error trying to start Kafka consumer: %s' % str(e))

        while not self.asked_to_stop():
            poll_age = (time.time() - self._last_polled) * 1000
            if (poll_age > self._session_timeout) and (last_batch_size > 0):
                logger.warn(u'Poll cycle took %.2f ms for %d records, '
                            u'which is greater than the session timeout of %d ms' %
                            (poll_age, last_batch_size, self._session_timeout))

            try:
                topic_partitions = consumer.poll(timeout_ms=self._poll_timeout)
                self._last_polled = time.time()
                logger.debug(u'Poll returned with %d topic partitions' % len(topic_partitions))
            except Exception as e:
                logger.error(u'Error while polling: %s' % str(e))
                continue

            last_batch_size = 0
            for topic_partition in list(topic_partitions.keys()):
                consumer_records = topic_partitions[topic_partition]
                last_batch_size += len(consumer_records)

                logger.debug(u'Processing topic partition: %s, consumer records: %d, committed: %s' % (
                    str(topic_partition), len(consumer_records), consumer.committed(topic_partition)))
                logger.debug(u'Consumed offsets: %s' % consumer._subscription.all_consumed_offsets())

                callback_succeeded = True
                consumer_records_skipped = 0
                consumer_records_validation_failed = 0
                for consumer_record in consumer_records:

                    consumer_record_key = consumer_record.key.decode(u'utf-8')

                    logger.debug(u'Processing consumer record with key: "%s" and value: "%s"' % (
                        consumer_record_key, consumer_record.value))

                    if self.keys and consumer_record_key not in self.keys:
                        logger.debug(
                                u'Consumer record key "%s" does not match any of the provided keys, skipping' %
                                consumer_record_key)
                        consumer_records_skipped += 1
                        continue

                    try:
                        consumer_record_object = json.loads(consumer_record.value)
                    except Exception as e:
                        logger.warn(
                                u'Could not convert consumer record "%s" to JSON, skipping: %s' % (
                                    consumer_record.value, str(e)))
                        consumer_records_validation_failed += 1
                        continue

                    if self._validate:
                        if not PanoptesConsumerRecordValidator.validate(
                                self.consumer_type, consumer_record_object):
                            logger.debug(u'Consumer record failed validation, skipping')
                            consumer_records_validation_failed += 1
                            continue
                    try:
                        callback_succeeded = self._callback(consumer_record_key, consumer_record_object)
                        # If the callback fails even for one consumer record, we want to fail (not update the committed)
                        # offset for the entire the batch, so exit
                        if not callback_succeeded:
                            logger.error(u'Callback function returned false')
                            break
                    except:
                        logger.exception(u'Error trying to execute callback function')
                        break

                # Update the committed offset if the callback function succeeds for *all* consumer records in this topic
                # partition
                if callback_succeeded:
                    try:
                        position = consumer.position(topic_partition)
                    except Exception as e:
                        logger.error(
                                u'Error trying to fetch position for topic partition "%s": %s' % (
                                    topic_partition, str(e)))
                    else:
                        offset = {topic_partition: OffsetAndMetadata(offset=position, metadata='')}
                        logger.debug(u'Going to commit offset %s' % str(offset))
                        try:
                            consumer.commit(offset)
                        except Exception as e:
                            logger.error(u'Error trying to commit offset "%s": %s' % (offset, str(e)))

                if consumer_records_skipped or consumer_records_validation_failed:
                    logger.debug(
                            u'Skipped %d consumer records due to non-matching keys and %d consumer records due to '
                            u'validation failures for topic partition: %s' % (
                                consumer_records_skipped, consumer_records_validation_failed, topic_partition))

    def stop_consumer(self):
        """
        Stops the consumer gracefully
        """
        self._asked_to_stop = True
        if self._consumer:
            self._consumer.unsubscribe()
            self._consumer.close()


class PanoptesResourcesConsumer(PanoptesConsumer):
    def __init__(self, panoptes_context, client_id, group, keys, poll_timeout, callback,
                 validate=False, session_timeout=30, max_poll_records=500, max_partition_fetch_bytes=1048576):
        """
        This class implements a helper/wrapper for writing Panoptes Consumers

        The consumer object produced by this class joins the group provided in the arguments, subscribes to the
        relevant
        topics (which are combination of the site name and consumer type), filters records by the provided keys,
        checks
        validity of the records by converting them to JSON and validating against consumer type specific schemas and
        then calls the callback function provided for each record.

        The consumer object also takes care of advancing the committed marker for each topic and partition - if the
        callback fails or returns false, the marker is not advanced.

        Args:
            panoptes_context (PanoptesContext): The PanoptesContext to be used by the consumer
            client_id (str): A non-empty string that uniquely identifies the consumer instance - this is used for \
            logging by and group administration by Kafka
            group (str): The Kafka group this consumer should be a part of
            keys (list, None): A comma separated list of keys the consumer should filter against
            poll_timeout (int): A non-negative integer which is the interval (in seconds) the consumer should
            sleep if \
            no records are available on the Kafka bus
            callback (callable): A callable to which each processed, validated consumer records (deserialized JSON \
            object) should be passed. The callable should return false if it cannot process the record due to
            temporary \
            issues - it would be redelivered to the callback in that case
            validate (bool): Whether each incoming record should be validated. Defaults to False
            session_timeout (int): A non-negative integer which is the interval (in seconds) after which the Kafka \
            Group Management system should consider the client disconnected
            max_poll_records (int): The maximum number of records to fetch in one poll cycle

        Returns:
            None
        """
        topics = make_topic_names_for_all_sites(panoptes_context,
                                                CONSUMER_TYPE_NAMES[PanoptesConsumerTypes.RESOURCES])

        super(PanoptesResourcesConsumer, self).__init__(panoptes_context=panoptes_context,
                                                        consumer_type=PanoptesConsumerTypes.RESOURCES,
                                                        topics=topics,
                                                        client_id=client_id,
                                                        group=group,
                                                        keys=keys,
                                                        poll_timeout=poll_timeout,
                                                        callback=callback,
                                                        validate=validate,
                                                        session_timeout=session_timeout,
                                                        max_poll_records=max_poll_records,
                                                        max_partition_fetch_bytes=max_partition_fetch_bytes)
