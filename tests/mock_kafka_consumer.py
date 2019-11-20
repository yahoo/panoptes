"""
Copyright 2019, Verizon Media
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
from builtins import object
import json
from kafka.consumer.fetcher import ConsumerRecord


class Subscription(object):

    def all_consumed_offsets(self):
        return u'Subscription.all_consumed_offsets'


class MockKafkaConsumer(object):

    def __init__(self,
                 bootstrap_servers=None,
                 client_id=None,
                 group_id=None,
                 enable_auto_commit=None,
                 session_timeout_ms=None,
                 request_timeout_ms=None,
                 heartbeat_interval_ms=None,
                 max_poll_records=None,
                 max_partition_fetch_bytes=None):

        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id
        self._group_id = group_id
        self._enable_auto_commit = enable_auto_commit
        self._session_timeout_ms = session_timeout_ms
        self._request_timeout_ms = request_timeout_ms
        self._heartbeat_interval_ms = heartbeat_interval_ms
        self._max_poll_records = max_poll_records
        self._max_partition_fetch_bytes = max_partition_fetch_bytes

        self._topics_subscribed_to = []

    @property
    def _subscription(self):

        sub = Subscription()
        return sub

    def subscribe(self, topics):
        self._topics_subscribed_to = list(set(
            self._topics_subscribed_to + topics
        ))

    def commit(self, offset):
        pass

    def unsubscribe(self):
        pass

    def close(self):
        pass

    def stop(self):
        pass

    def position(self, topic_partition):
        return 1

    def subscription(self):
        return ', '.join(self._topics_subscribed_to)

    def committed(self, topic_partition):
        return topic_partition

    def poll(self, timeout_ms):

        value = {
            u"metrics_group_interval": 60,
            u"resource": {
                u"resource_site": u"test_site",
                u"resource_id": u"test_id",
                u"resource_class": u"network",
                u"resource_plugin": u"test_plugin",
                u"resource_creation_timestamp": 1567823517.46,
                u"resource_subclass": u"test_subclass",
                u"resource_endpoint": u"test_endpoint",
                u"resource_metadata": {
                    u"test_metadata_key": u"test_metadata_value",
                    u"_resource_ttl": u"604800"
                },
                u"resource_type": u"test_type"
            },
            u"dimensions": [
                {
                    u"dimension_name": u"cpu_name",
                    u"dimension_value": u"test_cpu_name_value"
                },
                {
                    u"dimension_name": u"cpu_no",
                    u"dimension_value": u"test_cpu_no_value"
                },
                {
                    u"dimension_name": u"cpu_type",
                    u"dimension_value": u"test_cpu_type_value"
                }
            ],
            u"metrics_group_type": u"cpu",
            u"metrics": [
                {
                    u"metric_creation_timestamp": 1567823946.72,
                    u"metric_type": u"gauge",
                    u"metric_name": u"cpu_utilization",
                    u"metric_value": 0
                }
            ],
            u"metrics_group_creation_timestamp": 1567823946.72,
            u"metrics_group_schema_version": u"0.2"
        }

        return {
            u'400000005d73185508707bfc': [ConsumerRecord(
                topic=u'panoptes-metrics', partition=49, offset=704152, timestamp=-1, timestamp_type=0,
                key=b'class:subclass:type', value=json.dumps(value), checksum=-1526904207, serialized_key_size=19,
                serialized_value_size=1140)],
            u'400000005d731855164bb9bc': [ConsumerRecord(
                topic=u'panoptes-metrics', partition=49, offset=704152, timestamp=-1, timestamp_type=0,
                key=b'class:subclass:type::', value=json.dumps(value), checksum=-1526904207, serialized_key_size=19,
                serialized_value_size=1140)]
        }
