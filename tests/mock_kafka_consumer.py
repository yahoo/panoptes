"""
Copyright 2019, Verizon Media
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
import json
from kafka.consumer.fetcher import ConsumerRecord


class Subscription(object):

    def all_consumed_offsets(self):
        return 'Subscription.all_consumed_offsets'


class MockKafkaConsumer(object):

    def __init__(self, bootstrap_servers, client_id, group_id, enable_auto_commit,
                 session_timeout_ms, request_timeout_ms, heartbeat_interval_ms,
                 max_poll_records, max_partition_fetch_bytes):

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

    def position(self, topic_partition):
        return 1

    def subscription(self):
        return ', '.join(self._topics_subscribed_to)

    def committed(self, topic_partition):
        return topic_partition

    def poll(self, timeout_ms):

        value = {
            "metrics_group_interval": 60,
            "resource": {
                "resource_site": "test_site",
                "resource_id": "test_id",
                "resource_class": "network",
                "resource_plugin": "test_plugin",
                "resource_creation_timestamp": 1567823517.46,
                "resource_subclass": "test_subclass",
                "resource_endpoint": "test_endpoint",
                "resource_metadata": {
                    "test_metadata_key": "test_metadata_value",
                    "_resource_ttl": "604800"
                },
                "resource_type": "test_type"
            },
            "dimensions": [
                {
                    "dimension_name": "cpu_name",
                    "dimension_value": "test_cpu_name_value"
                },
                {
                    "dimension_name": "cpu_no",
                    "dimension_value": "test_cpu_no_value"
                },
                {
                    "dimension_name": "cpu_type",
                    "dimension_value": "test_cpu_type_value"
                }
            ],
            "metrics_group_type": "cpu",
            "metrics": [
                {
                    "metric_creation_timestamp": 1567823946.72,
                    "metric_type": "gauge",
                    "metric_name": "cpu_utilization",
                    "metric_value": 0
                }
            ],
            "metrics_group_creation_timestamp": 1567823946.72,
            "metrics_group_schema_version": "0.2"
        }

        return {
            '400000005d73185508707bfc': [ConsumerRecord(
                topic='panoptes-metrics', partition=49, offset=704152, timestamp=-1, timestamp_type=0,
                key='class:subclass:type', value=json.dumps(value), checksum=-1526904207, serialized_key_size=19,
                serialized_value_size=1140)],
            '400000005d731855164bb9bc': [ConsumerRecord(
                topic='panoptes-metrics', partition=49, offset=704152, timestamp=-1, timestamp_type=0,
                key='class:subclass:type::', value=json.dumps(value), checksum=-1526904207, serialized_key_size=19,
                serialized_value_size=1140)]
        }
