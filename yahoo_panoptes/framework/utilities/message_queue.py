"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements an abstract Message Producer based on Kafka Queues
"""
from builtins import object
import kafka
from kafka.partitioner import Murmur2Partitioner

from yahoo_panoptes.framework.validators import PanoptesValidators


class PanoptesMessageQueueProducer(object):
    """
    Creates a message producer client instance

    Args:
        panoptes_context (PanoptesContext): The PanoptesContext to use. The Kafka client associated with the context \
        would be used to create the message producer
        async(bool): Whether the created message producer should be asynchronous. Defaults to False. If you create an \
        asynchronous message producer, a background thread would be created in the process to handle the message sending
    """

    def __init__(self, panoptes_context, async=False):
        self._kafka_client = panoptes_context.kafka_client
        self._kafka_producer = kafka.KeyedProducer(self._kafka_client, async=async, partitioner=Murmur2Partitioner)

    def __del__(self):
        self.stop()

    def send_messages(self, topic, key, messages, partitioning_key=None):
        """
        Send messages to the specified topic

        This method tries to ensures that the topic exists before trying to send a message to it. If auto-creation of
        topics is enabled, then this should always succeed (barring Kafka/Zookeeper failures)

        Args:
            topic (str): The topic to which the message should be sent
            key (str): The key for the message
            messages (str): The message to send
            partitioning_key (str): If provided, then it would be used to select the partition for the message instead \
            of the message key

        Returns:
            None: Nothing. Passes through exceptions in case of failure

        """
        assert PanoptesValidators.valid_nonempty_string(topic), u'topic must be a non-empty string'
        assert PanoptesValidators.valid_nonempty_string(key), u'key must be a non-empty string'
        assert PanoptesValidators.valid_nonempty_string(messages), u'messages must be a non-empty string'

        self._kafka_client.ensure_topic_exists(topic)

        if partitioning_key:
            # We do this hack so that the partitioning key can be different from the message key
            partition = self._kafka_producer._next_partition(topic, partitioning_key.encode('utf-8'))

            self._kafka_producer._send_messages(
                topic,
                partition,
                messages.encode('utf-8'),
                key=key.encode('utf-8')
            )

        else:
            # In this case, the message key is used as the partitioning key
            self._kafka_producer.send_messages(
                topic,
                key.encode('utf-8'),
                messages.encode('utf-8')
            )

    def stop(self):
        """
        Stop the message producer

        This method should always be called as part of cleanups so that any pending messages (for async message \
        producers) can be flushed and the Kafka connection closed cleanly

        Returns:
            None: Nothing. Passes through exceptions in case of failure

        """
        if not self._kafka_producer.stopped:
            self._kafka_producer.stop()
