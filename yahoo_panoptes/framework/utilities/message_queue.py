"""
This module implements an abstract Message Producer based on Kafka Queues
"""
from kafka import KeyedProducer
from kafka.partitioner import Murmur2Partitioner

from ..validators import PanoptesValidators


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
        self._kafka_producer = KeyedProducer(self._kafka_client, async=async, partitioner=Murmur2Partitioner)

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
        assert PanoptesValidators.valid_nonempty_string(topic), 'topic must be a non-empty string'
        assert PanoptesValidators.valid_nonempty_string(key), 'key must be a non-empty string'
        assert PanoptesValidators.valid_nonempty_string(messages), 'messages must be a non-empty string'

        self._kafka_client.ensure_topic_exists(topic)

        if partitioning_key:
            # We do this hack so that the partitioning key can be different from the message key
            partition = self._kafka_producer._next_partition(topic, partitioning_key)
            self._kafka_producer._send_messages(topic, partition, messages, key=key)
        else:
            # In this case, the message key is used as the partitioning key
            self._kafka_producer.send_messages(topic, key, messages)

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
