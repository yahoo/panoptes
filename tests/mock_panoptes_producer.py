"""
Copyright 2019, Verizon Media Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
from builtins import object


class MockPanoptesMessageProducer(object):

    def __init__(self):
        """
        Initialize kafka client.

        Args:
            self: (todo): write your description
        """
        self._kafka_client = dict()
        self._kafka_client[u'stopped'] = False
        self._kafka_producer = []

    def __del__(self):
        """
        Remove a function from self.

        Args:
            self: (todo): write your description
        """
        pass

    @property
    def messages(self):
        """
        Returns a list of all messages.

        Args:
            self: (todo): write your description
        """
        return self._kafka_producer[:]

    def _next_partition(self, topic, partitioning_key):
        """
        Return the next partition.

        Args:
            self: (todo): write your description
            topic: (todo): write your description
            partitioning_key: (str): write your description
        """
        return "{}_{}".format(topic, partitioning_key)

    def _send_messages(self, topic, partition, messages, key):
        """
        Sends a list of the specified topic.

        Args:
            self: (todo): write your description
            topic: (str): write your description
            partition: (str): write your description
            messages: (list): write your description
            key: (str): write your description
        """
        return self.send_messages(topic, key, messages, partition)

    def send_messages(self, topic, key, messages, partitioning_key=None):
        """
        Sends messages to a list of topic.

        Args:
            self: (todo): write your description
            topic: (str): write your description
            key: (str): write your description
            messages: (list): write your description
            partitioning_key: (str): write your description
        """

        self._kafka_producer.append({
            u'topic': topic,
            u'key': key,
            u'message': messages
        })

    def ensure_topic_exists(self, topic):
        """
        Check if a topic exists.

        Args:
            self: (todo): write your description
            topic: (str): write your description
        """
        return True

    def stop(self):
        """
        Stop kafka client.

        Args:
            self: (todo): write your description
        """

        if not self._kafka_client[u'stopped']:
            self._kafka_client[u'stopped'] = True


class MockPanoptesKeyedProducer(MockPanoptesMessageProducer):
    def __init__(self, client, async, partitioner):
        """
        Initialize the client.

        Args:
            self: (todo): write your description
            client: (todo): write your description
            async: (bool): write your description
            partitioner: (todo): write your description
        """
        super(MockPanoptesKeyedProducer, self).__init__()
