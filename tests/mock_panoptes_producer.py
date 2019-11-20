"""
Copyright 2019, Verizon Media Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
from builtins import object


class MockPanoptesMessageProducer(object):

    def __init__(self, **args):
        self._stopped = False
        self._kafka_producer = []

    def __del__(self):
        pass

    @property
    def messages(self):
        return self._kafka_producer[:]

    def close(self, **args):
        pass

    def bootstrap_connected(self):
        return True

    def send_messages(self, topic, key, messages, partitioning_key=None):

        self._kafka_producer.append({
            u'topic': topic,
            u'key': key,
            u'message': messages
        })

    def stop(self):

        if not self._stopped:
            self._stopped = True


class MockPanoptesMessageProducerNoConnection(MockPanoptesMessageProducer):

    def bootstrap_connected(self):
        return False
