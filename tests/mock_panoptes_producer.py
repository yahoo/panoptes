from yahoo_panoptes.framework.validators import PanoptesValidators

class MockPanoptesMessageProducer(object):

    def __init__(self):
        self._kafka_client = dict()
        self._kafka_client['stopped'] = False
        self._kafka_producer = []

    def __del__(self):
        pass

    def send_messages(self, topic, key, messages, partitioning_key=None):

        assert PanoptesValidators.valid_nonempty_string(topic), 'topic must be a non-empty string'
        assert PanoptesValidators.valid_nonempty_string(key), 'key must be a non-empty string'
        assert PanoptesValidators.valid_nonempty_string(messages), 'messages must be a non-empty string'

        self._kafka_producer.append({
            'topic': topic,
            'key': key,
            'message': messages
        })

    def stop(self):

        if not self._kafka_client['stopped']:
            self._kafka_client['stopped'] = True
            