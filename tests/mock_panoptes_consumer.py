"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""

import json
import os
import time

from yahoo_panoptes.framework.utilities.consumer import PanoptesConsumerTypes, make_topic_names_for_all_sites, \
    CONSUMER_TYPE_NAMES


# Mocking this since the underlying functions used in the original are only available on Linux systems
def mock_get_client_id(prefix):
    return '_'.join([prefix, 'localhost', '1234'])


class MockPanoptesConsumer(object):
    """
    A mock consumer for testing purposes
    """
    files = []

    def __init__(self, panoptes_context, consumer_type, topics, client_id, group, keys, poll_timeout, callback,
                 validate=False, session_timeout=10, max_poll_records=500, max_partition_fetch_bytes=1048576):
        self._panoptes_context = panoptes_context
        self._topics = topics
        self._client_id = client_id
        self._keys = keys
        self._group = group
        self._poll_timeout = poll_timeout * 1000
        self._callback = callback
        self._validate = validate

    def start_consumer(self):
        """
        Mock start consumer method uses static json object

        Note:
            Calls back based on _callback attribute passing args 'key:foo:foo' and the unpacked resources.json
        """
        for file_name in self.files:
            full_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), file_name)
            with open(full_path) as f:
                consumer_record_object = json.load(f)

                # If the timestamp is negative, then convert it to a relative timestamp by adding the current time
                # Skip for resource objects

                try:
                    if consumer_record_object['metrics_group_creation_timestamp'] < 0:
                        consumer_record_object['metrics_group_creation_timestamp'] += time.time()
                except:
                    pass

                self._callback('key:foo:foo', consumer_record_object)


class MockPanoptesResourcesConsumer(MockPanoptesConsumer):
    def __init__(self, panoptes_context, client_id, group, keys, poll_timeout, callback,
                 validate=False, session_timeout=10, max_poll_records=500, max_partition_fetch_bytes=1048576):

        topics = make_topic_names_for_all_sites(panoptes_context,
                                                CONSUMER_TYPE_NAMES[PanoptesConsumerTypes.RESOURCES])

        super(MockPanoptesResourcesConsumer, self).__init__(panoptes_context=panoptes_context,
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
