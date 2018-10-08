"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""

import unittest
from mock import patch
import logging

from tests.mock_panoptes_consumer import MockPanoptesResourcesConsumer, mock_get_client_id
from tests.test_framework import PanoptesMockRedis
from yahoo_panoptes.framework.context import PanoptesContext
from yahoo_panoptes.framework.resources import PanoptesResourcesKeyValueStore

import yahoo_panoptes.resources.manager


class MockPanoptesContext(PanoptesContext):
    @patch('redis.StrictRedis', PanoptesMockRedis)
    def __init__(self, config_file='tests/config_files/test_panoptes_config.ini'):
        super(MockPanoptesContext, self).__init__(
                key_value_store_class_list=[PanoptesResourcesKeyValueStore],
                create_zookeeper_client=False,
                config_file=config_file
        )

    @property
    def logger(self):
        return logging.getLogger()


class TestResourcesManager(unittest.TestCase):
    @patch('yahoo_panoptes.resources.manager.PanoptesResourceManagerContext', MockPanoptesContext)
    @patch('yahoo_panoptes.resources.manager.PanoptesResourcesConsumer', MockPanoptesResourcesConsumer)
    @patch('yahoo_panoptes.resources.manager.get_client_id', mock_get_client_id)
    def _set_and_get_resources(self, input_files):
        MockPanoptesResourcesConsumer.files = input_files
        yahoo_panoptes.resources.manager.start()
        resource_store = yahoo_panoptes.resources.manager.resource_store
        return resource_store.get_resources()

    def test_resource_manager_initial_addition(self):
        """Tests adding initial resource"""
        resources = self._set_and_get_resources(['test_resources/input/resource_one.json'])
        self.assertEqual(len(resources), 1)

    def test_resource_manager_updation(self):
        """Tests that a resource is updated if the incoming resource has a newer timestamp"""
        resources = self._set_and_get_resources(['test_resources/input/resource_one.json',
                                                 'test_resources/input/resource_one_updated.json'])
        self.assertEqual(len(resources), 1)
        self.assertEqual(resources.resources.pop().resource_creation_timestamp, 1526331464.49)

    def test_resource_manager_stale(self):
        """Tests that a resource is NOT updated if it has an older timestamp"""
        resources = self._set_and_get_resources(['test_resources/input/resource_one_updated.json',
                                                 'test_resources/input/resource_one.json'])

        rl = list(resources)
        print(rl[0])
        print(rl[0].resource_metadata)
        self.assertEqual(len(resources), 1)
        self.assertEqual(resources.resources.pop().resource_creation_timestamp, 1526331464.49)

    def test_resource_manager_deletion(self):
        """Tests that a resource is deleted if it incoming new set does not contain it"""
        resources = self._set_and_get_resources(['test_resources/input/resource_one.json',
                                                 'test_resources/input/resource_two.json'])
        self.assertEqual(len(resources), 1)
        self.assertEqual(resources.resources.pop().resource_id, 'test_id_2')

