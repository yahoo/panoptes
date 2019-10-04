"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""

import unittest
from mock import patch, Mock

from tests.mock_panoptes_consumer import MockPanoptesResourcesConsumer, mock_get_client_id
from tests.test_framework import PanoptesMockRedis
from yahoo_panoptes.framework.context import PanoptesContext
from yahoo_panoptes.framework.resources import PanoptesResourceStore, PanoptesResourceError,\
    PanoptesResourcesKeyValueStore

import yahoo_panoptes.resources.manager


class MockPanoptesContext(PanoptesContext):
    @patch('redis.StrictRedis', PanoptesMockRedis)
    def __init__(self, config_file='tests/config_files/test_panoptes_config.ini'):
        super(MockPanoptesContext, self).__init__(
                key_value_store_class_list=[PanoptesResourcesKeyValueStore],
                create_zookeeper_client=False,
                config_file=config_file
        )


class MockPanoptesResourceStoreFailSecondTime(PanoptesResourceStore):
    def __init__(self, panoptes_context):
        self._call_count = 0
        super(MockPanoptesResourceStoreFailSecondTime, self).__init__(panoptes_context)

    def add_resource(self, plugin_signature, resource):
        self._call_count += 1

        if self._call_count > 1:
            raise PanoptesResourceError()
        else:
            super(MockPanoptesResourceStoreFailSecondTime, self).add_resource(plugin_signature, resource)


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

        self.assertEqual(len(resources), 1)
        self.assertEqual(resources.resources.pop().resource_creation_timestamp, 1526331464.49)

    def test_resource_manager_deletion(self):
        """Tests that a resource is deleted if it incoming new set does not contain it"""
        resources = self._set_and_get_resources(['test_resources/input/resource_one.json',
                                                 'test_resources/input/resource_two.json'])
        self.assertEqual(len(resources), 1)
        self.assertEqual(resources.resources.pop().resource_id, 'test_id_2')

    @patch('yahoo_panoptes.resources.manager.PanoptesResourceManagerContext', MockPanoptesContext)
    @patch('yahoo_panoptes.resources.manager.PanoptesResourcesConsumer', MockPanoptesResourcesConsumer)
    @patch('yahoo_panoptes.resources.manager.get_client_id', mock_get_client_id)
    def test_resource_manager_get_resources_exception(self):
        """Tests that if the get_resources method fails then it handled gracefully"""
        mock_get_resources = Mock(side_effect=Exception)
        with patch('yahoo_panoptes.resources.manager.PanoptesResourceStore.get_resources', mock_get_resources):
            MockPanoptesResourcesConsumer.files = ['test_resources/input/resource_one.json']
            yahoo_panoptes.resources.manager.start()

        # This has to fetched/tested outside of the patching above
        resource_store = yahoo_panoptes.resources.manager.resource_store
        self.assertEqual(len(resource_store.get_resources()), 0)

    def test_resource_manager_add_resource_exception(self):
        """Tests that if the add_resources method fails then it handled gracefully"""
        mock_add_resource = Mock(side_effect=Exception)
        with patch('yahoo_panoptes.resources.manager.PanoptesResourceStore.add_resource', mock_add_resource):
            resources = self._set_and_get_resources(['test_resources/input/resource_one.json'])
            self.assertEqual(len(resources), 0)

    def test_resource_manager_delete_resource_exception(self):
        """Tests that if the delete_resource method fails then it handled gracefully"""
        mock_delete_resource = Mock(side_effect=Exception)
        with patch('yahoo_panoptes.resources.manager.PanoptesResourceStore.delete_resource', mock_delete_resource):
            resources = self._set_and_get_resources(['test_resources/input/resource_one.json',
                                                     'test_resources/input/resource_two.json'])
            self.assertEqual(len(resources), 1)

    def test_resource_manager_update_resource_exception(self):
        """Tests that if resource updation fails then it handled gracefully"""
        with patch('yahoo_panoptes.resources.manager.PanoptesResourceStore', MockPanoptesResourceStoreFailSecondTime):
            resources = self._set_and_get_resources(['test_resources/input/resource_one.json',
                                                     'test_resources/input/resource_one_updated.json'])

            self.assertEqual(len(resources), 1)
            resource = resources.resources.pop()
            self.assertEqual(resource.resource_creation_timestamp, 1526331404.49)
