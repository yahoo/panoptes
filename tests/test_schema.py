"""
Copyright 2019, Verizon Media
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
import unittest

from yahoo_panoptes.framework.enrichment import PanoptesEnrichmentSet

from yahoo_panoptes.enrichment.schema.heartbeat import PanoptesHeartbeatEnrichmentGroup
from yahoo_panoptes.enrichment.schema.topology import PanoptesTopologyEnrichmentGroup
from yahoo_panoptes.enrichment.schema.operational import PanoptesOperationalEnrichmentGroup
from yahoo_panoptes.enrichment.schema.neighbor import PanoptesNeighborEnrichmentGroup
from yahoo_panoptes.enrichment.schema.neighbor_lookup import PanoptesBridgeLookupEnrichmentGroup, \
    PanoptesInverseInterfaceLookupEnrichmentGroup, PanoptesL3InterfaceLookupEnrichmentGroup, \
    PanoptesInterfaceLookupEnrichmentGroup, PanoptesDeviceLookupEnrichmentGroup


class Types:
    @staticmethod
    def integer_type(key=None):
        return 1

    @staticmethod
    def string_type(key):
        return "test_{}".format(key)

    @staticmethod
    def dictionary_type(key):
        return {}

    @staticmethod
    def list_type(key):
        return []

    @staticmethod
    def float_type(key):
        return 1.1


types = {
    'string': Types.string_type,
    'integer': Types.integer_type,
    'dict': Types.dictionary_type,
    'list': Types.list_type,
    'float': Types.float_type
}

bad_types = {
    'string': Types.integer_type,
    'integer': Types.string_type,
    'dict': Types.integer_type,
    'list': Types.dictionary_type,
    'float': Types.string_type
}


class TestFlatValidators(unittest.TestCase):

    def setUp(self):

        self._schemas = [
            PanoptesHeartbeatEnrichmentGroup,
            PanoptesTopologyEnrichmentGroup,
            PanoptesOperationalEnrichmentGroup,
            PanoptesNeighborEnrichmentGroup,
            PanoptesBridgeLookupEnrichmentGroup,
            PanoptesInverseInterfaceLookupEnrichmentGroup,
            PanoptesL3InterfaceLookupEnrichmentGroup,
            PanoptesInterfaceLookupEnrichmentGroup,
            PanoptesDeviceLookupEnrichmentGroup
        ]

    def _construct_flat_schema_entry(self, enrichment_group):

        if enrichment_group.enrichment_schema['enrichment_label']['type'] != 'dict':
            return None

        enrichment = PanoptesEnrichmentSet(key='enrichment_label')

        for key, value in enrichment_group.enrichment_schema['enrichment_label']['schema'].items():

            validator_type = value['type']

            if type(validator_type) is list:
                enrichment.add(key, types[value['type'][0]](key))

            elif type(validator_type) is str:
                enrichment.add(key, types[value['type']](key))

        return enrichment

    def test_schemas(self):

        for enrichment_group in self._schemas:

            enrichment_entry = enrichment_group(60, 60)

            enrichment = self._construct_flat_schema_entry(enrichment_entry)

            self.assertTrue(enrichment_entry.validator.validate(enrichment))

            for override_key, value in enrichment_entry.enrichment_schema['enrichment_label']['schema'].items():

                validator_type = value['type'][0] if type(value['type']) is list else value['type']
                stored_value = enrichment._raw_data['enrichment_label']

                enrichment.add(override_key, bad_types[validator_type]('__TEST__'))

                self.assertFalse(enrichment_entry.validator.validate(enrichment))

                enrichment.add(override_key, stored_value)
