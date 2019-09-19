"""
Copyright 2019, Verizon Media
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
import unittest

from yahoo_panoptes.framework.enrichment import PanoptesEnrichmentSet

from yahoo_panoptes.enrichment.schema.heartbeat import PanoptesHeartbeatEnrichmentSchemaValidator
from yahoo_panoptes.enrichment.schema.topology import PanoptesTopologyEnrichmentSchemaValidator
from yahoo_panoptes.enrichment.schema.operational import PanoptesOperationalEnrichmentSchemaValidator
from yahoo_panoptes.enrichment.schema.neighbor import PanoptesNeighborEnrichmentSchemaValidator
from yahoo_panoptes.enrichment.schema.neighbor_lookup import PanoptesInterfaceLookupEnrichmentSchemaValidator, \
    PanoptesDeviceLookupEnrichmentSchemaValidator, PanoptesBridgeLookupEnrichmentSchemaValidator, \
    PanoptesInverseInterfaceLookupEnrichmentSchemaValidator, PanoptesL3InterfaceLookupEnrichmentSchemaValidator


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
            PanoptesTopologyEnrichmentSchemaValidator,
            PanoptesOperationalEnrichmentSchemaValidator,
            PanoptesNeighborEnrichmentSchemaValidator,
            PanoptesInterfaceLookupEnrichmentSchemaValidator,
            PanoptesDeviceLookupEnrichmentSchemaValidator,
            PanoptesHeartbeatEnrichmentSchemaValidator,
            PanoptesBridgeLookupEnrichmentSchemaValidator,
            PanoptesInverseInterfaceLookupEnrichmentSchemaValidator,
            PanoptesL3InterfaceLookupEnrichmentSchemaValidator
        ]

    def _construct_flat_schema_entry(self, schema):

        if schema['enrichment_label']['type'] != 'dict':
            return None

        enrichment = PanoptesEnrichmentSet(key='enrichment_label')

        for key, value in schema['enrichment_label']['schema'].items():

            validator_type = value['type']

            if type(validator_type) is list:
                enrichment.add(key, types[value['type'][0]](key))

            elif type(validator_type) is str:
                enrichment.add(key, types[value['type']](key))

        return enrichment

    def test_schemas(self):

        for schema_validator in self._schemas:
            enrichment = self._construct_flat_schema_entry(schema_validator.schema)

            validator = schema_validator()

            self.assertTrue(validator.validate(enrichment))

            for override_key, value in schema_validator.schema['enrichment_label']['schema'].items():

                validator_type = value['type'][0] if type(value['type']) is list else value['type']
                stored_value = enrichment._raw_data['enrichment_label']

                enrichment.add(override_key, bad_types[validator_type]('__TEST__'))

                self.assertFalse(validator.validate(enrichment))

                enrichment.add(override_key, stored_value)
