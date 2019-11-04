"""
Copyright 2019, Verizon Media
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
from builtins import object
import unittest

from yahoo_panoptes.framework.enrichment import PanoptesEnrichmentSet

from yahoo_panoptes.enrichment.schema.heartbeat import PanoptesHeartbeatEnrichmentGroup
from yahoo_panoptes.enrichment.schema.topology import PanoptesTopologyEnrichmentGroup
from yahoo_panoptes.enrichment.schema.operational import PanoptesOperationalEnrichmentGroup
from yahoo_panoptes.enrichment.schema.neighbor import PanoptesNeighborEnrichmentGroup
from yahoo_panoptes.enrichment.schema.neighbor_lookup import PanoptesBridgeLookupEnrichmentGroup, \
    PanoptesInverseInterfaceLookupEnrichmentGroup, PanoptesL3InterfaceLookupEnrichmentGroup, \
    PanoptesInterfaceLookupEnrichmentGroup, PanoptesDeviceLookupEnrichmentGroup


class Types(object):
    @staticmethod
    def integer_type(key=None):
        return 1

    @staticmethod
    def string_type(key):
        return u"test_{}".format(key)

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
    u'string': Types.string_type,
    u'integer': Types.integer_type,
    u'dict': Types.dictionary_type,
    u'list': Types.list_type,
    u'float': Types.float_type
}

bad_types = {
    u'string': Types.integer_type,
    u'integer': Types.string_type,
    u'dict': Types.integer_type,
    u'list': Types.dictionary_type,
    u'float': Types.string_type
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

        if enrichment_group.enrichment_schema[u'enrichment_label'][u'type'] != u'dict':
            return None

        enrichment = PanoptesEnrichmentSet(key=u'enrichment_label')

        for key, value in list(enrichment_group.enrichment_schema[u'enrichment_label'][u'schema'].items()):
            validator_type = value[u'type']

            if type(validator_type) is list:
                enrichment.add(key, types[value[u'type'][0]](key))

            elif type(validator_type) is str:
                enrichment.add(key, types[value[u'type']](key))

        return enrichment

    def test_schemas(self):

        for enrichment_group in self._schemas:

            enrichment_entry = enrichment_group(60, 60)

            enrichment = self._construct_flat_schema_entry(enrichment_entry)

            self.assertTrue(enrichment_entry.validator.validate(enrichment))

            for override_key, value in list(enrichment_entry.enrichment_schema[u'enrichment_label'][u'schema'].items()):

                validator_type = value[u'type'][0] if type(value[u'type']) is list else value[u'type']
                stored_value = enrichment._raw_data[u'enrichment_label']

                enrichment.add(override_key, bad_types[validator_type](u'__TEST__'))

                self.assertFalse(enrichment_entry.validator.validate(enrichment))

                enrichment.add(override_key, stored_value)
