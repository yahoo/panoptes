"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements Topology EnrichmentGroup defined with schema validator
"""
from yahoo_panoptes.framework.enrichment import \
    PanoptesEnrichmentSchemaValidator, PanoptesEnrichmentGroup

SCHEMA_NAMESPACE = u'topology'


class PanoptesTopologyEnrichmentSchemaValidator(PanoptesEnrichmentSchemaValidator):
    schema = {
        'enrichment_label': {
            'type': 'dict',
            'schema': {
                'interface': {'type': 'string', 'required': True},
                'neighbor': {'type': 'dict', 'required': True},
                'map_type': {'type': 'string', 'required': True}
            }
        }
    }


class PanoptesTopologyEnrichmentGroup(PanoptesEnrichmentGroup):
    def __init__(self, enrichment_ttl, execute_frequency):
        super(PanoptesTopologyEnrichmentGroup, self).__init__(
            namespace=SCHEMA_NAMESPACE,
            schema_validator=PanoptesTopologyEnrichmentSchemaValidator(),
            enrichment_ttl=enrichment_ttl,
            execute_frequency=execute_frequency)
