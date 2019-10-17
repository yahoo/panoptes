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
        u'enrichment_label': {
            u'type': u'dict',
            u'schema': {
                u'interface': {u'type': u'string', u'required': True},
                u'neighbor': {u'type': u'dict', u'required': True},
                u'map_type': {u'type': u'string', u'required': True}
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
