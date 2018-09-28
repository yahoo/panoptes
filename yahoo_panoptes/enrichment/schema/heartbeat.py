"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements Heartbeat EnrichmentGroup defined with schema validator
"""

from ...framework.enrichment import PanoptesEnrichmentSchemaValidator, PanoptesEnrichmentGroup


HEARTBEAT_SCHEMA_NAMESPACE = 'heartbeat_ns'


class PanoptesHeartbeatEnrichmentSchemaValidator(PanoptesEnrichmentSchemaValidator):
    schema = {
        'enrichment_label': {
            'type': 'dict',
            'schema': {
                'timestamp': {'type': 'float'},
            }
        }
    }


class PanoptesHeartbeatEnrichmentGroup(PanoptesEnrichmentGroup):
    def __init__(self, enrichment_ttl, execute_frequency):
        super(PanoptesHeartbeatEnrichmentGroup, self).__init__(
            namespace=HEARTBEAT_SCHEMA_NAMESPACE,
            schema_validator=PanoptesHeartbeatEnrichmentSchemaValidator(),
            enrichment_ttl=enrichment_ttl,
            execute_frequency=execute_frequency)
