"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements Topology EnrichmentGroup defined with schema validator
"""

from ...framework.enrichment import PanoptesEnrichmentSchemaValidator, PanoptesEnrichmentGroup

SCHEMA_NAMESPACE = 'topology'


class PanoptesTopologyEnrichmentSchemaValidator(PanoptesEnrichmentSchemaValidator):
    schema = {
        'enrichment_label': {
            'type': 'dict',
            'schema': {
                'local_interface': {'type': 'string', 'required': True},
                'local_interface_layer': {'type': 'integer', 'required': True},
                'local_lag_members': {'type': 'list', 'required': True},
                'local_interface_stp_mode': {'type': 'string', 'required': True},
                'local_interface_ip_version': {'type': 'dict', 'required': True},
                'remote_device': {'type': ['string', 'dict'], 'required': True},
                'remote_interface': {'type': 'string', 'required': True},
                'remote_interface_index': {'type': 'string', 'required': True},
                'remote_interface_layer': {'type': 'integer', 'required': True},
                'remote_interface_stp_mode': {'type': 'string', 'required': True},
                'remote_lag_members': {'type': 'list', 'required': True},
                'remote_interface_ip_version': {'type': 'dict', 'required': True},
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
