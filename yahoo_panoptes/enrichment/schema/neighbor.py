"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements Neighbor EnrichmentGroup defined with schema validator
"""
from yahoo_panoptes.framework.enrichment import \
    PanoptesEnrichmentSchemaValidator, PanoptesEnrichmentGroup

NEIGHBOR_SCHEMA_NAMESPACE = 'neighbor'


class PanoptesNeighborEnrichmentSchemaValidator(PanoptesEnrichmentSchemaValidator):
    schema = {
        'enrichment_label': {
            'type': 'dict',
            'schema': {
                'description': {'type': 'string', 'required': True},
                'type': {'type': 'string', 'required': True},
                'layer': {'type': 'integer', 'required': True},
                'member_of_lag': {'type': 'dict', 'required': True},
                'member_of_svi': {'type': 'dict', 'required': True},
                'lag_members': {'type': 'dict', 'required': True},
                'ipv6_neighbor': {'type': 'dict', 'required': True},
                'ipv4_neighbor': {'type': 'dict', 'required': True},
                'l2_neighbor': {'type': 'dict', 'required': True},
                'ipv6_address': {'type': 'list', 'required': True},
                'ipv4_address': {'type': 'list', 'required': True},
                'vlans': {'type': 'dict', 'required': True},
                'spanning_tree': {'type': 'dict', 'required': True},
                'mac_address': {'type': 'string', 'required': True},
                'svi_physical_members': {'type': 'dict', 'required': True},
                'sub_interfaces': {'type': 'dict', 'required': True},
                'primary_interface': {'type': 'dict', 'required': True},
                'link_state': {'type': 'string', 'required': True},
                'admin_state': {'type': 'string', 'required': True},
                'category': {'type': 'string', 'required': True},
                'name': {'type': 'string', 'required': True}
            }
        }
    }


class PanoptesNeighborEnrichmentGroup(PanoptesEnrichmentGroup):
    def __init__(self, enrichment_ttl, execute_frequency):
        super(PanoptesNeighborEnrichmentGroup, self).__init__(
            namespace=NEIGHBOR_SCHEMA_NAMESPACE,
            schema_validator=PanoptesNeighborEnrichmentSchemaValidator(),
            enrichment_ttl=enrichment_ttl,
            execute_frequency=execute_frequency)
