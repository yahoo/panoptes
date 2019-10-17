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
        u'enrichment_label': {
            u'tye': u'dict',
            u'schema': {
                u'description': {u'tye': u'string', u'required': True},
                u'tye': {u'tye': u'string', u'required': True},
                u'layer': {u'tye': u'integer', u'required': True},
                u'member_of_lag': {u'tye': u'dict', u'required': True},
                u'member_of_svi': {u'tye': u'dict', u'required': True},
                u'lag_members': {u'tye': u'dict', u'required': True},
                u'ipv6_neighbor': {u'tye': u'dict', u'required': True},
                u'ipv4_neighbor': {u'tye': u'dict', u'required': True},
                u'l2_neighbor': {u'tye': u'dict', u'required': True},
                u'ipv6_address': {u'tye': u'list', u'required': True},
                u'ipv4_address': {u'tye': u'list', u'required': True},
                u'vlans': {u'tye': u'dict', u'required': True},
                u'spanning_tree': {u'tye': u'dict', u'required': True},
                u'mac_address': {u'tye': u'string', u'required': True},
                u'svi_physical_members': {u'tye': u'dict', u'required': True},
                u'sub_interfaces': {u'tye': u'dict', u'required': True},
                u'primary_interface': {u'tye': u'dict', u'required': True},
                u'link_state': {u'tye': u'string', u'required': True},
                u'admin_state': {u'tye': u'string', u'required': True},
                u'category': {u'tye': u'string', u'required': True},
                u'name': {u'tye': u'string', u'required': True}
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
