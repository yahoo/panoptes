"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements Neighbor Lookup EnrichmentGroup defined with schema validator
"""

from yahoo_panoptes.framework.enrichment import \
    PanoptesEnrichmentSchemaValidator, PanoptesEnrichmentGroup

BRIDGE_LOOKUP_SCHEMA_NAMESPACE = u'bridge_lookup'
INTERFACE_LOOKUP_SCHEMA_NAMESPACE = u'interface_lookup'
DEVICE_LOOKUP_SCHEMA_NAMESPACE = u'device_lookup'
L3_INTERFACE_LOOKUP_SCHEMA_NAMESPACE = u'l3_interface_lookup'
INVERSE_INTERFACE_LOOKUP_SCHEMA_NAMESPACE = u'inverse_interface_lookup'


class PanoptesBridgeLookupEnrichmentSchemaValidator(PanoptesEnrichmentSchemaValidator):
    schema = {
        u'enrichment_label': {
            u'type': u'dict',
            u'schema': {
                u'resource_id': {u'type': u'string', u'required': True},
                u'port_id_map': {u'type': u'dict', u'required': True}
            }
        }
    }


class PanoptesInverseInterfaceLookupEnrichmentSchemaValidator(PanoptesEnrichmentSchemaValidator):
    schema = {
        u'enrichment_label': {
            u'type': u'dict',
            u'schema': {
                u'resource_id': {u'type': u'string', u'required': True},
                u'interface_description': {u'type': u'string', u'required': True},
                u'interface_index': {u'type': u'string', u'required': True}
            }
        }
    }


class PanoptesL3InterfaceLookupEnrichmentSchemaValidator(PanoptesEnrichmentSchemaValidator):
    schema = {
        u'enrichment_label': {
            u'type': u'dict',
            u'schema': {
                u'resource_id': {u'type': u'string', u'required': True},
                u'interface_description': {u'type': u'string', u'required': True},
                u'interface_index': {u'type': u'string', u'required': True},
                u'ip_version': {u'type': u'integer', u'required': True}
            }
        }
    }


class PanoptesInterfaceLookupEnrichmentSchemaValidator(PanoptesEnrichmentSchemaValidator):
    schema = {
        u'enrichment_label': {
            u'type': u'dict',
            u'schema': {
                u'resource_id': {u'type': u'string', u'required': True},
                u'interface_description': {u'type': u'string', u'required': True},
                u'interface_index': {u'type': u'string', u'required': True},
                u'port_id_map': {u'type': u'dict', u'required': True}
            }
        }
    }


class PanoptesDeviceLookupEnrichmentSchemaValidator(PanoptesEnrichmentSchemaValidator):
    schema = {
        u'enrichment_label': {
            u'type': u'dict',
            u'schema': {
                u'ipv4': {u'type': u'list', u'required': True},
                u'ipv6': {u'type': u'list', u'required': True},
                u'source_resource_id': {u'type': u'string', u'required': True}
            }
        }
    }


class PanoptesBridgeLookupEnrichmentGroup(PanoptesEnrichmentGroup):
    def __init__(self, enrichment_ttl, execute_frequency):
        super(PanoptesBridgeLookupEnrichmentGroup, self).__init__(
            namespace=BRIDGE_LOOKUP_SCHEMA_NAMESPACE,
            schema_validator=PanoptesBridgeLookupEnrichmentSchemaValidator(),
            enrichment_ttl=enrichment_ttl,
            execute_frequency=execute_frequency)


class PanoptesInverseInterfaceLookupEnrichmentGroup(PanoptesEnrichmentGroup):
    def __init__(self, enrichment_ttl, execute_frequency):
        super(PanoptesInverseInterfaceLookupEnrichmentGroup, self).__init__(
            namespace=INVERSE_INTERFACE_LOOKUP_SCHEMA_NAMESPACE,
            schema_validator=PanoptesInverseInterfaceLookupEnrichmentSchemaValidator(),
            enrichment_ttl=enrichment_ttl,
            execute_frequency=execute_frequency)


class PanoptesL3InterfaceLookupEnrichmentGroup(PanoptesEnrichmentGroup):
    def __init__(self, enrichment_ttl, execute_frequency):
        super(PanoptesL3InterfaceLookupEnrichmentGroup, self).__init__(
            namespace=L3_INTERFACE_LOOKUP_SCHEMA_NAMESPACE,
            schema_validator=PanoptesL3InterfaceLookupEnrichmentSchemaValidator(),
            enrichment_ttl=enrichment_ttl,
            execute_frequency=execute_frequency)


class PanoptesInterfaceLookupEnrichmentGroup(PanoptesEnrichmentGroup):
    def __init__(self, enrichment_ttl, execute_frequency):
        super(PanoptesInterfaceLookupEnrichmentGroup, self).__init__(
            namespace=INTERFACE_LOOKUP_SCHEMA_NAMESPACE,
            schema_validator=PanoptesInterfaceLookupEnrichmentSchemaValidator(),
            enrichment_ttl=enrichment_ttl,
            execute_frequency=execute_frequency)


class PanoptesDeviceLookupEnrichmentGroup(PanoptesEnrichmentGroup):
    def __init__(self, enrichment_ttl, execute_frequency):
        super(PanoptesDeviceLookupEnrichmentGroup, self).__init__(
            namespace=DEVICE_LOOKUP_SCHEMA_NAMESPACE,
            schema_validator=PanoptesDeviceLookupEnrichmentSchemaValidator(),
            enrichment_ttl=enrichment_ttl,
            execute_frequency=execute_frequency)
