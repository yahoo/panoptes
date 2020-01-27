"""
This module implements a Panoptes Plugin that polls Juniper Devices for BGP Metrics
"""

from cached_property import threaded_cached_property
import ipaddress

from yahoo_panoptes.framework.enrichment import PanoptesEnrichmentSet
from yahoo_panoptes.framework.utilities.helpers import transform_index_ipv6_address, is_python_2
from yahoo_panoptes.plugins.enrichment.generic.snmp.plugin_enrichment_generic_snmp import \
    PanoptesEnrichmentGenericSNMPPlugin

jnxBgpM2PeerEntry = '.1.3.6.1.4.1.2636.5.1.1.2.1.1.1'

jnxBgpM2PeerState = jnxBgpM2PeerEntry + '.2'
jnxBgpM2PeerStatus = jnxBgpM2PeerEntry + '.3'
jnxBgpM2PeerLocalAs = jnxBgpM2PeerEntry + '.9'
jnxBgpM2PeerRemoteAddr = jnxBgpM2PeerEntry + '.11'
jnxBgpM2PeerRemoteAs = jnxBgpM2PeerEntry + '.13'
jnxBgpM2PeerIndex = jnxBgpM2PeerEntry + '.14'

jnxBgpM2PeerEventTimesEntry = '.1.3.6.1.4.1.2636.5.1.1.2.4.1.1'
jnxBgpM2PeerFsmEstablishedTime = jnxBgpM2PeerEventTimesEntry + '.1'

jnxBgpM2PeerCountersEntry = '.1.3.6.1.4.1.2636.5.1.1.2.6.1.1'
jnxBgpM2PeerInUpdates = jnxBgpM2PeerCountersEntry + '.1'
jnxBgpM2PeerOutUpdates = jnxBgpM2PeerCountersEntry + '.2'
jnxBgpM2PeerFsmEstablishedTrans = jnxBgpM2PeerCountersEntry + '.5'

jnxBgpM2PrefixCountersEntry = '.1.3.6.1.4.1.2636.5.1.1.2.6.2.1'
jnxBgpM2PrefixInPrefixes = jnxBgpM2PrefixCountersEntry + '.7'
jnxBgpM2PrefixInPrefixesAccepted = jnxBgpM2PrefixCountersEntry + '.8'
jnxBgpM2PrefixInPrefixesRejected = jnxBgpM2PrefixCountersEntry + '.9'
jnxBgpM2PrefixInPrefixesActive = jnxBgpM2PrefixCountersEntry + '.11'

ifXEntry = '.1.3.6.1.2.1.31.1.1.1'
ifName = ifXEntry + '.1'
ifHighSpeed = ifXEntry + '.15'
ifAlias = ifXEntry + '.18'

ipNetToPhysicalState = '.1.3.6.1.2.1.4.35.1.7'


def transform_ip_octstr(ip_octstr):
    """
    Converts octet strings containing IPv4 or IPv6 addresses into a human readable format.
    Args:
        ip_octstr (str): The octet string returned via SNMP
    Returns:
        str: human readable IPv4 or IPv6 IP address:
            2001:dea:0:10::82, 1.2.3.4
    """
    if isinstance(ip_octstr, bytes) and not is_python_2():
        byte_arr = [u'{:02x}'.format(_) for _ in ip_octstr]
    else:
        byte_arr = ['%0.2x' % ord(_) for _ in ip_octstr]
    if len(byte_arr) == 4:
        return '.'.join([str(int(x, 16)) for x in byte_arr])
    else:
        byte_string = ""
        for p, i in enumerate(byte_arr):
            if p % 2 != 0:
                byte_string += '{}{}:'.format(byte_arr[p - 1].lstrip('0'), byte_arr[p])
        # ipaddress formats the string nicely -- collapses blocks of zero
        return str(ipaddress.ip_address(byte_string[:-1].encode('ascii').decode('ascii')))


def transformer(value, transform):
    if transform == 'ip':
        return transform_ip_octstr(value)
    elif transform == 'index_ipv6_address':
        return transform_index_ipv6_address(value)
    else:
        return value


class JuniperBGPInfoPluginEnrichmentMetrics(PanoptesEnrichmentGenericSNMPPlugin):

    def __init__(self):
        super(JuniperBGPInfoPluginEnrichmentMetrics, self).__init__()

    def peer_external_connection(self):
        return "'unknown-peer_external_connection'"

    def bgp_adjacency_subtype(self):
        return "'unknown-bgp_adjacency_subtype'"

    def bgp_adjacency_type(self):
        return "'unknown-bgp_adjacency_type'"

    def _build_oids_map(self):

        def decode_byte_keys(dictionary):
            for key in dictionary:
                if isinstance(dictionary[key], bytes):
                    dictionary[key] = dictionary[key].decode(u'ascii', u'ignore')
            return dictionary

        self._oids_map = {
            "peer_state": {
                "method": "bulk_walk",
                "oid": jnxBgpM2PeerState
            },
            "peer_status": {
                "method": "bulk_walk",
                "oid": jnxBgpM2PeerStatus
            },
            "peer_local_as": {
                "method": "bulk_walk",
                "oid": jnxBgpM2PeerLocalAs
            },
            "peer_remote_as": {
                "method": "bulk_walk",
                "oid": jnxBgpM2PeerRemoteAs
            },
            "peer_index": {
                "method": "bulk_walk",
                "oid": jnxBgpM2PeerIndex
            },
            "peer_session_established_time": {
                "method": "bulk_walk",
                "oid": jnxBgpM2PeerFsmEstablishedTime
            },
            "peer_in_updates": {
                "method": "bulk_walk",
                "oid": jnxBgpM2PeerInUpdates
            },
            "peer_out_updates": {
                "method": "bulk_walk",
                "oid": jnxBgpM2PeerOutUpdates
            },
            "peer_session_transitions": {
                "method": "bulk_walk",
                "oid": jnxBgpM2PeerFsmEstablishedTrans
            },
            "prefix_in_prefixes": {
                "method": "bulk_walk",
                "oid": jnxBgpM2PrefixInPrefixes,
                "index_transform": self._join_jnxBGP_prefix_counters_entry
            },
            "prefix_in_prefixes_accepted": {
                "method": "bulk_walk",
                "oid": jnxBgpM2PrefixInPrefixesAccepted,
                "index_transform": self._join_jnxBGP_prefix_counters_entry
            },
            "prefix_in_prefixes_rejected": {
                "method": "bulk_walk",
                "oid": jnxBgpM2PrefixInPrefixesRejected,
                "index_transform": self._join_jnxBGP_prefix_counters_entry
            },
            "prefix_in_prefixes_active": {
                "method": "bulk_walk",
                "oid": jnxBgpM2PrefixInPrefixesActive,
                "index_transform": self._join_jnxBGP_prefix_counters_entry
            },
            "interface_name": {
                "method": "static",
                "values": decode_byte_keys(
                    self._interface_to_bgp(ifName)
                )
            },
            "interface_speed": {
                "method": "static",
                "values": self._interface_to_bgp(ifHighSpeed)
            },
            "interface_alias": {
                "method": "static",
                "values": decode_byte_keys(
                    self._interface_to_bgp(ifAlias)
                )
            },
            "local_address": {
                "method": "static",
                "values": self._create_static_local_remote_addr[0]
            },
            "peer_address": {
                "method": "static",
                "values": self._create_static_local_remote_addr[1]
            }
        }

    def _interface_to_bgp(self, oid):
        """
        Does a 2 way jump using the previously made dimensions table and maps
        the jnxBgpM2PeerIndex indices directly to the corresponding ifXTable
        entry depending on the oid passed in
        Returns:
            dict: jnxBgpM2PeerIndex keys mapping to the corresponding ifXTable entry values
        """
        varbinds = self._snmp_connection.bulk_walk(oid)
        return self._join_interface_and_bgp_info(self._dimensions_join, varbinds)

    def _join_interface_and_bgp_info(self, mappings, varbinds):
        """
        Helper function for _interface_to_bgp
        Returns:
            dict: jnxBgpM2PeerIndex keys mapping to the corresponding ifXTable entry values
        """

        generic_mapping = dict()
        interface_to_bgp = dict()
        for varbind in varbinds:
            generic_mapping[varbind.index] = varbind.value

        for key, value in mappings.items():

            if value in generic_mapping:
                interface_to_bgp[key] = generic_mapping[value]

        return interface_to_bgp

    @threaded_cached_property
    def _join_jnxBGP_prefix_counters_entry(self):
        return {index: self._process_peer_index_to_inet[index.split('.')[0]] for index in
                self._prefix_counters_entry_indices}

    @threaded_cached_property
    def _prefix_counters_entry_indices(self):
        """
        Returns:
            list: Array of jnxBgpM2PrefixInPrefixes indices to later iterate over
        """
        indices = []
        varbinds = self._snmp_connection.bulk_walk(jnxBgpM2PrefixInPrefixes)
        for varbind in varbinds:
            indices.append(varbind.index)
        return indices

    @threaded_cached_property
    def _bgp_remote_addr(self):
        varbinds = self._snmp_connection.bulk_walk(jnxBgpM2PeerRemoteAddr)
        return varbinds

    @threaded_cached_property
    def _ipNetToPhysicalState(self):
        return self._snmp_connection.bulk_walk(ipNetToPhysicalState)

    @threaded_cached_property
    def _jnx_peer_index(self):
        return self._snmp_connection.bulk_walk(jnxBgpM2PeerIndex)

    @threaded_cached_property
    def _create_static_local_remote_addr(self):
        """
        Maps jnxBgpM2PeerIndices to their corresponding Local / Peering Addresses
        Returns
            list: [jnxBgpM2PeerIndex to Local Address, jnxBgpM2PeerIndex to Peering Address]
        """
        peer_index_to_local_address = dict()
        peer_index_to_peer_address = dict()
        # making the assumption that (IPv4 - IPv4) & (IPv6 - IPv6)
        for varbind in self._jnx_peer_index:

            peer_index = varbind.index.split('.')

            if int(peer_index[1]) == 1:  # IPv4
                local_address = ".".join(peer_index[2:6])
                peer_address = ".".join(peer_index[7:])
            else:  # IPv6
                local_address = transformer(".".join(peer_index[2:18]), 'index_ipv6_address')
                peer_address = transformer(".".join(peer_index[19:]), 'index_ipv6_address')

            peer_index_to_local_address[varbind.index] = local_address
            peer_index_to_peer_address[varbind.index] = peer_address

        return [peer_index_to_local_address, peer_index_to_peer_address]

    @threaded_cached_property
    def _process_peer_index_to_inet(self):
        """
        Returns:
            dict: The jnxBgpM2PeerIndex table inverted
        """
        idx_to_inet = {}
        for varbind in self._jnx_peer_index:
            idx_to_inet[varbind.value] = varbind.index
        return idx_to_inet

    @threaded_cached_property
    def _dimensions_join(self):
        """
        Returns:
            dict: Mapping from BGP Peer Table to the Interface Table
        """
        ipaddr_to_ifidx = {}
        for item in self._ipNetToPhysicalState:
            indices = item.index.split('.')[:3]
            ipaddr = str(".".join(item.index.split('.')[3:]))

            if int(indices[1]) == 2:
                ipaddr = transformer(ipaddr, 'index_ipv6_address')

            ipaddr_to_ifidx[ipaddr] = str(indices[0])

        bgp_peer_to_ifidx = {}
        remoteaddr = self._bgp_remote_addr
        for item in remoteaddr:
            ipaddr = transform_ip_octstr(item.value)
            if ipaddr in ipaddr_to_ifidx:
                bgp_peer_to_ifidx[item.index] = ipaddr_to_ifidx[ipaddr]

        return bgp_peer_to_ifidx

    def _build_metrics_groups_conf(self):
        self._metrics_groups = [
            {
                "dimensions": {
                    "peer_local_as": {
                        "value": "peer_local_as.$index"
                    },
                    "peer_remote_as": {
                        "value": "peer_remote_as.$index"
                    },
                    "interface_name": {
                        "value": "interface_name.$index"
                    },
                    "interface_alias": {
                        "value": "interface_alias.$index"
                    },
                    "bgp_adjacency_type": {
                        "value": self.bgp_adjacency_type()
                    },
                    "bgp_adjacency_subtype": {
                        "value": self.bgp_adjacency_subtype()
                    },
                    "peer_external_connection": {
                        "value": self.peer_external_connection()
                    },
                    "local_address": {
                        "value": "local_address.$index"
                    },
                    "peer_address": {
                        "value": "peer_address.$index"
                    }

                },
                "group_name": "bgp_session",
                "metrics": {
                    "peer_state": {
                        "metric_type": "gauge",
                        "value": "peer_state.$index"
                    },
                    "peer_status": {
                        "metric_type": "gauge",
                        "value": "peer_status.$index"
                    },
                    "prefix_in_prefixes": {
                        "metric_type": "gauge",
                        "value": "prefix_in_prefixes.$index"
                    },
                    "prefix_in_prefixes_accepted": {
                        "metric_type": "gauge",
                        "value": "prefix_in_prefixes_accepted.$index"
                    },
                    "prefix_in_prefixes_rejected": {
                        "metric_type": "gauge",
                        "value": "prefix_in_prefixes_rejected.$index"
                    },
                    "prefix_in_prefixes_active": {
                        "metric_type": "gauge",
                        "value": "prefix_in_prefixes_active.$index"
                    },
                    "interface_speed": {
                        "metric_type": "gauge",
                        "transform": "lambda x: x * 1000000",
                        "value": "interface_speed.$index"
                    },
                    "peer_session_established_time": {
                        "metric_type": "gauge",
                        "value": "peer_session_established_time.$index"
                    },
                    "peer_session_transitions": {
                        "metric_type": "gauge",
                        "value": "peer_session_transitions.$index"
                    },
                    "peer_in_updates": {
                        "metric_type": "counter",
                        "value": "peer_in_updates.$index"
                    },
                    "peer_out_updates": {
                        "metric_type": "counter",
                        "value": "peer_out_updates.$index"
                    },

                }
            }
        ]

    def get_enrichment(self):
        self._build_oids_map()
        self._build_metrics_groups_conf()
        enrichment_set = {
            "oids": self.oids_map,
            "metrics_groups": self.metrics_groups
        }

        try:
            self.enrichment_group.add_enrichment_set(PanoptesEnrichmentSet(self.device_fqdn, enrichment_set))
        except Exception as e:
            self._logger.error('Error while adding enrichment set {} to enrichment group for the device {}: {}'.
                               format(enrichment_set, self.device_fqdn, repr(e)))

        self.enrichment_group_set.add_enrichment_group(self.enrichment_group)

        self._logger.debug('Metrics enrichment for device {}: {}'.format(self.device_fqdn, self.enrichment_group_set))

        return self.enrichment_group_set
