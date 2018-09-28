"""
SNMP V2 mib class
"""
from .base import oid, Mib


class MibSNMPV2(Mib):
    """
    Mib class with snmp v2 oids
    """
    sysDescr = oid('.1.3.6.1.2.1.1.1')
    interfaces = oid('.1.3.6.1.2.1.2')
