"""
SNMP V2 mib class
"""
from yahoo_panoptes.framework.utilities.snmp.mibs.base import oid, Mib


class MibSNMPV2(Mib):
    """
    Mib class with snmp v2 oids
    """
    sysDescr = oid('.1.3.6.1.2.1.1.1')
    interfaces = oid('.1.3.6.1.2.1.2')

    hrStorageType = oid('.1.3.6.1.2.1.25.2.3.1.2')
    hrStorageDescr = oid('.1.3.6.1.2.1.25.2.3.1.3')
    hrStorageAllocationUnits = oid('.1.3.6.1.2.1.25.2.3.1.4')
    hrStorageSize = oid('.1.3.6.1.2.1.25.2.3.1.5')
    hrStorageUsed = oid('.1.3.6.1.2.1.25.2.3.1.6')
    hrStorageAllocationFailures = oid('.1.3.6.1.2.1.25.2.3.1.7')
