"""
Juniper device snmp mib classes
"""
from yahoo_panoptes.framework.utilities.snmp.mibs.base import oid, SNMPGauge32,\
    SNMPInteger, SNMPInteger32, SNMPString
from yahoo_panoptes.framework.utilities.snmp.mibs.snmpv2 import MibSNMPV2


class MibJuniper(MibSNMPV2):
    """
    Generic Juniper MIB
    """
    juniperMIB = oid(u'.1.3.6.1.4.1.2636')
    jnxMibs = juniperMIB + oid(u'3')                                                    # 1.3.6.1.4.1.2636.3
    jnxBoxAnatomy = jnxMibs + oid(u'1')                                                 # 1.3.6.1.4.1.2636.3.1
    jnxOperatingTable = jnxBoxAnatomy + oid(u'13')                                      # 1.3.6.1.4.1.2636.3.1.13
    jnxOperatingEntry = jnxOperatingTable + oid(u'1')                                   # 1.3.6.1.4.1.2636.3.1.13.1
    jnxOperatingContentsIndex = jnxOperatingEntry + oid(u'1', SNMPInteger32)            # 1.3.6.1.4.1.2636.3.1.13.1.1
    jnxOperatingL1Index = jnxOperatingEntry + oid(u'2', SNMPInteger)                    # 1.3.6.1.4.1.2636.3.1.13.1.2
    jnxOperatingDescr = jnxOperatingEntry + oid(u'5', SNMPString)                       # 1.3.6.1.4.1.2636.3.1.13.1.5
    jnxOperatingState = jnxOperatingEntry + oid(u'6', SNMPInteger)                      # 1.3.6.1.4.1.2636.3.1.13.1.6
    jnxOperatingTemp = jnxOperatingEntry + oid(u'7', SNMPGauge32)                       # 1.3.6.1.4.1.2636.3.1.13.1.7
    jnxOperatingCPU = jnxOperatingEntry + oid(u'8', SNMPGauge32)                        # 1.3.6.1.4.1.2636.3.1.13.1.8
    jnxOperatingBuffer = jnxOperatingEntry + oid(u'11', SNMPGauge32)                    # 1.3.6.1.4.1.2636.3.1.13.1.11
    jnxOperatingHeap = jnxOperatingEntry + oid(u'12', SNMPGauge32)                      # 1.3.6.1.4.1.2636.3.1.13.1.12
    jnxOperatingMemory = jnxOperatingEntry + oid(u'15', SNMPGauge32)                    # 1.3.6.1.4.1.2636.3.1.13.1.15
    jnxOperating1MinLoadAvg = jnxOperatingEntry + oid(u'20', SNMPGauge32)               # 1.3.6.1.4.1.2636.3.1.13.1.20
    jnxOperating5MinLoadAvg = jnxOperatingEntry + oid(u'21', SNMPGauge32)               # 1.3.6.1.4.1.2636.3.1.13.1.21
    jnxOperating15MinLoadAvg = jnxOperatingEntry + oid(u'22', SNMPGauge32)              # 1.3.6.1.4.1.2636.3.1.13.1.22

    jnxJsSPUMonitoringCPUUsage = juniperMIB + oid(u'3.39.1.12')  # TODO no entry for this oid entries for ...39.1.11
