ifXTable = u'.1.3.6.1.2.1.31.1.1.1'
ifName = ifXTable + u'.1'
ifInMulticastPkts = ifXTable + u'.2'
ifHCInOctets = ifXTable + u'.6'
ifHCInUcastPkts = ifXTable + u'.7'
ifHCInMulticastPkts = ifXTable + u'.8'
ifHCInBroadcastPkts = ifXTable + u'.9'
ifHCOutOctets = ifXTable + u'.10'
ifHCOutUcastPkts = ifXTable + u'.11'
ifHCOutMulticastPkts = ifXTable + u'.12'
ifHCOutBroadcastPkts = ifXTable + u'.13'
ifHighSpeed = ifXTable + u'.15'
ifAlias = ifXTable + u'.18'

ifx_table_oids = [ifHCInOctets,
                  ifHCInUcastPkts,
                  ifHCInMulticastPkts,
                  ifHCInBroadcastPkts,
                  ifHCOutOctets,
                  ifHCOutUcastPkts,
                  ifHCOutMulticastPkts,
                  ifHCOutBroadcastPkts,
                  ifHighSpeed,
                  ]
