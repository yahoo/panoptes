ifXTable = '.1.3.6.1.2.1.31.1.1.1'
ifName = ifXTable + '.1'
ifInMulticastPkts = ifXTable + '.2'
ifHCInOctets = ifXTable + '.6'
ifHCInUcastPkts = ifXTable + '.7'
ifHCInMulticastPkts = ifXTable + '.8'
ifHCInBroadcastPkts = ifXTable + '.9'
ifHCOutOctets = ifXTable + '.10'
ifHCOutUcastPkts = ifXTable + '.11'
ifHCOutMulticastPkts = ifXTable + '.12'
ifHCOutBroadcastPkts = ifXTable + '.13'
ifHighSpeed = ifXTable + '.15'
ifAlias = ifXTable + '.18'

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
