ifTable = u'.1.3.6.1.2.1.2.2.1'
ifDescr = ifTable + u'.2'
ifType = ifTable + u'.3'
ifMtu = ifTable + u'.4'
ifSpeed = ifTable + u'.5'
ifPhysAddress = ifTable + u'.6'
ifAdminStatus = ifTable + u'.7'
ifOperStatus = ifTable + u'.8'
ifInDiscards = ifTable + u'.13'
ifInErrors = ifTable + u'.14'
ifOutDiscards = ifTable + u'.19'
ifOutErrors = ifTable + u'.20'

ifTypeEnum = [(u"other", 1), (u"regular1822", 2), (u"hdh1822", 3), (u"ddnX25", 4), (u"rfc877x25", 5),
              (u"ethernetCsmacd", 6), (u"iso88023Csmacd", 7), (u"iso88024TokenBus", 8), (u"iso88025TokenRing", 9),
              (u"iso88026Man", 10), (u"starLan", 11), (u"proteon10Mbit", 12), (u"proteon80Mbit", 13),
              (u"hyperchannel", 14), (u"fddi", 15), (u"lapb", 16), (u"sdlc", 17), (u"ds1", 18), (u"e1", 19),
              (u"basicISDN", 20), (u"primaryISDN", 21), (u"propPointToPointSerial", 22), (u"ppp", 23),
              (u"softwareLoopback", 24), (u"eon", 25), (u"ethernet3Mbit", 26), (u"nsip", 27), (u"slip", 28),
              (u"ultra", 29), (u"ds3", 30), (u"sip", 31), (u"frameRelay", 32), (u"rs232", 33), (u"para", 34),
              (u"arcnet", 35), (u"arcnetPlus", 36), (u"atm", 37), (u"miox25", 38), (u"sonet", 39),
              (u"x25ple", 40), (u"iso88022llc", 41), (u"localTalk", 42), (u"smdsDxi", 43), (u"frameRelayService", 44),
              (u"v35", 45), (u"hssi", 46), (u"hippi", 47), (u"modem", 48), (u"aal5", 49), (u"sonetPath", 50),
              (u"sonetVT", 51), (u"smdsIcip", 52), (u"propVirtual", 53), (u"propMultiplexor", 54), (u"ieee80212", 55),
              (u"fibreChannel", 56), (u"hippiInterface", 57), (u"frameRelayInterconnect", 58), (u"aflane8023", 59),
              (u"aflane8025", 60), (u"cctEmul", 61), (u"fastEther", 62), (u"isdn", 63), (u"v11", 64), (u"v36", 65),
              (u"g703at64k", 66), (u"g703at2mb", 67), (u"qllc", 68), (u"fastEtherFX", 69), (u"channel", 70),
              (u"ieee80211", 71), (u"ibm370parChan", 72), (u"escon", 73), (u"dlsw", 74), (u"isdns", 75), (u"isdnu", 76),
              (u"lapd", 77), (u"ipSwitch", 78), (u"rsrb", 79), (u"atmLogical", 80), (u"ds0", 81), (u"ds0Bundle", 82),
              (u"bsc", 83), (u"async", 84), (u"cnr", 85), (u"iso88025Dtr", 86), (u"eplrs", 87), (u"arap", 88),
              (u"propCnls", 89), (u"hostPad", 90), (u"termPad", 91), (u"frameRelayMPI", 92), (u"x213", 93),
              (u"adsl", 94), (u"radsl", 95), (u"sdsl", 96), (u"vdsl", 97), (u"iso88025CRFPInt", 98), (u"myrinet", 99),
              (u"voiceEM", 100), (u"voiceFXO", 101), (u"voiceFXS", 102), (u"voiceEncap", 103), (u"voiceOverIp", 104),
              (u"atmDxi", 105), (u"atmFuni", 106), (u"atmIma", 107), (u"pppMultilinkBundle", 108),
              (u"ipOverCdlc", 109), (u"ipOverClaw", 110), (u"stackToStack", 111), (u"virtualIpAddress", 112),
              (u"mpc", 113), (u"ipOverAtm", 114), (u"iso88025Fiber", 115), (u"tdlc", 116), (u"gigabitEthernet", 117),
              (u"hdlc", 118), (u"lapf", 119), (u"v37", 120), (u"x25mlp", 121), (u"x25huntGroup", 122),
              (u"transpHdlc", 123), (u"interleave", 124), (u"fast", 125), (u"ip", 126), (u"docsCableMaclayer", 127),
              (u"docsCableDownstream", 128), (u"docsCableUpstream", 129), (u"a12MppSwitch", 130), (u"tunnel", 131),
              (u"coffee", 132), (u"ces", 133), (u"atmSubInterface", 134), (u"l2vlan", 135), (u"l3ipvlan", 136),
              (u"l3ipxvlan", 137), (u"digitalPowerline", 138), (u"mediaMailOverIp", 139), (u"dtm", 140), (u"dcn", 141),
              (u"ipForward", 142), (u"msdsl", 143), (u"ieee1394", 144), (u"if-gsn", 145), (u"dvbRccMacLayer", 146),
              (u"dvbRccDownstream", 147), (u"dvbRccUpstream", 148), (u"atmVirtual", 149), (u"mplsTunnel", 150),
              (u"srp", 151), (u"voiceOverAtm", 152), (u"voiceOverFrameRelay", 153), (u"idsl", 154),
              (u"compositeLink", 155), (u"ss7SigLink", 156), (u"propWirelessP2P", 157), (u"frForward", 158),
              (u"rfc1483", 159), (u"usb", 160), (u"ieee8023adLag", 161), (u"bgppolicyaccounting", 162),
              (u"frf16MfrBundle", 163), (u"h323Gatekeeper", 164), (u"h323Proxy", 165),
              (u"mpls", 166), (u"mfSigLink", 167), (u"hdsl2", 168), (u"shdsl", 169), (u"ds1FDL", 170), (u"pos", 171),
              (u"dvbAsiIn", 172), (u"dvbAsiOut", 173), (u"plc", 174), (u"nfas", 175), (u"tr008", 176),
              (u"gr303RDT", 177), (u"gr303IDT", 178), (u"isup", 179), (u"propDocsWirelessMaclayer", 180),
              (u"propDocsWirelessDownstream", 181), (u"propDocsWirelessUpstream", 182), (u"hiperlan2", 183),
              (u"propBWAp2Mp", 184), (u"sonetOverheadChannel", 185), (u"digitalWrapperOverheadChannel", 186),
              (u"aal2", 187), (u"radioMAC", 188), (u"atmRadio", 189), (u"imt", 190), (u"mvl", 191),
              (u"reachDSL", 192), (u"frDlciEndPt", 193), (u"atmVciEndPt", 194), (u"opticalChannel", 195),
              (u"opticalTransport", 196), (u"propAtm", 197), (u"voiceOverCable", 198), (u"infiniband", 199),
              (u"teLink", 200), (u"q2931", 201), (u"virtualTg", 202), (u"sipTg", 203), (u"sipSig", 204),
              (u"docsCableUpstreamChannel", 205), (u"econet", 206), (u"pon155", 207),
              (u"pon622", 208), (u"bridge", 209), (u"linegroup", 210), (u"voiceEMFGD", 211), (u"voiceFGDEANA", 212),
              (u"voiceDID", 213), (u"mpegTransport", 214), (u"sixToFour", 215), (u"gtp", 216), (u"pdnEtherLoop1", 217),
              (u"pdnEtherLoop2", 218), (u"opticalChannelGroup", 219), (u"homepna", 220), (u"gfp", 221),
              (u"ciscoISLvlan", 222), (u"actelisMetaLOOP", 223), (u"fcipLink", 224), (u"rpr", 225), (u"qam", 226),
              (u"lmp", 227), (u"cblVectaStar", 228), (u"docsCableMCmtsDownstream", 229), (u"adsl2", 230),
              (u"macSecControlledIF", 231), (u"macSecUncontrolledIF", 232), (u"aviciOpticalEther", 233),
              (u"atmbond", 234), (u"voiceFGDOS", 235), (u"mocaVersion1", 236), (u"ieee80216WMAN", 237),
              (u"adsl2plus", 238), (u"dvbRcsMacLayer", 239), (u"dvbTdm", 240), (u"dvbRcsTdma", 241), (u"x86Laps", 242),
              (u"wwanPP", 243), (u"wwanPP2", 244), (u"voiceEBS", 245), (u"ifPwType", 246), (u"ilan", 247),
              (u"pip", 248), (u"aluELP", 249), (u"gpon", 250), (u"vdsl2", 251)]


if_table_oids = [ifMtu, ifSpeed, ifAdminStatus, ifPhysAddress, ifOperStatus, ifInErrors, ifOutErrors,
                 ifInDiscards, ifOutDiscards]

if_status_code = {u'1': u'up',
                  u'2': u'down',
                  u'3': u'testing',
                  u'4': u'unknown',
                  u'5': u'dormant',
                  u'6': u'notPresent',
                  u'7': u'lowerLayerDown'}


def getIfTypeDesc(ifType):
    return ifTypeEnum[int(ifType) - 1][0]


def getIfStatus(ifStatus):
    return if_status_code.get(ifStatus)
