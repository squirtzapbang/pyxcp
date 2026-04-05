#!/usr/bin/env python

"""XCP info/exploration tool."""

import argparse
from pprint import pprint

from pyxcp.cmdline import ArgumentParser
from pyxcp.types import TryCommandResult


def getPagInfo(x):
    result = {}
    if x.slaveProperties.supportsCalpag:
        status, pag = x.try_command(x.getPagProcessorInfo)
        if status == TryCommandResult.OK:
            result["maxSegments"] = pag.maxSegments
            result["pagProperties"] = {"freezeSupported": pag.pagProperties.freezeSupported}
            result["segments"] = []
            for i in range(pag.maxSegments):
                segment = {"index": i}

                # Mode 1: Standard info
                status, std_info = x.try_command(x.getSegmentInfo, 1, i, 0, 0)
                if status == TryCommandResult.OK:
                    segment["maxPages"] = std_info.maxPages
                    segment["addressExtension"] = std_info.addressExtension
                    segment["maxMapping"] = std_info.maxMapping
                    segment["compressionMethod"] = std_info.compressionMethod
                    segment["encryptionMethod"] = std_info.encryptionMethod

                    # Mode 0: Basic address info
                    # Mode 0, Info 0: Address
                    status, addr_info = x.try_command(x.getSegmentInfo, 0, i, 0, 0)
                    if status == TryCommandResult.OK:
                        segment["address"] = addr_info.basicInfo
                    # Mode 0, Info 1: Length
                    status, len_info = x.try_command(x.getSegmentInfo, 0, i, 1, 0)
                    if status == TryCommandResult.OK:
                        segment["length"] = len_info.basicInfo

                    # Mode 2: Address mapping info
                    if std_info.maxMapping > 0:
                        segment["mappings"] = []
                        for m in range(std_info.maxMapping):
                            mapping = {"index": m}
                            # Mode 2, Info 0: source address
                            status, src_addr = x.try_command(x.getSegmentInfo, 2, i, 0, m)
                            if status == TryCommandResult.OK:
                                mapping["sourceAddress"] = src_addr.mappingInfo
                            # Mode 2, Info 1: destination address
                            status, dst_addr = x.try_command(x.getSegmentInfo, 2, i, 1, m)
                            if status == TryCommandResult.OK:
                                mapping["destinationAddress"] = dst_addr.mappingInfo
                            # Mode 2, Info 2: length
                            status, map_len = x.try_command(x.getSegmentInfo, 2, i, 2, m)
                            if status == TryCommandResult.OK:
                                mapping["length"] = map_len.mappingInfo
                            segment["mappings"].append(mapping)

                    # Get info for each page
                    segment["pages"] = []
                    for p in range(std_info.maxPages):
                        status, pgi = x.try_command(x.getPageInfo, i, p)
                        if status == TryCommandResult.OK:
                            segment["pages"].append(
                                {
                                    "index": p,
                                    "properties": {
                                        "xcpWriteAccessWithEcu": pgi.properties.xcpWriteAccessWithEcu,
                                        "xcpWriteAccessWithoutEcu": pgi.properties.xcpWriteAccessWithoutEcu,
                                        "xcpReadAccessWithEcu": pgi.properties.xcpReadAccessWithEcu,
                                        "xcpReadAccessWithoutEcu": pgi.properties.xcpReadAccessWithoutEcu,
                                        "ecuAccessWithXcp": pgi.properties.ecuAccessWithXcp,
                                        "ecuAccessWithoutXcp": pgi.properties.ecuAccessWithoutXcp,
                                    },
                                    "initSegment": pgi.initSegment,
                                }
                            )

                    result["segments"].append(segment)
                else:
                    # If Mode 1 fails, we might still want to continue with next segment?
                    # The original code used 'break', which stops processing segments.
                    break
    return result


def getPgmInfo(x):
    result = {}
    if x.slaveProperties.supportsPgm:
        status, pgm = x.try_command(x.getPgmProcessorInfo)
        if status == TryCommandResult.OK:
            result["pgmProperties"] = pgm.pgmProperties
            result["maxSector"] = pgm.maxSector
            result["sectors"] = []
            for i in range(pgm.maxSector):
                sector = {"index": i}
                # Mode 0: get start address for this SECTOR
                status, info0 = x.try_command(x.getSectorInfo, 0, i)
                if status == TryCommandResult.OK:
                    sector["clearSequenceNumber"] = info0.clearSequenceNumber
                    sector["programSequenceNumber"] = info0.programSequenceNumber
                    sector["programmingMethod"] = info0.programmingMethod
                    sector["address"] = info0.sectorInfo
                else:
                    break

                # Mode 1: get length of this SECTOR [BYTE]
                status, info1 = x.try_command(x.getSectorInfo, 1, i)
                if status == TryCommandResult.OK:
                    sector["length"] = info1.sectorInfo

                # Mode 2: get name length of this SECTOR
                status, info2 = x.try_command(x.getSectorInfo, 2, i)
                if status == TryCommandResult.OK:
                    sector["nameLength"] = info2.nameLength

                result["sectors"].append(sector)
    return result


def main():
    parser = argparse.ArgumentParser(description="XCP info/exploration tool.")
    parser.add_argument("--no-daq", action="store_true", help="Do not query DAQ information.")
    parser.add_argument("--no-pag", action="store_true", help="Do not query PAG information.")
    parser.add_argument("--no-pgm", action="store_true", help="Do not query PGM information.")
    parser.add_argument("--no-ids", action="store_true", help="Do not scan implemented IDs.")
    ap = ArgumentParser(parser)

    with ap.run() as x:
        x.connect()
        if x.slaveProperties.optionalCommMode:
            x.try_command(x.getCommModeInfo, extra_msg="availability signaled by CONNECT, this may be a slave configuration error.")
        print("\nSlave Properties:")
        print("=================")
        pprint(x.slaveProperties)
        status, vers = x.try_command(x.getVersion)
        if status == TryCommandResult.OK:
            print("\nVersion:")
            print("========")
            print(f"   protocol : {vers.protocolMajor}.{vers.protocolMinor}")
            print(f"   transport: {vers.transportMajor}.{vers.transportMinor}")
        if not ap.args.no_ids:
            result = x.id_scanner()
            print("\n")
            print("Implemented IDs:")
            print("================")
            for key, value in result.items():
                print(f"{key}: {value}", end="\n\n")

        cps = x.getCurrentProtectionStatus()
        print("\nProtection Status")
        print("=================")
        for k, v in cps.items():
            print(f"    {k:6s}: {v}")
        x.cond_unlock()

        if not ap.args.no_daq:
            print("\nDAQ Info:")
            print("=========")
            if x.slaveProperties.supportsDaq:
                daq_info = x.getDaqInfo()
                pprint(daq_info)

                daq_pro = daq_info["processor"]
                daq_properties = daq_pro["properties"]
                if x.slaveProperties.transport_layer == "CAN":
                    print("")
                    if daq_properties["pidOffSupported"]:
                        print("*** pidOffSupported -- i.e. one CAN-ID per DAQ-list.")
                    else:
                        print("*** NO support for PID_OFF")
                num_predefined = daq_pro["minDaq"]
                print("\nPredefined DAQ-Lists")
                print("====================")
                if num_predefined > 0:
                    print(f"There are {num_predefined} predefined DAQ-lists")
                    for idx in range(num_predefined):
                        print(f"DAQ-List #{idx}\n____________\n")
                        status, dm = x.try_command(x.getDaqListMode, idx)
                        if status == TryCommandResult.OK:
                            print(dm)
                        status, di = x.try_command(x.getDaqListInfo, idx)
                        if status == TryCommandResult.OK:
                            print(di)
                else:
                    print("*** NO Predefined DAQ-Lists")
            else:
                print("*** DAQ IS NOT SUPPORTED .")

        if not ap.args.no_pag:
            print("\nPAG Info:")
            print("=========")
            if x.slaveProperties.supportsCalpag:
                pgi = getPagInfo(x)
                pprint(pgi)
            else:
                print("*** PAGING IS NOT SUPPORTED.")

        if not ap.args.no_pgm:
            print("\nPGM Info:")
            print("=========")
            if x.slaveProperties.supportsPgm:
                pgi = getPgmInfo(x)
                pprint(pgi)
            else:
                print("*** FLASH PROGRAMMING IS NOT SUPPORTED.")

        if x.slaveProperties.transport_layer == "CAN":
            print("\nTransport-Layer CAN:")
            print("====================")
            status, res = x.try_command(x.getSlaveID, 0)
            if status == TryCommandResult.OK:
                print("CAN identifier for CMD/STIM:\n", res)
            else:
                pass
                # print("*** GET_SLAVE_ID() IS NOT SUPPORTED.")  # no response from bc address ???

            print("\nPer DAQ-list Identifier")
            print("-----------------------")
            daq_id = 0
            while True:
                status, res = x.try_command(x.getDaqId, daq_id)
                if status == TryCommandResult.OK:
                    print(f"DAQ-list #{daq_id}:", res)
                    daq_id += 1
                else:
                    break
            if daq_id == 0:
                print("N/A")
        x.disconnect()
        print("\nDone.")


if __name__ == "__main__":
    main()
