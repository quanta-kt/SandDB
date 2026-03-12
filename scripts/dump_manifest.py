import sys
import os
import struct

file = sys.argv[1]

def read_u8(f):
    return struct.unpack(">B", f.read(1))[0]

def read_u64(f):
    return struct.unpack(">Q", f.read(8))[0]

def read_u32(f):
    return struct.unpack(">I", f.read(4))[0]

def read_string(f):
    length = read_u64(f)
    data = f.read(length)
    return repr(data)[1:]

with open(file, "rb") as f:
    f.seek(0)
    magic = read_u32(f)
    version = read_u8(f)

    print(f"=== HEADER ===")
    print(f"  magic: {hex(magic)}")
    print(f"  version: {version}")

    while True:
        crc = read_u32(f)
        length = read_u32(f)
        next_sst_id = read_u64(f)

        print(f"=== ENTRY ===")
        print(f"  crc: {hex(crc)}")
        print(f"  length: {length}")
        print(f"  next_sst_id: {next_sst_id}\n")


        added_count = read_u64(f)
        print(f"  === ADDED {added_count} SSTs ===")

        for i in range(added_count):
            eid = read_u64(f)
            level = read_u8(f)
            min_key = read_string(f)
            max_key = read_string(f)

            print(f"    id: {eid}")
            print(f"    level: {level}")
            print(f"    min_key: {min_key}")
            print(f"    max_key: {max_key}\n")

        removed_count = read_u64(f)
        print(f"  === REMOVED {removed_count} SSTs ===")

        for i in range(removed_count):
            eid = read_u64(f)
            print(f"    id: {eid}")

        current_pos = f.tell()
        if current_pos == f.seek(0, os.SEEK_END):
            break

        f.seek(current_pos)
        
        print()

