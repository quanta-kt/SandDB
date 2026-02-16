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

    count = 0

    while True:
        crc = f.read(4)
        if not crc:
            break

        crc = struct.unpack(">I", crc)[0]
        size = read_u64(f)
        key = read_string(f);
        value = read_string(f);
        print(f"crc: {crc}")
        print(f"len: {size}")
        print(f"{key} => {value}");
        print("---\n");

        count += 1


    print(f"read {count} records")
