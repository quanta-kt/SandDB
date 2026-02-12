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
    page_size = read_u32(f)

    print(f"=== HEADER ===")
    print(f"  magic: {hex(magic)}")
    print(f"  version: {version}")
    print(f"  page_size: {page_size}\n")

    f.seek(-12, os.SEEK_END)

    chunk_dir_pos = read_u64(f)
    chunk_count = read_u32(f)

    print(f"=== FOOTER ===")
    print(f"  chunk_dir_pos: {hex(chunk_dir_pos)}")
    print(f"  chunk_count: {hex(chunk_count)}\n")

    chunks = []

    f.seek(chunk_dir_pos)
    print("=== CHUNK DIRECTORY ===")
    for i in range(chunk_count):
        chunk_offset = read_u64(f)
        min_key = read_string(f)
        max_key = read_string(f)

        chunks.append(chunk_offset)

        print(f"  === CHUNK {i} ===")
        print(f"    chunk_offset: {(chunk_offset)}")
        print(f"    min_key: {min_key}")
        print(f"    max_key: {max_key}")

    print()

    print(f"=== CHUNKS ===\n")
    for chunk_offset in chunks:
        f.seek(chunk_offset)

        item_count = read_u32(f)
        compressed_size = read_u64(f)
        uncompressed_size = read_u64(f)

        print(f"  === CHUNK {i} ===")
        print(f"    item_count: {item_count}")
        print(f"    compressed_size: {compressed_size}")
        print(f"    uncompressed_size: {uncompressed_size}")

        for j in range(item_count):
            key = read_string(f)
            value = read_string(f)
            print(f"    {key} => {value}")
