# SSTable File format

All values are in big endian unless otherwise specified.

## Strings

Strings are stored as a length followed by the string data.
Length is a 64 bit unsigned integer.

## Structure

| Section           | Size         | Description                        |
|-------------------|--------------|------------------------------------|
| Header            | 9 bytes      | File header with metadata          |
| Data chunks       | dynamic      | Pages containing stored data       |
| Chunk directory   | dynamic      | Directory of chunk locations       |
| Footer            | 12 bytes     | File footer with summary info      |

## Header

| Field        | Type | Description                                     |
|--------------|------|-------------------------------------------------|
| Magic number | u32  | Magic number of the file. Must be `0xFAA7BEEF`. |
| Version      | u8   | Version of the file format.                     |

## Footer

| Field                | Type   | Description                        |
|----------------------|--------|------------------------------------|
| Ptr to chunk dir     | u64    | Offset to the chunk directory      |
| Chunk count          | u32    | Number of chunks in the file       |

## Data chunks

| Section      | Size         | Description  |
|--------------|--------------|--------------|
| Chunk header | 20 bytes     | Metadata for the chunk (see below) |
| Items        | dynamic      | Actual data stored in the chunk (see below)|


### Chunk header

| Field              | Type         | Description         |
|--------------------|--------------|---------------------|
| Item count         | u32          | Number of items     |
| Compressed size    | u64          | Size after compression |
| Uncompressed size  | u64          | Original size       |

### Item

| Field      | Type   | Description |
|------------|--------|-------------|
| Prefix len | u64    | Prefix length shared with previous key in this chunk. Should be 0 for first item. |
| Key suffix | string | Suffix of the key of the item. |
| Value      | string | The value of the item. |

Full key is computed by looking at previous key upto given prefix length and
adding key suffix to it. First key in the chunk does not share prefix with any
other item and it's prefix length should therefore be 0.

## Chunk directory

| Field | Type | Description |
|-------|------|-------------|
| Entries | chunk directory entry | Array of chunk directory entries. |

### Chunk directory entry

| Field | Type | Description |
|-------|------|-------------|
| Chunk offset | u64 | Offset to the chunk in the file. |
| Min key | string | Smallest key in chunk. |
| Max key | string | Largest key in chunk. |
