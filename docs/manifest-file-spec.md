# Manifest file

A binary file that lists SSTables currently in the database.
Structured like a WAL for atomic updates.

Each entry has:

* New sstables
* Deleted sstable ID
* Next SST ID

This WAL-like format allow readers to read even when a writer is writing, since
the writer works in append-only mode.

A CRC prefixed to each entry makes writing to manifest atomic in addition to
helping with corruption.

# File format (Version 1)

## Header

| Field | Type | Description |
|-------|------|-------------|
| Magic number | u32 | Magic number of the file. Must be `0xBEEFFE57`. |
| Version | u8 | Version of the file format. Must be `1`. |

## Entry

| Field         | Type      | Description |
|---------------|-----------|-------------|
| CRC32C        | u32       | CRC32C of the entry, excluding the CRC32C and the length field. |
| Length        | u32       | Length of "Data" field. |
| Next SST ID   | u64       | ID of the sstable.                |
| Added count   | u64       |                                   |
| Added         | SSTable[] | Array of SSTables added.          |
| Removed count | u64       |                                   |
| Removed       | u64[]     | Array of IDs of SSTables removed  |

### SSTable 

| Field   | Type   | Description             |
|---------|--------|-------------------------|
| ID      | u64    | ID of the sstable.      |
| Level   | u8     | Level of the sstable.   |
| Min key | string | Min key of the sstable. |
| Max key | string | Max key of the sstable. |

