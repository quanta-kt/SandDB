# Manifest file

Each database has a manifest file that lists the SSTables currently in the database.

It is a binary file with sequential entries of WAL-like events such as:

* add sstable
* remove sstable

This WAL-like events format allow readers to read even when a writer is writing, since
the writer works in append-only mode.

# File format (Version 1)

## Header

| Field | Type | Description |
|-------|------|-------------|
| Magic number | u32 | Magic number of the file. Must be `0xBEEFFE57`. |
| Version | u8 | Version of the file format. Must be `1`. |
| Next SST ID | 64u | Next ID to use for SST filename |

## Entry

| Field | Type | Description |
|-------|------|-------------|
| CRC32C | u32 | CRC32C of the entry, excluding the CRC32C and the length field. |
| Length | u32 | Length of the entry. |
| Type | u8 | Type of the entry. 1 = add sstable, 2 = remove sstable. |
| Data | depends on the type | Data of the entry. |

### Data

#### Add sstable

| Field | Type | Description |
|-------|------|-------------|
| Level | u8 | Level of the sstable. |
| Min key | string | Min key of the sstable. |
| Max key | string | Max key of the sstable. |
| ID | u64 | ID of the sstable. |

#### Remove sstable

| Field | Type | Description |
|-------|------|-------------|
| ID | u64 | ID of the sstable. |
