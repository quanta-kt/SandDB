# SandDB

## Overview

SandDB is an on-disk persistent key-value store that uses Log-Structured Merge Trees (LSM-trees).
The goal is to be optimized for write-heavy workloads, while still providing good read performance.

A lot of inspiration comes from [RocksDB](https://github.com/facebook/rocksdb) and
[Git's reftable](https://git-scm.com/docs/reftable).

## Roadmap

- [x] Read/write operations
- [x] SSTable compaction
- [ ] Bloom filters
- [ ] Durability with WAL
- [ ] Atomicity via MVCC snapshots
- [ ] C API
- [ ] TCP server interface

## Architecture

### LSM-tree Structure

SandDB organizes data across multiple levels:

- **Memtable**: In-memory data structure that is flushed to disk when it reaches a threshold.
- **Level 0**: Stored on disk as SSTables, contains the most recently flushed memtables.
- **Levels 1-N**: Contains tables that are merged from the previous level.

## File Formats

### SSTable Format

See [SSTable File Specification](docs/sst-file-spec.md) for detailed format documentation.

### Manifest Format

The manifest file tracks available SSTables and their metadata such as level and key range.

See [Manifest File Specification](docs/manifest-file-spec.md) for detailed format documentation.

## Usage

### Building

```bash
cargo build --release
```

### CLI Interface

A simple CLI is provided for testing the database.
This is by no means a production-ready interface.

**Running:**

```bash
cargo run --bin cli <database-directory>
```

Available commands:
- `set <key> <value>` - Store a key-value pair
- `get <key>` - Retrieve a value by key
- `exit` - Exit the CLI

Example session:
```
> set user:1 "John Doe"
Key set
> get user:1
John Doe
> exit
```
