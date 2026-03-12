use std::collections::BTreeMap;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::Cursor;

use crate::crc::crc32c;
use crate::io_ext::ReadExt;

use super::MAGIC;
use super::SSTableDesc;

enum ReadEntryResult {
    Invalid,
    Update {
        next_sst_id: u64,
        added: Vec<SSTableDesc>,
        removed: Vec<u64>,
    },
}

pub struct ReadResult {
    pub sstables: BTreeMap<u64, SSTableDesc>,
    pub next_sst_id: u64,
}

pub struct ManifestReader<R>(R)
where
    R: Read + Seek;

impl<R> ManifestReader<R>
where
    R: Read + Seek,
{
    pub fn new(inner: R) -> Self {
        Self(inner)
    }

    pub fn read(mut self) -> Result<ReadResult, io::Error> {
        self.read_validate_header()?;
        self.read_entries()
    }

    fn read_validate_header(&mut self) -> io::Result<()> {
        let magic = self.0.read_u32()?;
        let version = self.0.read_u8()?;

        if magic != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid manifest magic number",
            ));
        }

        if version != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unsupported manifest version: {version}"),
            ));
        }

        Ok(())
    }

    fn read_entries(&mut self) -> io::Result<ReadResult> {
        let mut sstables = BTreeMap::new();
        let mut next_sst_id: u64 = 0;

        loop {
            let entry = self.read_entry();

            match entry {
                Ok(ReadEntryResult::Update {
                    next_sst_id: sst_id_update,
                    added,
                    removed,
                }) => {

                    next_sst_id = sst_id_update;

                    for id in removed {
                        if sstables.remove(&id).is_none() {
                            return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    "invalid remove entry: SST ID doesn't exist."
                            ));
                        }
                    }

                    for sstable in added {
                        if sstables.insert(sstable.id, sstable).is_some() {
                            return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    "invalid add entry: SST ID already exists."
                            ));
                        }

                    }
                }

                Ok(ReadEntryResult::Invalid) => {
                    break;
                }

                Err(e) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        break;
                    }

                    return Err(e);
                }
            }
        }

        Ok(ReadResult {
            sstables,
            next_sst_id
        })
    }

    /// Reads a single entry from the file from the current position.
    fn read_entry(&mut self) -> io::Result<ReadEntryResult> {
        let mut added = Vec::<SSTableDesc>::new();
        let mut removed = Vec::<u64>::new();

        let crc = self.0.read_u32()?;
        let length = self.0.read_u32()?;
        let buf = self.0.read_bytes_with_len(length as usize)?;
        if crc != crc32c(&buf) {
            return Ok(ReadEntryResult::Invalid);
        }

        let mut reader = Cursor::new(buf);

        let next_sst_id = reader.read_u64()?;

        let added_len = reader.read_u64()?;
        added.reserve_exact(added_len as usize);

        for _ in 0..added_len {
            let id = reader.read_u64()?;
            let level = reader.read_u8()?;
            let min_key = reader.read_string()?;
            let max_key = reader.read_string()?;

            added.push(SSTableDesc {
                id,
                level,
                min_key,
                max_key,
            });
        }

        let removed_len = reader.read_u64()?;
        removed.reserve_exact(removed_len as usize);

        for _ in 0..removed_len {
            let id = reader.read_u64()?;
            removed.push(id);
        }

        Ok(ReadEntryResult::Update {
            next_sst_id,
            added,
            removed
        })
    }
}

