use std::io;
use std::fs::OpenOptions;
use std::fs::File;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;

use crate::Store;
use crate::io_ext::ReadExt;
use crate::io_ext::WriteExt;

/// A store implementaion that WALs to a file on top of another backing store.
pub struct WalStore<S: Store> {
    wal: File,
    inner: S, 
}

impl<S: Store> WalStore<S> {
    pub fn new(store: S, wal_file_path: &Path) -> Self {
        let wal = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(wal_file_path)
            .unwrap_or_else(|_|
                panic!("unable to open WAL file '{:?}'", wal_file_path)
            );

        WalStore {
            inner: store,
            wal
        }
    }

    pub fn restore(&mut self) -> io::Result<()> {
        self.wal.seek(SeekFrom::Start(0))?;

        while let Some(item) = self.read_one()? {
            self.insert(&item.0, &item.1)?;
        }

        self.flush()?;
        // Discard flushed WAL entries
        self.truncate()?;

        Ok(())
    }

    fn read_one(&mut self) -> io::Result<Option<(String, Vec<u8>)>> {
        let pos = self.wal.stream_position()?;
        let len = self.wal.metadata()?.len();
        if pos == len {
            return Ok(None);
        }

        let key = self.wal.read_string()?;
        let value = self.wal.read_bytes()?;

        Ok(Some((key, value)))
    }

    fn write_one(&mut self, key: &str, value: &[u8]) -> io::Result<()> {
        self.wal.write_string(key)?;
        self.wal.write_bytes(value)?;
        self.wal.sync_all()?;
        Ok(())
    }

    fn truncate(&mut self) -> io::Result<()> {
        self.wal.set_len(0)?;
        self.wal.sync_all()?;
        Ok(())
    }
}

impl<S: Store> Store for WalStore<S> {
    fn insert(&mut self, key: &str, value: &[u8]) -> std::io::Result<()> {
        self.write_one(key, value)?;
        self.inner.insert(key, value)
    }

    fn insert_batch(&mut self, entries: &std::collections::BTreeMap<String, Vec<u8>>) -> std::io::Result<()> {
        for entry in entries.iter() {
            self.write_one(&entry.0, &entry.1)?;
        }

        self.inner.insert_batch(entries)
    }

    fn get(&self, key: &str) -> std::io::Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }

    fn get_range<'a, R: std::ops::RangeBounds<str> + Clone + 'a>(
        &'a self,
        range: R,
    ) -> std::io::Result<impl Iterator<Item = (String, Vec<u8>)> + 'a> {
        self.inner.get_range(range)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

