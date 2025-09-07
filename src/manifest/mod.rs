/// Manifest file readering and writing routines.
/// Manifest file format is specified in [docs/manifest-file-spec.md](docs/manifest-file-spec.md).
use std::ops::RangeBounds;
use std::{fs, io};
use std::{
    fs::{File, OpenOptions},
    io::{Cursor, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

use fs2::FileExt;

use crate::{
    crc::crc32c,
    io_ext::{ReadExt, WriteExt},
};

const MAGIC: u32 = 0xBEEFFE57;

const TYPE_ADD_SSTABLE: u8 = 1;
const TYPE_REMOVE_SSTABLE: u8 = 2;

pub struct Manifest {
    pub sstables: Vec<SSTable>,
}

pub struct SSTable {
    pub id: u64,
    pub level: u8,
    pub min_key: String,
    pub max_key: String,
}

pub struct AddSSTable {
    sstable: SSTable,
}

pub struct RemoveSSTable {
    id: u64,
}

pub enum Entry {
    AddSSTable(AddSSTable),
    RemoveSSTable(RemoveSSTable),
}

enum ReadResult {
    Entry(Entry),
    Invalid,
}

/// Reader for manifest file.
/// Manifest file format is specified in [docs/manifest-file-spec.md](docs/manifest-file-spec.md).
///
/// This struct is largly "use-once". The functions intentially *consume* the reader here for
/// simplicity. For example, we don't have to rewind/seek the reader to prepare it for another use.
pub struct ManifestReader<R>(R)
where
    R: Read + Seek;

impl<R> ManifestReader<R>
where
    R: Read + Seek,
{
    /// Create a new manifest reader.
    pub fn new(inner: R) -> Self {
        Self(inner)
    }

    /// Determine the SSTables that may contain the given key.
    /// This limits our search space before we actually begin to read SSTables from the disk.
    ///
    /// An SSTable entry has a min key and max key describing the range of keys it contains.
    ///
    /// Note that this does not actually read the SSTables from the disk and only returns
    /// _descriptors/IDs_ of the SSTables which can be used to read the SSTables from the disk
    /// using an [`SSTableReader`](crate::sstable::reader::SSTableReader).
    ///
    /// Example:
    ///
    /// ```ignore
    /// let reader = ManifestReader::new(File::open("manifest").unwrap());
    /// let candidate_sstables: Vec<SSTable> = reader.get_candidate_sstables_for_key("key1").unwrap();
    /// ```
    pub fn get_candidate_sstables_for_key(self, key: &str) -> io::Result<Vec<SSTable>> {
        Ok(self
            .read()?
            .sstables
            .into_iter()
            .filter(|sstable| sstable.min_key.as_str() <= key && sstable.max_key.as_str() >= key)
            .collect())
    }

    pub fn get_candidate_sstables_for_range<Range: RangeBounds<str>>(
        self,
        range: Range,
    ) -> io::Result<Vec<SSTable>> {
        Ok(self
            .read()?
            .sstables
            .into_iter()
            .filter(|sstable| range.contains(&sstable.min_key) || range.contains(&sstable.max_key))
            .collect())
    }

    /// Reads the manifest file.
    ///
    /// Returns a Vec of all SSTable descriptors in the manifest file.
    ///
    /// We continue to read the manifest file ever after an invalid entry is encountered.
    /// This behaviour is useful for recovering from corruption.
    ///
    /// Sometimes it is desirable to read only until the last valid entry. For such times,
    /// use [`read_skip_invalid`](Self::read_skip_invalid) instead.
    ///
    /// Example:
    ///
    /// ```ignore
    /// let reader = ManifestReader::new(File::open("manifest").unwrap());
    /// let manifest: Manifest = reader.read().unwrap();
    /// let sstables: Vec<SSTable> = manifest.sstables;
    ///
    /// assert_eq!(sstables.len(), 2);
    /// ```
    pub fn read(mut self) -> Result<Manifest, io::Error> {
        self.read_validate_header()?;

        let sstables = self.read_sstables(true)?;
        Ok(Manifest { sstables })
    }

    /// Reads the manifest file until a invalid entry is encountered.
    ///
    /// Example:
    ///
    /// ```ignore
    /// let reader = ManifestReader::new(File::open("manifest").unwrap());
    /// let manifest: Manifest = reader.read_skip_invalid().unwrap();
    /// let sstables: Vec<SSTable> = manifest.sstables;
    ///
    /// assert_eq!(sstables.len(), 2);
    /// ```
    fn read_skip_invalid(&mut self) -> Result<Manifest, io::Error> {
        self.read_validate_header()?;

        let sstables = self.read_sstables(false)?;
        Ok(Manifest { sstables })
    }

    /// Reads the manifest file header and returns the next SST ID.
    ///
    /// When the header is invalid, this function returns an error.
    ///
    /// Example:
    ///
    /// ```ignore
    /// let reader = ManifestReader::new(File::open("manifest").unwrap());
    /// let next_sst_id = reader.read_validate_header().unwrap();
    /// ```
    fn read_validate_header(&mut self) -> io::Result<u64> {
        let magic = self.0.read_u32()?;
        let version = self.0.read_u8()?;
        let next_sst_id = self.0.read_u64()?;

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

        Ok(next_sst_id)
    }

    /// Reads the SSTables from the manifest file. Stopping at the first invalid entry
    /// if `stop_at_invalid` is true. Otherwise, it will continue to read the manifest file,
    /// trying to recover from corruption.
    ///
    /// Each entry is prefixed with a CRC32C, this is used to determine if the entry is corrupt.
    /// We try to recover from the corruption by attempting to read until either:
    ///
    /// - We find a valid entry.
    /// - We reach the end of the file.
    ///
    /// Returns a Vec of all SSTable descriptors in the manifest file.
    ///
    /// Example:
    ///
    /// ```ignore
    /// let reader = ManifestReader::new(File::open("manifest").unwrap());
    /// let sstables: Vec<SSTable> = reader.read_sstables(true).unwrap();
    /// ```
    fn read_sstables(&mut self, stop_at_invalid: bool) -> io::Result<Vec<SSTable>> {
        let mut sstables = Vec::<Option<SSTable>>::new();

        loop {
            let entry = self.read_entry();

            match entry {
                Ok(ReadResult::Entry(Entry::AddSSTable(add_sstable))) => {
                    sstables.push(Some(add_sstable.sstable));
                }

                Ok(ReadResult::Entry(Entry::RemoveSSTable(remove_sstable))) => {
                    let index = sstables.iter().position(|sstable| {
                        sstable
                            .as_ref()
                            .map(|sstable| sstable.id == remove_sstable.id)
                            .unwrap_or(false)
                    });

                    if let Some(index) = index {
                        sstables[index] = None;
                    }
                }

                Ok(ReadResult::Invalid) => {
                    if !stop_at_invalid {
                        continue;
                    }

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

        sstables.sort_by(|a, b| {
            a.as_ref()
                .map(|a| (a.level, a.id))
                .cmp(&b.as_ref().map(|b| (b.level, b.id)))
        });

        Ok(sstables.into_iter().flatten().collect())
    }

    /// Reads a single entry from the file from the current position.
    fn read_entry(&mut self) -> io::Result<ReadResult> {
        let crc = self.0.read_u32()?;

        let length = self.0.read_u32()?;
        let buf = self.0.read_bytes_with_len(length as usize)?;

        if crc != crc32c(&buf) {
            return Ok(ReadResult::Invalid);
        }

        let mut reader = Cursor::new(buf);

        let ty = reader.read_u8()?;
        if ty == TYPE_ADD_SSTABLE {
            let level = reader.read_u8()?;
            let min_key = reader.read_string()?;
            let max_key = reader.read_string()?;
            let id = reader.read_u64()?;

            Ok(ReadResult::Entry(Entry::AddSSTable(AddSSTable {
                sstable: SSTable {
                    level,
                    min_key,
                    max_key,
                    id,
                },
            })))
        } else if ty == TYPE_REMOVE_SSTABLE {
            let id = reader.read_u64()?;

            Ok(ReadResult::Entry(Entry::RemoveSSTable(RemoveSSTable {
                id,
            })))
        } else {
            Ok(ReadResult::Invalid)
        }
    }
}

/// Writer for manifest files.
///
/// Not thread-safe.
///
/// Manifest file format is specified in [docs/manifest-file-spec.md](docs/manifest-file-spec.md).
///
/// Example:
///
/// ```ignore
/// let writer = ManifestWriter::open(PathBuf::from("manifest")).unwrap();
/// let mut transaction = writer.transaction();
/// transaction.add_sstable(0, "key1", "key2");
/// transaction.commit().unwrap();
/// ```
///
/// This struct itself does not provide any write functionality. Instead, it provides a [`ManifestTransaction`]
/// which can be used to write entries to the manifest file and atomically commited to the file.
///
/// Since the Transaction borrows the writer mutably, the borrow checker ensures that only one transation is running
/// at a time.
pub struct ManifestWriter {
    file: File,

    lock_path: PathBuf,
    lock: File,
}

impl ManifestWriter {
    /// Opens a manifest file for writing.
    ///
    /// If the file does not exist, it will be created.
    ///
    /// Additionally, creates a lock file to prevent multiple writers from writing to the same file.
    ///
    /// On open, it will compact the manifest file if it already exists.
    pub fn open(path: PathBuf) -> io::Result<ManifestWriter> {
        let lock_path = path.clone().with_extension("lock");

        let lock = File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;

        lock.try_lock_exclusive()?;

        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;

        let pos = file.seek(SeekFrom::End(0)).unwrap();

        let mut writer = ManifestWriter::new(file, lock_path, lock);

        if pos == 0 {
            writer.init();
        } else {
            writer.compact();
        }

        Ok(writer)
    }

    fn new(inner: File, lock_path: PathBuf, lock: File) -> ManifestWriter {
        ManifestWriter {
            file: inner,
            lock_path,
            lock,
        }
    }

    fn init(&mut self) {
        self.file.seek(SeekFrom::Start(0)).unwrap();
        self.file.set_len(0).unwrap();

        // Magic number
        self.file.write_u32(MAGIC).unwrap();

        // Version
        self.file.write_u8(1).unwrap();

        // Next SST file ID
        self.file.write_u64(0).unwrap();
        self.file.sync_all().unwrap();
    }

    fn compact(&mut self) {
        self.file.seek(SeekFrom::Start(0)).unwrap();
        let manifest = ManifestReader::new(&mut self.file)
            .read_skip_invalid()
            .unwrap();

        let mut txn = self.transaction();

        txn.clear();

        for sstable in manifest.sstables {
            txn.write_sstable_with_id(
                sstable.level,
                &sstable.min_key,
                &sstable.max_key,
                sstable.id,
            );
        }

        txn.commit().unwrap();
    }

    ///  Starts a new transaction. Writing to the manifest file is done through this transaction.
    pub fn transaction(&mut self) -> ManifestTransaction {
        ManifestTransaction {
            inner: self,
            write_buf: Vec::new(),
            clear: false,
            next_sst_id: None,
        }
    }
}

impl Drop for ManifestWriter {
    fn drop(&mut self) {
        // There's little we can do if this fails.
        let _ = fs2::FileExt::unlock(&self.lock);
        let _ = fs::remove_file(&self.lock_path);
    }
}

/// A manifest transaction.
///
/// This batches writes in a buffer so that they can be atomically commited to the file at the same time
/// and avoiding partial writes or inconsistent states.
pub struct ManifestTransaction<'a> {
    inner: &'a mut ManifestWriter,
    write_buf: Vec<u8>,
    clear: bool,
    next_sst_id: Option<u64>,
}

impl<'a> ManifestTransaction<'a> {
    /// Commits the transaction to the manifest file.
    ///
    /// All the buffered writes are flushed to the file at the same time.
    ///
    /// Consumes the transaction so that it can't be used anymore.
    ///
    /// Example:
    ///
    /// ```ignore
    /// let writer = ManifestWriter::open(PathBuf::from("manifest")).unwrap();
    /// let mut transaction = writer.transaction();
    /// transaction.add_sstable(0, "key1", "key2");
    /// transaction.commit().unwrap();
    /// ```
    pub fn commit(self) -> io::Result<()> {
        if let Some(next_sst_id) = self.next_sst_id {
            self.inner.file.seek(SeekFrom::Start(5))?;
            self.inner.file.write_u64(next_sst_id)?;
            self.inner.file.seek(SeekFrom::End(0))?;
        }

        if self.clear {
            self.inner.file.seek(SeekFrom::Start(13))?;
            self.inner.file.set_len(13)?;
        }

        self.inner.file.write_all(&self.write_buf)?;
        self.inner.file.sync_data()?;
        drop(self);

        Ok(())
    }

    /// Cleans the manifest file when the transaction is committed.
    ///
    /// Note that is does not clear entries that were previously added in this
    /// transaction.
    fn clear(&mut self) {
        self.clear = true;
    }

    /// Batch a new sstable addition to the manifest file.
    ///
    /// Returns the ID of the added sstable that will be written to the file.
    pub fn add_sstable(&mut self, level: u8, min_key: &str, max_key: &str) -> u64 {
        let id = self.allocate_sstable_id();
        self.write_sstable_with_id(level, min_key, max_key, id);

        id
    }

    fn write_sstable_with_id(&mut self, level: u8, min_key: &str, max_key: &str, id: u64) {
        let mut buf = Vec::new();

        buf.write_u8(TYPE_ADD_SSTABLE).unwrap();
        buf.write_u8(level).unwrap();
        buf.write_string(min_key).unwrap();
        buf.write_string(max_key).unwrap();
        buf.write_u64(id).unwrap();

        let crc = crc32c(&buf);

        self.write_buf.write_u32(crc).unwrap();
        self.write_buf.write_u32(buf.len() as u32).unwrap();
        self.write_buf.write_all(&buf).unwrap();
    }

    fn allocate_sstable_id(&mut self) -> u64 {
        let current = self.next_sst_id;

        let id = if let Some(current) = current {
            current
        } else {
            self.inner.file.seek(SeekFrom::Start(5)).unwrap();
            let id = self.inner.file.read_u64().unwrap();
            self.inner.file.seek(SeekFrom::End(0)).unwrap();

            id
        };

        self.next_sst_id = Some(id + 1);

        id
    }

    /// Batch a sstable removal from the manifest file.
    ///
    /// Example:
    ///
    /// ```ignore
    /// let writer = ManifestWriter::open(PathBuf::from("manifest")).unwrap();
    /// let mut transaction = writer.transaction();
    /// transaction.remove_sstable(0);
    /// transaction.commit().unwrap();
    /// ```
    pub fn remove_sstable(&mut self, id: u64) {
        let mut buf = Vec::new();

        buf.write_u8(TYPE_REMOVE_SSTABLE).unwrap();
        buf.write_u64(id).unwrap();

        let crc = crc32c(&buf);

        self.write_buf.write_u32(crc).unwrap();
        self.write_buf.write_u32(buf.len() as u32).unwrap();
        self.write_buf.write_all(&buf).unwrap();
    }

    pub fn remove_sstables(&mut self, ids: Vec<u64>) {
        for id in ids {
            self.remove_sstable(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_can_be_written_and_read() {
        let filename = "test_manifest_can_be_written_and_read";

        if PathBuf::from(filename).exists() {
            fs::remove_file(filename).unwrap();
        }

        let mut writer = ManifestWriter::open(PathBuf::from(filename)).unwrap();
        let mut transaction = writer.transaction();
        transaction.add_sstable(0, "key1", "key2");
        transaction.commit().unwrap();

        let reader = File::open(filename).unwrap();
        let sstables = ManifestReader::new(reader).read().unwrap();
        assert_eq!(sstables.sstables.len(), 1);
        assert_eq!(sstables.sstables[0].id, 0);
    }

    #[test]
    fn test_manifest_does_not_persist_until_commit() {
        let filename = "test_manifest_does_not_persist_until_commit";

        if PathBuf::from(filename).exists() {
            fs::remove_file(filename).unwrap();
        }

        let mut writer = ManifestWriter::open(PathBuf::from(filename)).unwrap();

        let mut transaction = writer.transaction();
        transaction.add_sstable(0, "key1", "key2");

        let reader = File::open(filename).unwrap();
        let sstables = ManifestReader::new(reader).read().unwrap();
        assert_eq!(sstables.sstables.len(), 0);

        transaction.commit().unwrap();

        let reader = File::open(filename).unwrap();
        let sstables = ManifestReader::new(reader).read().unwrap();
        assert_eq!(sstables.sstables.len(), 1);
        assert_eq!(sstables.sstables[0].id, 0);
    }

    #[test]
    fn test_first_sstable_id_is_0() {
        let filename = "test_first_sstable_id_is_0";

        if PathBuf::from(filename).exists() {
            fs::remove_file(filename).unwrap();
        }

        let mut writer = ManifestWriter::open(PathBuf::from(filename)).unwrap();
        let mut transaction = writer.transaction();
        let id = transaction.add_sstable(0, "key1", "key2");

        assert_eq!(id, 0);

        transaction.commit().unwrap();

        let reader = File::open(filename).unwrap();
        let sstables = ManifestReader::new(reader).read().unwrap();
        assert_eq!(sstables.sstables.len(), 1);
        assert_eq!(sstables.sstables[0].id, 0);
    }

    #[test]
    fn test_manifest_persists_item_removal_on_reopen() {
        let filename = "test_manifest_persists_item_removal_on_reopen";

        if PathBuf::from(filename).exists() {
            fs::remove_file(filename).unwrap();
        }

        let mut writer = ManifestWriter::open(PathBuf::from(filename)).unwrap();
        let mut transaction = writer.transaction();
        let id0 = transaction.add_sstable(0, "key1", "key2");
        let id1 = transaction.add_sstable(0, "key2", "key3");
        transaction.remove_sstable(id0);
        transaction.remove_sstable(id1);
        let id2 = transaction.add_sstable(0, "key3", "key4");
        let id3 = transaction.add_sstable(0, "key4", "key5");
        transaction.commit().unwrap();

        drop(writer);

        let reader = File::open(filename).unwrap();
        let sstables = ManifestReader::new(reader).read().unwrap();
        assert_eq!(sstables.sstables.len(), 2);
        assert_eq!(sstables.sstables[0].id, id2);
        assert_eq!(sstables.sstables[1].id, id3);

        let writer = ManifestWriter::open(PathBuf::from(filename)).unwrap();

        let reader = File::open(filename).unwrap();
        let sstables = ManifestReader::new(reader).read().unwrap();
        assert_eq!(sstables.sstables.len(), 2);
        assert_eq!(sstables.sstables[0].id, id2);
        assert_eq!(sstables.sstables[1].id, id3);

        drop(writer);
    }
}
