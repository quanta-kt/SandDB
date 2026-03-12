use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Seek;
use std::io::SeekFrom;
use std::ops::Bound::*;
use std::ops::RangeBounds;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

use fs2::FileExt;
use arc_swap::ArcSwap;

pub mod reader;
pub mod writer;

pub(crate) const MAGIC: u32 = 0xBEEFFE57;

#[derive(Debug, Clone)]
pub struct SSTableDesc {
    pub id: u64,
    pub level: u8,
    pub min_key: String,
    pub max_key: String,
}

pub struct Manifest {
    sstables: ArcSwap<BTreeMap<u64, SSTableDesc>>,
    next_sstable_id: Arc<AtomicU64>,

    file: File,

    _lock_path: PathBuf,
    _lock_file: File,

    writer_lock: Mutex<()>,
}

pub struct ManifestUpdate {
    add: Vec<SSTableDesc>,
    remove: Vec<u64>,
    next_sstable_id: Arc<AtomicU64>,
}

impl ManifestUpdate {
    fn new(next_sstable_id: Arc<AtomicU64>) -> Self {
        Self {
            add: Vec::new(),
            remove: Vec::new(),
            next_sstable_id,
        }
    }

    pub fn add<K1, K2>(&mut self, level: u8, min_key: K1, max_key: K2) -> u64
    where
        K1: AsRef<str>,
        K2: AsRef<str>
    {
        let id = self.next_sstable_id.fetch_add(1, Ordering::Relaxed);

        self.add.push(SSTableDesc {
            id,
            level,
            min_key: min_key.as_ref().to_owned(),
            max_key: max_key.as_ref().to_owned(),
        });

        id
    }

    pub fn remove(&mut self, id: u64) {
        self.remove.push(id);
    }
}

impl Manifest {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let manifest_file_path = path.as_ref().join("manifest");

        let _lock_path = manifest_file_path.with_extension("lock");

        let _lock_file = File::options()
            .create(true)
            .read(true)
            .write(true)
            .open(&_lock_path)?;

        _lock_file.try_lock_exclusive()?;

        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(&manifest_file_path)?;

        // Ensure the file isn't empty and at least the header is written since our reader expects
        // at least the header to be present.
        writer::ManifestWriter::ensure_header(&mut file)?;

        // We are past the header, seek back to read
        file.seek(SeekFrom::Start(0))?;

        let state = reader::ManifestReader::new(&file).read()?;
        // after the, we are at the end of the file, which is what manifest writer expects.

        Ok(Self {
            sstables: ArcSwap::from_pointee(state.sstables),
            next_sstable_id: Arc::new(AtomicU64::new(state.next_sst_id)),

            file,

            _lock_path,
            _lock_file,

            writer_lock: Mutex::new(()),
        })
    }

    #[cfg(test)]
    pub fn get_sstables(&self) -> Vec<SSTableDesc> {
        let mut result: Vec<_> = self
            .sstables
            .load()
            .values()
            .cloned()
            .collect();

        result.sort_unstable_by_key(|it| (it.level, Reverse(it.id)));

        result
    }

    pub fn get_sstables_at_level(&self, level: u8) -> Vec<SSTableDesc> {
        let mut result: Vec<_> = self
            .sstables
            .load()
            .values()
            .filter(|it| it.level == level)
            .cloned()
            .collect();

        result.sort_unstable_by_key(|it| (it.level, Reverse(it.id)));

        result
    }

    pub fn get_candidate_sstables_for_key(&self, key: &str) -> Vec<SSTableDesc> {
        let mut result: Vec<_> = self
            .sstables
            .load()
            .values()
            .filter(|sstable| sstable.min_key.as_str() <= key && sstable.max_key.as_str() >= key)
            .cloned()
            .collect();

        result.sort_unstable_by_key(|it| (it.level, Reverse(it.id)));

        result
    }

    pub fn get_candidate_sstables_for_range<Range: RangeBounds<str>>(
        &self,
        range: Range,
    ) -> Vec<SSTableDesc> {
        let is_empty = match (range.start_bound(), range.end_bound()) {
            (Included(a), Included(b)) => a > b,
            (Included(a), Excluded(b)) => a >= b,
            (Excluded(a), Included(b)) => a >= b,
            (Excluded(a), Excluded(b)) => a >= b,
            _ => false,
        };

        if is_empty {
            return vec![];
        }

        let mut result: Vec<_> = self
            .sstables
            .load()
            .values()
            .filter(|sstable| {
                let min = range.start_bound();
                let min_matches = match min {
                    Included(x) => x <= sstable.max_key.as_str(),
                    Excluded(x) => x < sstable.max_key.as_str(),
                    Unbounded => true,
                };

                let max = range.end_bound();
                let max_matches = match max {
                    Included(x) => x >= sstable.min_key.as_str(),
                    Excluded(x) => x > sstable.min_key.as_str(),
                    Unbounded => true,
                };

                return min_matches && max_matches;
            })
            .cloned()
            .collect();

        result.sort_unstable_by_key(|it| (it.level, Reverse(it.id)));

        result
    }

    pub fn start_update(&self) -> ManifestUpdate {
        ManifestUpdate::new(self.next_sstable_id.clone())
    }

    pub fn update(&self, update: ManifestUpdate) -> io::Result<()> {
        let lock = self.writer_lock.lock().unwrap();

        let mut writer = writer::ManifestWriter::open(self.file.try_clone()?)?;
        writer.write(
            &update.add,
            &update.remove,
            self.next_sstable_id.load(Ordering::Relaxed),
        )?;

        let mut state = (*self.sstables.load_full()).clone();

        for sst in update.add {
            state.insert(sst.id, sst);
        }

        for id in update.remove {
            state.remove(&id);
        }

        self.sstables.store(Arc::new(state));

        drop(writer);
        drop(lock);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::ops::Bound;
    use std::fs;

    #[test]
    fn test_manifest_can_be_written_and_read() {
        let path = PathBuf::from("test_manifest_can_be_written_and_read");
        if path.exists() {
            fs::remove_dir_all(&path).unwrap();
        }

        fs::create_dir(&path).unwrap();

        let manifest = Manifest::open(&path).unwrap();
        let mut update = manifest.start_update();
        update.add(0, "key1", "key2");
        manifest.update(update).unwrap();
        drop(manifest);

        let manifest = Manifest::open(&path).unwrap();
        let sstables = manifest.get_sstables();
        assert_eq!(sstables.len(), 1);
        assert_eq!(sstables[0].id, 0);
    }

    #[test]
    fn test_first_sstable_id_is_0() {
        let path = PathBuf::from("test_first_sstable_id_is_0");
        if path.exists() {
            fs::remove_dir_all(&path).unwrap();
        }

        fs::create_dir(&path).unwrap();

        let manifest = Manifest::open(&path).unwrap();
        let mut update = manifest.start_update();
        update.add(0, "key1", "key2");
        manifest.update(update).unwrap();

        let sstables = manifest.get_sstables();
        assert_eq!(sstables.len(), 1);
        assert_eq!(sstables[0].id, 0);

        drop(manifest);

        let manifest = Manifest::open(&path).unwrap();
        let sstables = manifest.get_sstables();
        assert_eq!(sstables.len(), 1);
        assert_eq!(sstables[0].id, 0);
    }

    #[test]
    fn test_manifest_persists_item_removal_on_reopen() {
        let path = PathBuf::from("test_manifest_persists_item_removal_on_reopen");
        if path.exists() {
            fs::remove_dir_all(&path).unwrap();
        }

        fs::create_dir(&path).unwrap();

        let manifest = Manifest::open(&path).unwrap();

        let mut update = manifest.start_update();
        let id0 = update.add(0, "key1", "key2");
        let id1 = update.add(0, "key2", "key3");
        manifest.update(update).unwrap();

        let mut update = manifest.start_update();
        update.remove(id0);
        update.remove(id1);
        let id2 = update.add(0, "key3", "key4");
        let id3 = update.add(0, "key4", "key5");
        manifest.update(update).unwrap();

        let sstables = manifest.get_sstables();
        assert_eq!(sstables.len(), 2);
        assert_eq!(sstables[0].id, id3);
        assert_eq!(sstables[1].id, id2);

        drop(manifest);

        let manifest = Manifest::open(&path).unwrap();
        let sstables = manifest.get_sstables();
        assert_eq!(sstables.len(), 2);
        assert_eq!(sstables[0].id, id3);
        assert_eq!(sstables[1].id, id2);
    }

    #[test]
    fn test_manifest_returns_candidates_in_range() {
        let path = PathBuf::from("test_manifest_returns_candidates_in_range");
        if path.exists() {
            fs::remove_dir_all(&path).unwrap();
        }

        fs::create_dir(&path).unwrap();

        let mut manifest = Manifest::open(&path).unwrap();
        let mut update = manifest.start_update();
        let id0 = update.add(0, "key10", "key20");
        let id1 = update.add(0, "key20", "key30");
        let id2 = update.add(0, "key25", "key35");
        let id3 = update.add(0, "key00", "key99");
        manifest.update(update).unwrap();


        for _ in 0..2 {
            let get_candidates = |range: (Bound<&str>, Bound<&str>)| {
                manifest.get_candidate_sstables_for_range(range)
                    .iter().
                    map(|it| it.id)
                    .collect::<Vec<_>>()
            };

            // empty
            assert_eq!(get_candidates((Excluded("key15"), Excluded("key15"))), vec![]);
            assert_eq!(get_candidates((Included("key15"), Excluded("key15"))), vec![]);
            assert_eq!(get_candidates((Included("key15"), Excluded("key15"))), vec![]);

            // single
            assert_eq!(get_candidates((Included("key15"), Excluded("key16"))), vec![id3, id0]);
            assert_eq!(get_candidates((Excluded("key15"), Excluded("key17"))), vec![id3, id0]);

            // min unbounded
            assert_eq!(get_candidates((Unbounded, Included("key16"))), vec![id3, id0]);
            assert_eq!(get_candidates((Unbounded, Excluded("key17"))), vec![id3, id0]);

            // max unbounded
            assert_eq!(get_candidates((Included("key30"), Unbounded)), vec![id3, id2, id1]);
            assert_eq!(get_candidates((Excluded("key30"), Unbounded)), vec![id3, id2]);

            // intersection
            assert_eq!(get_candidates((Included("key22"), Excluded("key25"))), vec![id3, id1]);
            assert_eq!(get_candidates((Included("key22"), Included("key25"))), vec![id3, id2, id1]);
            assert_eq!(get_candidates((Excluded("key20"), Included("key25"))), vec![id3, id2, id1]);
            assert_eq!(get_candidates((Included("key20"), Included("key25"))), vec![id3, id2, id1, id0]);

            // another pass of same checks after re-opening manifest
            drop(manifest);
            manifest = Manifest::open(&path).unwrap();
        }
    }
}

