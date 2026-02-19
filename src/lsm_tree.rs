use std::{
    collections::BTreeMap,
    fs::{self, File},
    io,
    ops::RangeBounds,
    path::{Path, PathBuf},
};
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Mutex;

use fs2::FileExt;

use crate::{
    Store,
    manifest::{Manifest, ManifestReader, ManifestWriter, SSTable},
    sstable::reader::{CachedSSTableReader, FsSSTReader, SSTableReader},
    util::{KeyOnlyOrd, merge_sorted_uniq},
};
use crate::sstable::writer::SSTableWriter;

const DB_LOCK_FILENAME: &str = ".lock";

// FIXME: This is very arbitrarily chosen
const COMPACT_EVERY_N_SSTABLES: u8 = 25;

const MAX_LEVEL: u8 = 3;

pub struct LSMTree<S: SSTableReader> {
    directory: PathBuf,
    lock: Option<File>,

    manifest_writer: Mutex<ManifestWriter>,
    sstable_reader: S,

    // Number of level-0 SSTables.
    // This is updated everytime we read manifest and
    // may be 0 if we haven't read it yet.
    level_zero_count: AtomicU8,
}

fn sst_file_path(directory: &Path, id: u64) -> PathBuf {
    directory.join(format!("sstable_{id:016}.sst"))
}

impl LSMTree<CachedSSTableReader<FsSSTReader>> {
    pub fn new(directory: PathBuf) -> io::Result<Self> {
        if !directory.exists() {
            fs::create_dir_all(&directory)?;
        }

        let lock = File::options()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(directory.join(DB_LOCK_FILENAME))?;

        lock.try_lock_exclusive()?;

        let manifest_writer = ManifestWriter::open(directory.join("manifest"))?;
        let sstable_reader = FsSSTReader::new(directory.clone()).cached();

        Ok(Self {
            directory,
            lock: Some(lock),
            manifest_writer: Mutex::new(manifest_writer),
            sstable_reader,
            level_zero_count: AtomicU8::new(0),
        })
    }
}

impl<S: SSTableReader> LSMTree<S> {
    fn manifest_reader(&self) -> ManifestReader<File> {
        let manifest_path = self.directory.join("manifest");
        let manifest_file = File::open(manifest_path).unwrap();
        ManifestReader::new(manifest_file)
    }

    fn read_manifest(&self) -> Result<Manifest, io::Error> {
        let manifest = self.manifest_reader().read()?;

        // Each time we read the manifest, we update the level zero count
        self.level_zero_count.store(
            manifest.sstables.iter().filter(|it| it.level == 0).count() as u8, 
            Ordering::Relaxed
        );

        Ok(manifest)
    }

    pub fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        let candidate_ssts = self.manifest_reader().get_candidate_sstables_for_key(key)?;

        for candidate in candidate_ssts.iter().rev() {
            let candidate_chunks = self
                .sstable_reader
                .get_candidate_chunks_for_key(candidate.id, key);

            for chunk in candidate_chunks {
                let chunk_data = self.sstable_reader.read_chunk(candidate.id, chunk.index);

                if let Some(chunk_data) = chunk_data {
                    if let Ok(value) = chunk_data.binary_search_by_key(&key, |(k, _)| k) {
                        return Ok(Some(chunk_data[value].1.clone()));
                    }
                }
            }
        }

        Ok(None)
    }

    pub fn get_range<'a, R: RangeBounds<str> + Clone + 'a>(
        &'a self,
        range: R,
    ) -> io::Result<impl Iterator<Item = (String, Vec<u8>)> + 'a> {
        let mut candidate_ssts = self
            .manifest_reader()
            .get_candidate_sstables_for_range(range.clone())?;

        // Since manifest is ordered oldest to most recent, we need to reverse the list
        // to pick the most recent entries during the merge.
        candidate_ssts.reverse();

        let iter = candidate_ssts
            .into_iter()
            .map(move |candidate| {
                let candidate_id = candidate.id;

                let candidate_chunks: Vec<_> = self
                    .sstable_reader
                    .get_candidate_chunks_for_range(candidate_id, range.clone())
                    .into_iter()
                    .collect();

                let range = range.clone();

                candidate_chunks
                    .into_iter()
                    .flat_map(move |chunk| {
                        let range = range.clone();

                        // FIXME: Evaluate if it should be OK to cache range queries.
                        // especially when they are large. I suspect this could pollute
                        // the cache with pages that might never be used again.
                        self.sstable_reader
                            .read_chunk(candidate_id, chunk.index)
                            .into_iter()
                            .flatten()
                            .filter(move |(key, _)| range.contains(key.as_str()))
                    })
                    .map(Into::<KeyOnlyOrd>::into)
            })
            .collect::<Vec<_>>();

        Ok(merge_sorted_uniq(iter).map(Into::<(String, Vec<u8>)>::into))
    }

    pub fn write_sstable(&self, source: &BTreeMap<String, Vec<u8>>) -> io::Result<()> {
        self.compact()?;

        let max_key = source
            .iter()
            .next_back()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Source is empty"))?
            .0;

        let min_key = source
            .iter()
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Source is empty"))?
            .0;

        let mut writer = self.manifest_writer.lock().unwrap();
        let mut txn = writer.transaction();
        let id = txn.add_sstable(0, min_key, max_key);

        let mut writer = SSTableWriter::open(&self.directory, id)?;
        for (key, value) in source.iter() {
            writer.write(key, value)?;
        }
        writer.finalize()?;

        txn.commit()?;

        self.level_zero_count.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    pub fn compact(&self) -> io::Result<()> {
        let diff = COMPACT_EVERY_N_SSTABLES.saturating_sub(self.level_zero_count.load(Ordering::Relaxed));

        // level_zero_count == 0 might mean we haven't read the count off manifest yet.
        // We therefore proceed with compaction in that case.
        // If compaction will cancel itself if there are actually no level 0 SSTs.
        if (1..COMPACT_EVERY_N_SSTABLES - 1).contains(&diff) {
            return Ok(());
        }

        let mut level = 0;

        loop {
            let compacted = self.compact_level(level)?;
            if !compacted || level == MAX_LEVEL {
                return Ok(());
            }

            level += 1;
        }
    }

    fn compact_level(&self, level: u8) -> io::Result<bool> {
        let to_compact = self
            .read_manifest()?
            .sstables
            .into_iter()
            .filter(|it| it.level == level)
            .collect::<Vec<_>>();

        if to_compact.len() < COMPACT_EVERY_N_SSTABLES as usize {
            return Ok(false);
        }

        let target_level = std::cmp::min(level + 1, MAX_LEVEL);
        self.merge_ssts(to_compact, target_level)?;

        if level == 0 {
            // We've compacted all level zero sstables, so we reset the count
            self.level_zero_count.store(0, Ordering::Relaxed);
        }

        Ok(true)
    }

    fn merge_ssts(&self, mut to_merge: Vec<SSTable>, target_level: u8) -> io::Result<()> {
        // Since we want to keep the most recent value for a key, we need to reverse the list
        // to pick the most recent value for a key as manifest is ordered oldest to most recent.
        // See [`util::merge_sorted_uniq`].
        to_merge.reverse();

        let min_key = to_merge
            .iter()
            .map(|it| it.min_key.as_str())
            .min()
            // SAFETY: we know that there are at least 3 sstables
            .unwrap();

        let max_key = to_merge
            .iter()
            .map(|it| it.max_key.as_str())
            .max()
            // SAFETY: we know that there are at least 3 sstables
            .unwrap();

        let sources = to_merge
            .iter()
            .map(|table| {
                let reader = FsSSTReader::new(self.directory.clone());
                reader
                    .chunk_iterator(table.id)
                    .flatten()
                    // Order and de-dup based only on the key
                    .map(Into::<KeyOnlyOrd>::into)
            })
            .collect::<Vec<_>>();

        let merged = merge_sorted_uniq(sources).map(Into::<(String, Vec<u8>)>::into);

        let mut writer = self.manifest_writer.lock().unwrap();
        let mut txn = writer.transaction();
        txn.remove_sstables(to_merge.iter().map(|it| it.id).collect());

        let sst_id = txn.add_sstable(target_level, min_key, max_key);

        let mut writer = SSTableWriter::open(&self.directory, sst_id)?;
        for (key, value) in merged {
            writer.write(key, value)?;
        }
        writer.finalize()?;

        txn.commit()?;

        for table in to_merge.iter() {
            let path = sst_file_path(&self.directory, table.id);
            if let Err(e) = fs::remove_file(path) {
                eprintln!("Error removing sstable: {e}");
            }
        }

        Ok(())
    }
}

impl<S: SSTableReader> Drop for LSMTree<S> {
    fn drop(&mut self) {
        if let Some(lock) = self.lock.take() {
            let _ = fs2::FileExt::unlock(&lock);
            drop(lock);

            let res = fs::remove_file(self.directory.join(DB_LOCK_FILENAME));

            if let Err(e) = res {
                eprintln!("Error removing lock file: {e}");
            }
        }
    }
}

impl<S: SSTableReader> Store for LSMTree<S> {
    fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        LSMTree::get(self, key)
    }

    fn get_range<'a, R: RangeBounds<str> + Clone + 'a>(
        &'a self,
        range: R,
    ) -> io::Result<impl Iterator<Item = (String, Vec<u8>)> + 'a> {
        LSMTree::get_range(self, range)
    }

    fn insert(&self, key: &str, value: &[u8]) -> io::Result<()> {
        let mut entries = BTreeMap::new();
        entries.insert(key.to_owned(), value.to_owned());
        self.write_sstable(&entries)
    }

    fn insert_batch(&self, entries: &BTreeMap<String, Vec<u8>>) -> io::Result<()> {
        self.write_sstable(entries)
    }

    fn flush(&self) -> io::Result<()> {
        // This store always writes to disk on insert, so we don't really need to flush here.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_writing_n_sstables_compacts() {
        let filename = "test_writing_n_sstables_compacts";

        if PathBuf::from(filename).exists() {
            fs::remove_dir_all(filename).unwrap();
        }

        let mut tree = LSMTree::new(PathBuf::from(filename)).unwrap();

        for i in 0..(COMPACT_EVERY_N_SSTABLES * 2) + 1 {
            tree.write_sstable(&BTreeMap::from([(
                format!("key{}", i),
                format!("value{}", i).as_bytes().to_vec(),
            )]))
            .unwrap();
        }

        let manifest_reader =
            ManifestReader::new(File::open(PathBuf::from(filename).join("manifest")).unwrap());
        let sstables = manifest_reader.read().unwrap();

        // group by levels
        let mut levels = BTreeMap::new();

        for sstable in sstables.sstables {
            levels
                .entry(sstable.level)
                .or_insert(Vec::new())
                .push(sstable);
        }

        assert!(levels.len() <= MAX_LEVEL as usize);

        for level in 0..=MAX_LEVEL {
            let sstables = levels.get(&level);

            if let Some(sstables) = sstables {
                assert!(sstables.len() <= COMPACT_EVERY_N_SSTABLES as usize);
            }
        }
    }

    #[test]
    fn test_writing_n_sstables_across_runs_compacts() {
        let filename = "test_writing_n_sstables_across_runs_compacts";

        if PathBuf::from(filename).exists() {
            fs::remove_dir_all(filename).unwrap();
        }

        for i in 0..COMPACT_EVERY_N_SSTABLES + 1 {
            for j in 0..COMPACT_EVERY_N_SSTABLES + 1 {
                let tree = LSMTree::new(PathBuf::from(filename)).unwrap();

                tree.write_sstable(&BTreeMap::from([(
                    format!("key_{}_{}", i, j),
                    format!("value_{}_{}", i, j).as_bytes().to_vec(),
                )]))
                .unwrap();
            }

            let manifest_reader =
                ManifestReader::new(File::open(PathBuf::from(filename).join("manifest")).unwrap());
            let sstables = manifest_reader.read().unwrap();

            // group by levels
            let mut levels = BTreeMap::new();

            for sstable in sstables.sstables {
                levels
                    .entry(sstable.level)
                    .or_insert(Vec::new())
                    .push(sstable);
            }

            assert!(levels.len() <= MAX_LEVEL as usize);

            for level in 0..=MAX_LEVEL {
                let sstables = levels.get(&level);

                if let Some(sstables) = sstables {
                    assert!(sstables.len() <= COMPACT_EVERY_N_SSTABLES as usize);
                }
            }
        }
    }

    #[test]
    fn test_sst_merge() {
        let path = PathBuf::from("test_sst_merge");
        let _ = fs::remove_dir_all(path.clone());
        fs::create_dir_all(path.clone()).unwrap();

        let mut tree = LSMTree::new(path.clone()).unwrap();

        tree.write_sstable(&BTreeMap::from([
            ("key1".to_string(), "value1".as_bytes().to_vec()),
            ("key2".to_string(), "value2".as_bytes().to_vec()),
            ("key3".to_string(), "value3".as_bytes().to_vec()),
        ]))
        .unwrap();

        tree.write_sstable(&BTreeMap::from([
            ("key2".to_string(), "value2-new".as_bytes().to_vec()),
            ("key3".to_string(), "value3-new".as_bytes().to_vec()),
        ]))
        .unwrap();

        let ssts = tree.manifest_reader().read().unwrap();

        tree.merge_ssts(ssts.sstables, 1).unwrap();

        // We expect these SSTables to be merged into a single SSTable at level 1, with ID 2

        // Verify manifest
        let manifest_reader = ManifestReader::new(File::open(path.join("manifest")).unwrap());
        let sstables = manifest_reader.read().unwrap();
        assert_eq!(sstables.sstables.len(), 1);
        assert_eq!(sstables.sstables[0].id, 2);
        assert_eq!(sstables.sstables[0].level, 1);
        assert_eq!(sstables.sstables[0].min_key, "key1");

        // Verify SSTable
        let sstable_reader = FsSSTReader::new(path.clone());
        let sstable = sstable_reader.read_chunk(2, 0).unwrap();
        assert_eq!(sstable.len(), 3);
        assert_eq!(sstable[0].0, "key1");
        assert_eq!(sstable[0].1, "value1".as_bytes().to_vec());
        assert_eq!(sstable[1].0, "key2");
        assert_eq!(sstable[1].1, "value2-new".as_bytes().to_vec());
        assert_eq!(sstable[2].0, "key3");
        assert_eq!(sstable[2].1, "value3-new".as_bytes().to_vec());
    }
}
