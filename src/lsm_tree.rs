use std::{
    collections::BTreeMap,
    fs::{self, File},
    io,
    path::{Path, PathBuf},
};

use fs2::FileExt;

use crate::{
    Store,
    manifest::{Manifest, ManifestReader, ManifestWriter, SSTable},
    sstable::{
        SSTableWriter,
        reader::{CachedSSTableReader, FsSSTReader, SSTableReader},
    },
    util::merge_sorted_uniq,
};

const DB_LOCK_FILENAME: &str = ".lock";

// FIXME: This is very arbitrarily chosen
const COMPACT_EVERY_N_SSTABLES: u8 = 25;

const MAX_LEVEL: u8 = 3;

pub struct LSMTree<S: SSTableReader> {
    directory: PathBuf,
    lock: Option<File>,

    manifest_writer: ManifestWriter,
    sstable_reader: S,

    level_zero_count: u8,
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
            manifest_writer,
            sstable_reader,
            level_zero_count: 0,
        })
    }
}

impl<S: SSTableReader> LSMTree<S> {
    fn manifest_reader(&self) -> ManifestReader<File> {
        let manifest_path = self.directory.join("manifest");
        let manifest_file = File::open(manifest_path).unwrap();
        ManifestReader::new(manifest_file)
    }

    fn read_manifest(&mut self) -> Result<Manifest, io::Error> {
        let manifest = self.manifest_reader().read()?;

        // Each time we read the manifest, we update the level zero count
        self.level_zero_count = manifest.sstables.iter().filter(|it| it.level == 0).count() as u8;

        Ok(manifest)
    }

    pub fn get(&mut self, key: &str) -> io::Result<Option<Vec<u8>>> {
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

    pub fn write_sstable(&mut self, source: &BTreeMap<String, Vec<u8>>) -> io::Result<()> {
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

        let mut txn = self.manifest_writer.transaction();
        let id = txn.add_sstable(0, min_key, max_key);

        SSTableWriter::write_sstable(
            self.directory.clone(),
            id,
            &mut source
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_slice()))
                .peekable(),
        )?;

        txn.commit()?;

        self.level_zero_count += 1;

        Ok(())
    }

    pub fn compact(&mut self) -> io::Result<()> {
        if self.level_zero_count < COMPACT_EVERY_N_SSTABLES {
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

    fn compact_level(&mut self, level: u8) -> io::Result<bool> {
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
            self.level_zero_count = 0;
        }

        Ok(true)
    }

    fn merge_ssts(&mut self, to_merge: Vec<SSTable>, target_level: u8) -> io::Result<()> {
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
                reader.chunk_iterator(table.id).flatten()
            })
            .collect::<Vec<_>>();

        let merged = merge_sorted_uniq(sources);

        let mut txn = self.manifest_writer.transaction();
        txn.remove_sstables(to_merge.iter().map(|it| it.id).collect());

        let sst_id = txn.add_sstable(target_level, min_key, max_key);

        let writer = SSTableWriter::new(File::create(sst_file_path(&self.directory, sst_id))?);

        writer.write(&mut merged.peekable());
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
    fn get(&mut self, key: &str) -> io::Result<Option<Vec<u8>>> {
        LSMTree::get(self, key)
    }

    fn insert(&mut self, key: &str, value: &[u8]) -> io::Result<()> {
        let mut entries = BTreeMap::new();
        entries.insert(key.to_owned(), value.to_owned());
        self.write_sstable(&entries)
    }

    fn insert_batch(&mut self, entries: &BTreeMap<String, Vec<u8>>) -> io::Result<()> {
        self.write_sstable(entries)
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
}
