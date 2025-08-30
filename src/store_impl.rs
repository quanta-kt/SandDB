use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;

use crate::lsm_tree::LSMTree;
use crate::sstable::reader::{CachedSSTableReader, FsSSTReader};
use crate::store::Store;

const MAX_SIZE: usize = 512;
const MAX_MEMTABLE_SIZE: usize = 64 * 1024; // 64 KiB

struct StoreImpl<L: Store> {
    memtable_size: usize,
    memtable: BTreeMap<String, Vec<u8>>,
    lsm_tree: L,
}

impl StoreImpl<LSMTree<CachedSSTableReader<FsSSTReader>>> {
    pub fn open(
        directory: PathBuf,
    ) -> io::Result<StoreImpl<LSMTree<CachedSSTableReader<FsSSTReader>>>> {
        let lsm_tree = LSMTree::new(directory)?;
        StoreImpl::new(lsm_tree)
    }
}

impl<L: Store> StoreImpl<L> {
    fn new(lsm_tree: L) -> io::Result<StoreImpl<L>> {
        Ok(StoreImpl {
            memtable_size: 0,
            memtable: BTreeMap::new(),
            lsm_tree,
        })
    }

    fn flush_memtable(&mut self) -> io::Result<()> {
        self.lsm_tree.insert_batch(&self.memtable)?;
        self.memtable.clear();

        Ok(())
    }
}

impl<L: Store> Store for StoreImpl<L> {
    fn insert(&mut self, key: &str, value: &[u8]) -> io::Result<()> {
        if key.len() > MAX_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Key too long"));
        }

        if value.len() > MAX_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Value too long",
            ));
        }

        self.memtable_size += key.len() + value.len();

        if self.memtable_size > MAX_MEMTABLE_SIZE {
            self.flush_memtable()?;
            self.memtable_size = key.len() + value.len();
        }

        self.memtable.insert(key.to_owned(), value.to_owned());

        Ok(())
    }

    fn insert_batch(&mut self, entries: &BTreeMap<String, Vec<u8>>) -> io::Result<()> {
        for (key, value) in entries.iter() {
            self.insert(key, value)?;
        }

        Ok(())
    }

    fn get(&mut self, key: &str) -> io::Result<Option<Vec<u8>>> {
        if let Some(value) = self.memtable.get(key) {
            return Ok(Some(value.to_owned()));
        }

        self.lsm_tree.get(key)
    }
}

impl<L: Store> Drop for StoreImpl<L> {
    fn drop(&mut self) {
        if self.memtable.is_empty() {
            return;
        }

        if let Err(e) = self.flush_memtable() {
            eprintln!("Error flushing memtable on drop: {e}");
        }
    }
}

pub fn make_store(directory: PathBuf) -> io::Result<impl Store> {
    StoreImpl::open(directory)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn test_inserted_entries_can_be_retrieved() {
        let dir = PathBuf::from("test_inserted_entries_can_be_retrieved");
        if dir.exists() {
            fs::remove_dir_all(&dir).unwrap();
        }

        fs::create_dir_all(&dir).unwrap();

        let mut store = make_store(dir).unwrap();

        let actual_value = vec![0, 1, 2];

        store.insert("hello", actual_value.as_slice()).unwrap();

        let value = store.get("hello").unwrap();

        assert_eq!(value, Some(actual_value));
    }

    #[test]
    fn test_inserted_entries_can_be_retrieved_on_reopen() {
        let dir = PathBuf::from("test_inserted_entries_can_be_retrieved_on_reopen");
        if dir.exists() {
            fs::remove_dir_all(&dir).unwrap();
        }

        fs::create_dir_all(&dir).unwrap();

        let mut store = make_store(dir.clone()).unwrap();

        store.insert("hello", "world".as_bytes()).unwrap();
        drop(store);

        let mut store = make_store(dir.clone()).unwrap();
        let value = store.get("hello").unwrap();
        assert_eq!(value, Some("world".as_bytes().to_vec()));
    }

    #[test]
    fn test_1000_entries_can_be_inserted_and_retrived_on_reopen() {
        let dir = PathBuf::from("test_1000_entries_can_be_inserted_and_retrived_on_reopen");
        if dir.exists() {
            fs::remove_dir_all(&dir).unwrap();
        }

        fs::create_dir_all(&dir).unwrap();

        let mut store = make_store(dir.clone()).unwrap();

        for i in 0..1000 {
            store
                .insert(
                    &format!("key_{:04}", i),
                    &format!("value_{:04}", i).as_bytes(),
                )
                .unwrap();
        }

        drop(store);

        let mut store = make_store(dir.clone()).unwrap();
        for i in 0..1000 {
            let value = store.get(&format!("key_{:04}", i)).unwrap();
            assert_eq!(value, Some(format!("value_{:04}", i).as_bytes().to_vec()));
        }
    }

    #[test]
    fn test_5000_entries_can_be_inserted_and_retrived_on_reopen() {
        let dir = PathBuf::from("test_5000_entries_can_be_inserted_and_retrived_on_reopen");
        if dir.exists() {
            fs::remove_dir_all(&dir).unwrap();
        }

        fs::create_dir_all(&dir).unwrap();

        let mut store = make_store(dir.clone()).unwrap();

        for i in 0..5000 {
            store
                .insert(
                    &format!("key_{:04}", i),
                    &format!("value_{:04}", i).as_bytes(),
                )
                .unwrap();
        }

        drop(store);

        let mut store = make_store(dir.clone()).unwrap();
        for i in 0..5000 {
            let value = store.get(&format!("key_{:04}", i)).unwrap();
            assert_eq!(value, Some(format!("value_{:04}", i).as_bytes().to_vec()));
        }
    }

    #[test]
    fn test_memtable_flushes_after_max_size() {
        let dir = PathBuf::from("test_memtable_flushes_after_max_size");
        if dir.exists() {
            fs::remove_dir_all(&dir).unwrap();
        }

        fs::create_dir_all(&dir).unwrap();

        let mut store = make_store(dir.clone()).unwrap();

        let file_count = fs::read_dir(&dir).unwrap().count();

        let key_len = "a_long_long_long_key_0000".len();
        let value_len = "a_long_long_long_value_0000".len();

        let n_items = (MAX_MEMTABLE_SIZE / (key_len + value_len)) + 1;

        for i in 0..n_items {
            store
                .insert(
                    &format!("a_long_long_long_key_{:04}", i),
                    &format!("a_long_long_long_value_{:04}", i).as_bytes(),
                )
                .unwrap();
        }

        assert_eq!(fs::read_dir(&dir).unwrap().count(), file_count + 1);

        drop(store);
    }

    #[test]
    fn test_only_one_process_can_open_the_store() {
        let dir = PathBuf::from("test_only_one_process_can_open_the_store");
        if dir.exists() {
            fs::remove_dir_all(&dir).unwrap();
        }

        fs::create_dir_all(&dir).unwrap();

        let store = make_store(dir.clone()).unwrap();
        let store2 = make_store(dir.clone());
        let store3 = make_store(dir.clone());

        assert!(store2.is_err());
        assert!(store3.is_err());

        drop(store);
    }

    #[test]
    fn test_last_inserted_entries_are_not_lost_on_reopen() {
        let dir = PathBuf::from("test_last_inserted_entries_are_not_lost_on_reopen");
        if dir.exists() {
            fs::remove_dir_all(&dir).unwrap();
        }

        fs::create_dir_all(&dir).unwrap();

        let mut store = make_store(dir.clone()).unwrap();
        store.insert("foo", "bar".as_bytes()).unwrap();
        drop(store);

        let mut store = make_store(dir.clone()).unwrap();
        store.insert("foo", "baz".as_bytes()).unwrap();
        drop(store);

        let mut store = make_store(dir.clone()).unwrap();
        let value = store.get("foo").unwrap();
        assert!(value.is_some());
        assert_eq!(value, Some(b"baz".to_vec()));
    }
}
