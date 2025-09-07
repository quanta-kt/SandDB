use std::collections::BTreeMap;
use std::io;
use std::ops::RangeBounds;
use std::path::PathBuf;

use crate::lsm_tree::LSMTree;
use crate::sstable::reader::{CachedSSTableReader, FsSSTReader};
use crate::store::Store;
use crate::util::{KeyOnlyOrd, merge_sorted_uniq};

const MAX_SIZE: usize = 512;
const MAX_MEMTABLE_SIZE: usize = 64 * 1024; // 64 KiB

pub struct StoreImpl<L: Store> {
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

    fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        if let Some(value) = self.memtable.get(key) {
            return Ok(Some(value.to_owned()));
        }

        self.lsm_tree.get(key)
    }

    fn get_range<'a, R: RangeBounds<str> + Clone + 'a>(
        &'a self,
        range: R,
    ) -> io::Result<impl Iterator<Item = (String, Vec<u8>)> + 'a> {
        let memtable_iter = self
            .memtable
            .range(range.clone())
            .map(|(k, v)| (k.clone(), v.clone()))
            .map(Into::<KeyOnlyOrd>::into);

        let lsm_tree_iter = self
            .lsm_tree
            .get_range(range)?
            .map(Into::<KeyOnlyOrd>::into);

        Ok(merge_sorted_uniq(vec![
            // Since these are entirely different types, we need to box them,
            // monomorphization is not possible. Put them behind a trait object.
            Box::new(memtable_iter) as Box<dyn Iterator<Item = _>>,
            Box::new(lsm_tree_iter) as Box<dyn Iterator<Item = _>>,
        ])
        .map(Into::<(String, Vec<u8>)>::into))
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

pub struct DefaultStore(StoreImpl<LSMTree<CachedSSTableReader<FsSSTReader>>>);

impl Store for DefaultStore {
    fn insert(&mut self, key: &str, value: &[u8]) -> io::Result<()> {
        self.0.insert(key, value)
    }

    fn insert_batch(&mut self, entries: &BTreeMap<String, Vec<u8>>) -> io::Result<()> {
        self.0.insert_batch(entries)
    }

    fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        self.0.get(key)
    }

    fn get_range<'a, R: RangeBounds<str> + Clone + 'a>(
        &'a self,
        range: R,
    ) -> io::Result<impl Iterator<Item = (String, Vec<u8>)> + 'a> {
        self.0.get_range(range)
    }
}

pub fn make_store(directory: PathBuf) -> io::Result<DefaultStore> {
    Ok(DefaultStore(StoreImpl::open(directory)?))
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

    #[test]
    fn test_can_retrieve_range() {
        let dir = PathBuf::from("test_can_retrieve_range");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let mut store = make_store(dir.clone()).unwrap();
        store.insert("foo", "bar".as_bytes()).unwrap();
        store.insert("foo2", "bar2".as_bytes()).unwrap();
        store.insert("foo3", "bar3".as_bytes()).unwrap();

        let iter = store.get_range(..).unwrap();
        let values = iter.collect::<Vec<_>>();

        assert_eq!(
            values,
            vec![
                ("foo".to_owned(), "bar".as_bytes().to_vec()),
                ("foo2".to_owned(), "bar2".as_bytes().to_vec()),
                ("foo3".to_owned(), "bar3".as_bytes().to_vec())
            ]
        );
    }

    #[test]
    fn test_can_retrieve_range_across_memtable_and_lsm_tree() {
        let dir = PathBuf::from("test_can_retrieve_range_across_memtable_and_lsm_tree");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let mut store = make_store(dir.clone()).unwrap();
        store.insert("foo", "bar".as_bytes()).unwrap();
        store.insert("foo2", "bar2".as_bytes()).unwrap();

        // Dropping the store flushes the memtable to the LSM tree
        drop(store);

        let mut store = make_store(dir.clone()).unwrap();

        // These keys should be in the memtable
        store.insert("foo3", "bar3".as_bytes()).unwrap();
        store.insert("foo4", "bar4".as_bytes()).unwrap();

        let actual: Vec<_> = store.get_range(..).unwrap().collect();

        assert_eq!(
            actual,
            vec![
                ("foo".to_owned(), "bar".as_bytes().to_vec()),
                ("foo2".to_owned(), "bar2".as_bytes().to_vec()),
                ("foo3".to_owned(), "bar3".as_bytes().to_vec()),
                ("foo4".to_owned(), "bar4".as_bytes().to_vec()),
            ]
        );
    }

    #[test]
    fn test_can_retrieve_range_across_memtable_and_multiple_sstables() {
        let dir = PathBuf::from("test_can_retrieve_range_across_memtable_and_multiple_sstables");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let mut store = make_store(dir.clone()).unwrap();
        store.insert("sst1:foo0", "bar0".as_bytes()).unwrap();
        store.insert("sst1:foo1", "bar1".as_bytes()).unwrap();
        store.insert("sst1:foo2", "bar2".as_bytes()).unwrap();
        drop(store);

        let mut store = make_store(dir.clone()).unwrap();
        store.insert("sst2:foo0", "bar0".as_bytes()).unwrap();
        store.insert("sst2:foo1", "bar1".as_bytes()).unwrap();
        store.insert("sst2:foo2", "bar2".as_bytes()).unwrap();
        drop(store);

        let mut store = make_store(dir.clone()).unwrap();
        store.insert("sst3:foo0", "bar0".as_bytes()).unwrap();
        store.insert("sst3:foo1", "bar1".as_bytes()).unwrap();
        store.insert("sst3:foo2", "bar2".as_bytes()).unwrap();
        drop(store);

        let mut store = make_store(dir.clone()).unwrap();
        store.insert("memtable:foo0", "bar0".as_bytes()).unwrap();
        store.insert("memtable:foo1", "bar1".as_bytes()).unwrap();
        store.insert("memtable:foo2", "bar2".as_bytes()).unwrap();
        // this should be the last entry
        store.insert("z:memtable:foo2", "bar2".as_bytes()).unwrap();
        drop(store);

        let store = make_store(dir.clone()).unwrap();
        let actual: Vec<_> = store.get_range(..).unwrap().collect();

        assert_eq!(
            actual,
            vec![
                ("memtable:foo0".to_owned(), "bar0".as_bytes().to_vec()),
                ("memtable:foo1".to_owned(), "bar1".as_bytes().to_vec()),
                ("memtable:foo2".to_owned(), "bar2".as_bytes().to_vec()),
                ("sst1:foo0".to_owned(), "bar0".as_bytes().to_vec()),
                ("sst1:foo1".to_owned(), "bar1".as_bytes().to_vec()),
                ("sst1:foo2".to_owned(), "bar2".as_bytes().to_vec()),
                ("sst2:foo0".to_owned(), "bar0".as_bytes().to_vec()),
                ("sst2:foo1".to_owned(), "bar1".as_bytes().to_vec()),
                ("sst2:foo2".to_owned(), "bar2".as_bytes().to_vec()),
                ("sst3:foo0".to_owned(), "bar0".as_bytes().to_vec()),
                ("sst3:foo1".to_owned(), "bar1".as_bytes().to_vec()),
                ("sst3:foo2".to_owned(), "bar2".as_bytes().to_vec()),
                ("z:memtable:foo2".to_owned(), "bar2".as_bytes().to_vec()),
            ]
        );
    }

    #[test]
    fn test_range_reads_memtable_entreies_override_sstable_entreies() {
        let dir = PathBuf::from("test_range_reads_memtable_entreies_override_sstable_entreies");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let mut store = make_store(dir.clone()).unwrap();
        store.insert("foo", b"bar").unwrap();
        drop(store);

        let mut store = make_store(dir.clone()).unwrap();
        store.insert("foo", b"bar2").unwrap();

        let actual: Vec<_> = store.get_range(..).unwrap().collect();

        assert_eq!(actual, vec![("foo".to_string(), b"bar2".to_vec())]);
    }

    #[test]
    fn test_range_reads_drop_duplicates() {
        let dir = PathBuf::from("test_range_reads_drop_duplicates");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let mut store = make_store(dir.clone()).unwrap();
        store.insert("foo0", "wrong".as_bytes()).unwrap();
        store.insert("foo2", "right".as_bytes()).unwrap();
        drop(store);

        let mut store = make_store(dir.clone()).unwrap();
        store.insert("foo0", "wrong2".as_bytes()).unwrap();
        store.insert("foo3", "wrong3".as_bytes()).unwrap();
        drop(store);

        let mut store = make_store(dir.clone()).unwrap();
        store.insert("foo0", "right".as_bytes()).unwrap();
        store.insert("foo4", "right".as_bytes()).unwrap();
        drop(store);

        let mut store = make_store(dir.clone()).unwrap();
        store.insert("foo3", "right".as_bytes()).unwrap();

        let actual: Vec<_> = store.get_range(..).unwrap().collect();

        assert_eq!(
            actual,
            vec![
                ("foo0".to_owned(), "right".as_bytes().to_vec()),
                ("foo2".to_owned(), "right".as_bytes().to_vec()),
                ("foo3".to_owned(), "right".as_bytes().to_vec()),
                ("foo4".to_owned(), "right".as_bytes().to_vec()),
            ]
        );
    }
}
