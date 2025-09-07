use std::collections::BTreeMap;
use std::io;
use std::ops::RangeBounds;

pub trait Store {
    fn insert(&mut self, key: &str, value: &[u8]) -> io::Result<()>;

    fn insert_batch(&mut self, entries: &BTreeMap<String, Vec<u8>>) -> io::Result<()>;

    fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>>;

    fn get_range<'a, R: RangeBounds<str> + Clone + 'a>(
        &'a self,
        range: R,
    ) -> io::Result<impl Iterator<Item = (String, Vec<u8>)> + 'a>;
}
