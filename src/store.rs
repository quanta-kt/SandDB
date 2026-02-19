use std::collections::BTreeMap;
use std::io;
use std::ops::RangeBounds;

pub trait Cursor: Iterator<Item = io::Result<(String, Vec<u8>)>> {}
impl<I: Iterator<Item = io::Result<(String, Vec<u8>)>>> Cursor for I {}

pub trait Store {
    fn insert(&self, key: &str, value: &[u8]) -> io::Result<()>;

    fn insert_batch(&self, entries: &BTreeMap<String, Vec<u8>>) -> io::Result<()>;

    fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>>;

    fn get_range<'a, R: RangeBounds<str> + Clone + 'a>(
        &'a self,
        range: R,
    ) -> io::Result<impl Cursor + 'a>;

    fn flush(&self) -> io::Result<()>;
}
