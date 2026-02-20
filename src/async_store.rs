use std::collections::BTreeMap;
use std::io;
use std::ops::RangeBounds;

use async_trait::async_trait;
use tokio::sync::mpsc::Receiver;

pub type AsyncCursor = Receiver<io::Result<(String, Vec<u8>)>>;

#[async_trait]
pub trait AsyncStore {
    async fn insert(&self, key: &str, value: &[u8]) -> io::Result<()>;

    async fn insert_batch(&self, entries: &BTreeMap<String, Vec<u8>>) -> io::Result<()>;

    async fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>>;

    fn get_range<R>(
        &self,
        range: R,
    ) -> AsyncCursor
        where R: RangeBounds<String> + Send + Clone + 'static;

    async fn flush(&self) -> io::Result<()>;

    async fn shutdown(self);
}
