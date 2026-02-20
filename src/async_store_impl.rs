use std::collections::BTreeMap;
use std::io;
use std::ops::RangeBounds;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::oneshot;
use tokio::sync::mpsc;

use crate::store;
use crate::async_store::AsyncStore;
use crate::async_store::AsyncCursor;

const CHANNEL_BUFFER_SIZE: usize = 255;

enum Message {
    Insert {
        entry: (String, Vec<u8>),
        resp: oneshot::Sender<io::Result<()>>,
    },

    InsertBatch {
        entries: BTreeMap<String, Vec<u8>>,
        resp: oneshot::Sender<io::Result<()>>,
    },

    Flush {
        resp: oneshot::Sender<io::Result<()>>,
    },

    Shutdown,
}

pub struct AsyncStoreImpl<S: store::Store + Sync + Send> {
    store: Arc<S>,
    channel: mpsc::Sender<Message>,
    join_handle: tokio::task::JoinHandle<()>,
}

impl<S: store::Store + Sync + Send + 'static> AsyncStoreImpl<S> {
    pub fn new(store: S) -> Self {
        let (tx, rx) = mpsc::channel(CHANNEL_BUFFER_SIZE);

        let store = Arc::new(store);
        let store_clone = store.clone();

        let join_handle = tokio::spawn(async move {
            run_store(store_clone, rx).await;
        });

        Self {
            store,
            channel: tx,
            join_handle,
        }
    }

}

async fn process_message<S>(store: Arc<S>, message: Message)
where S: store::Store + Sync + Send + 'static {
    match message {
        Message::Insert {
            entry,
            resp,
        } => {
            tokio::task::spawn_blocking(move || {
                let result = store.insert(&entry.0, &entry.1);
                let _ = resp.send(result);
            }).await.unwrap();
        },

        Message::InsertBatch {
            entries,
            resp,
        } => {
            tokio::task::spawn_blocking(move || {
                let result = store.insert_batch(&entries);
                let _ = resp.send(result);
            }).await.unwrap();
        },

        Message::Flush {
            resp
        } => {
            tokio::task::spawn_blocking(move || {
                let result = store.flush();
                let _ = resp.send(result);
            }).await.unwrap();
        },

        Message::Shutdown => {
            unreachable!("this function should never be called with Message::Shutdown");
        },
    }
}

async fn run_store<S>(store: Arc<S>, mut rx: mpsc::Receiver<Message>)
where S: store::Store + Send + Sync + 'static {
    loop {
        match rx.recv().await {
            Some(Message::Shutdown) => {
                return;
            },

            Some(message) => {
                process_message(store.clone(), message).await;
            },

            None => break,
        }
    }
}


#[async_trait]
impl<S: store::Store + Send + Sync + 'static> AsyncStore for AsyncStoreImpl<S> {
    async fn insert(&self, key: &str, value: &[u8]) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();

        self.channel.send(Message::Insert {
            entry: (key.to_owned(), value.to_owned()),
            resp: tx,
        }).await.unwrap();

        rx.await.unwrap()
    }

    async fn insert_batch(&self, entries: &BTreeMap<String, Vec<u8>>) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();

        self.channel.send(Message::InsertBatch {
            entries: entries.to_owned(),
            resp: tx,
        }).await.unwrap();

        rx.await.unwrap()
    }

    async fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        let store = self.store.clone();
        let key = key.to_owned();

        tokio::task::spawn_blocking(move || {
            store.get(&key)
        }).await.unwrap()
    }

    fn get_range<R>(
        &self,
        range: R,
    ) -> AsyncCursor
        where R: RangeBounds<String> + Send + Clone + 'static {

        let (tx, rx) = mpsc::channel::<io::Result<(String, Vec<u8>)>>(255);

        let store = self.store.clone();

        tokio::task::spawn_blocking(move || {
            let range = (
                range.start_bound().map(String::as_str),
                range.end_bound().map(String::as_str),
            );

            let cursor = match store.get_range(range) {
                Ok(cursor) => cursor,
                Err(e) => {
                    let _ = tx.send(Err(e));
                    return;
                }
            };

            for item in cursor {
                eprintln!("sending {:?}", item);

                tx.blocking_send(item).unwrap();
            }
        });

        rx
    }

    async fn flush(&self) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();

        self.channel.send(Message::Flush {
            resp: tx,
        }).await.unwrap();

        rx.await.unwrap()
    }

    async fn shutdown(self) {
        if let Err(_) = self.channel.send(Message::Shutdown).await {
            self.join_handle.abort();
        }

        let _ = self.join_handle.await;
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::path;

    use crate::make_store;

    #[tokio::test]
    async fn test_can_read_write_async() {
        let path: path::PathBuf = "test_can_read_write_async".into();
        if path.exists() {
            std::fs::remove_dir_all(path.clone()).unwrap();
        }

        let store = make_store(path).unwrap().to_async();
        store.insert("hi", b"hello").await.unwrap();

        let value = store.get("hi").await.unwrap();
        assert_eq!(value, Some(b"hello".to_vec()));
    }

    #[tokio::test]
    async fn test_can_iter_async() {
        let path: path::PathBuf = "test_can_iter_async".into();
        if path.exists() {
            std::fs::remove_dir_all(path.clone()).unwrap();
        }

        let store = make_store(path).unwrap().to_async();
        for i in 0..1024 {
            store
                .insert(
                    &format!("key_{:04}", i),
                    &format!("value_{:04}", i).bytes().collect::<Vec<u8>>()
                )
                .await
                .unwrap();
        }

        let mut actual = Vec::with_capacity(900);

        let mut stream = store.get_range("key_0100".to_string().."key_1000".to_string());
        while let Some(item) = stream.recv().await {
            actual.push(item.unwrap());
        }

        let expected: Vec<_> = (100..1000)
            .map(|i|
                (format!("key_{:04}", i), format!("value_{:04}", i)
                    .bytes()
                    .collect::<Vec<_>>()))
            .collect();

        assert_eq!(actual, expected);
    }
}

