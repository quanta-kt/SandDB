use std::{collections::HashMap, hash::Hash};

use crate::datastructure::doubly_linked_list::{DoublyLinkedList, NodeHandle};

pub struct LruCache<'a, K, V>
where
    K: Eq + Hash + Clone,
{
    map: HashMap<K, (V, NodeHandle<'a, K>)>,
    list: DoublyLinkedList<'a, K>,
    capacity: usize,
}

impl<'a, K, V> LruCache<'a, K, V>
where
    K: Eq + Hash + Clone,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            list: DoublyLinkedList::new(),
            capacity,
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        let entry = self.map.get_mut(key);

        if let Some(entry) = entry {
            let mut node = self.list.push_front(key.clone());

            std::mem::swap(&mut entry.1, &mut node);
            self.list.remove(node);

            Some(&entry.0)
        } else {
            None
        }
    }

    pub fn put(&mut self, key: K, value: V) {
        if self.map.len() >= self.capacity {
            let last = self
                .list
                .tail()
                .expect("BUG(LRU): list is empty while map capacity is exceeded");

            self.map.remove(last.value());
            self.list.remove(last);
        }

        let node = self.list.push_front(key.clone());

        self.map.insert(key, (value, node));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_cache() {
        let mut cache = LruCache::new(2);

        cache.put("foo", "bar");
        cache.put("baz", "qux");

        assert_eq!(cache.get(&"foo"), Some(&"bar"));
        assert_eq!(cache.get(&"baz"), Some(&"qux"));

        cache.put("quux", "corge");

        assert_eq!(cache.get(&"foo"), None);
        assert_eq!(cache.get(&"baz"), Some(&"qux"));
        assert_eq!(cache.get(&"quux"), Some(&"corge"));
    }
}
