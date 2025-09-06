//! A simple LRU cache implementation.

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::hash::Hash;
use std::{borrow::Borrow, hash::Hasher};

use crate::datastructure::slotmap::{NodeHandle, SlotMap};

struct KeyRef<K>
where
    K: Eq + Hash + ?Sized,
{
    key: *const K,
}

impl<K> KeyRef<K>
where
    K: Eq + Hash + ?Sized,
{
    fn from_ref(key: &K) -> Self {
        Self {
            key: key as *const K,
        }
    }
}

impl<K> PartialEq for KeyRef<K>
where
    K: Eq + Hash + ?Sized,
{
    fn eq(&self, other: &Self) -> bool {
        unsafe { *self.key == *other.key }
    }
}

impl<K> Eq for KeyRef<K> where K: Eq + Hash + ?Sized {}

impl<K> Hash for KeyRef<K>
where
    K: Eq + Hash + ?Sized,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        unsafe { &*self.key }.hash(state);
    }
}

/// A wrapper to allow a blanket implmentation of Borrow without conflicting with the stdlib blanket impl
/// of Borrow.
#[repr(transparent)]
#[allow(unused)] // Since this is transparent, we don't contruct it directly but cast from K to Self
struct KeyWrapper<K: ?Sized>(K);

impl<K> KeyWrapper<K>
where
    K: ?Sized,
{
    fn from_ref(key: &K) -> &Self {
        // Safety: KeyWrapper is repr(transparent) so its OK to cast from K to Self
        unsafe { &*(key as *const K as *const Self) }
    }
}

impl<K, Q> Borrow<KeyWrapper<Q>> for KeyRef<K>
where
    K: Borrow<Q> + Eq + Hash + ?Sized,
    Q: ?Sized,
{
    fn borrow(&self) -> &KeyWrapper<Q> {
        KeyWrapper::from_ref(unsafe { &*self.key }.borrow())
    }
}

impl<K> PartialEq for KeyWrapper<K>
where
    K: Eq + Hash + ?Sized,
{
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl<K> Eq for KeyWrapper<K> where K: Eq + Hash + ?Sized {}

impl<K> Hash for KeyWrapper<K>
where
    K: Eq + Hash + ?Sized,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

/// A simple LRU cache implementation.
///
/// # Example
/// ```ignore
/// let mut cache = LruCache::new(2);
///
/// cache.put("foo", "bar");
/// cache.put("baz", "qux");
///
/// assert_eq!(cache.get(&"foo"), Some(&"bar"));
/// assert_eq!(cache.get(&"baz"), Some(&"qux"));
///
/// cache.put("quux", "corge");
///
/// assert_eq!(cache.get(&"foo"), None);
/// assert_eq!(cache.get(&"baz"), Some(&"qux"));
/// assert_eq!(cache.get(&"quux"), Some(&"corge"));
/// ```
pub struct LruCache<K, V>
where
    K: Eq + Hash,
{
    // To be able to mutably borrow these without a mutable reference to self we use
    // UnsafeCell.
    //
    // User should not have to take a mutable reference to self read from the cache using
    // Self::get. But we do need to mutably borrow these without to move the read values
    // to the front of the list.
    map: UnsafeCell<HashMap<KeyRef<K>, (V, NodeHandle<K>)>>,
    list: UnsafeCell<SlotMap<K>>,

    capacity: usize,
}

impl<K, V> LruCache<K, V>
where
    K: Eq + Hash,
{
    /// Creates a new `LruCache` with the given capacity.
    /// The cache will evict the least recently used key when the capacity is exceeded.
    pub fn new(capacity: usize) -> Self {
        Self {
            map: UnsafeCell::new(HashMap::with_capacity(capacity)),
            list: UnsafeCell::new(SlotMap::new_with_capacity(capacity)),
            capacity,
        }
    }
}

impl<K, V> LruCache<K, V>
where
    K: Eq + Hash,
{
    /// Returns a refence to the value of the key if it exists in the cache or None
    /// otherwise.
    ///
    /// If the key is found, it is moved to the front of the LRU list.
    ///
    /// # Example
    /// ```ignore
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.put("foo", "bar");
    ///
    /// assert_eq!(cache.get(&"foo"), Some(&"bar"));
    /// assert_eq!(cache.get(&"baz"), None);
    /// ```
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        Q: Hash + Eq + ?Sized,
        K: Borrow<Q>,
    {
        // Safety: we are sure this is OK becuase we only have one mutable reference
        // to the map and list at a time.
        let map = unsafe { self.map_mut() };
        let list = unsafe { self.list_mut() };

        let key = KeyWrapper::from_ref(key);

        let (value, node) = map.get_mut(key)?;

        let new_node = list
            .move_to_front(*node)
            .expect("BUG(LRU): node existed in the hashmap but not present in the list.");

        *node = new_node;

        Some(value)
    }

    /// Puts a new key-value pair into the cache.
    /// The value is inserted at the front of the LRU list.
    ///
    /// If the key already exists in the cache, the value is updated and the
    /// key is moved to the front of the LRU list.
    ///
    /// If the cache is at capacity and the key is not already present, the least
    /// recently used key is removed.
    pub fn put(&mut self, key: K, value: V) {
        // Safety: we are sure this is OK becuase we only have one mutable reference
        // to the map and list at a time.
        let map = unsafe { self.map_mut() };
        let list = unsafe { self.list_mut() };

        let key_wrapper = KeyWrapper::from_ref(&key);

        if let Some((old_value, node)) = map.get_mut(key_wrapper) {
            let new_node = list
                .move_to_front(*node)
                .expect("BUG(LRU): node existed in the hashmap but not present in the list.");
            *node = new_node;
            *old_value = value;

            return;
        }

        // Make room for the new key by removing the least recently used key
        // if we are at capacity.
        if map.len() >= self.capacity {
            let last = list
                .pop_back()
                .expect("BUG(LRU): list is empty while map capacity is exceeded");

            map.remove(&KeyRef::from_ref(&last));
        }

        let new_node = list.push_front(key);

        let key = list
            .get(new_node)
            .expect("BUG(LRU): a node just pushed to the list is not present in the list.");

        map.insert(KeyRef::from_ref(key), (value, new_node));
    }
}

impl<K, V> LruCache<K, V>
where
    K: Eq + Hash,
{
    #[allow(clippy::mut_from_ref)]
    unsafe fn list_mut(&self) -> &mut SlotMap<K> {
        unsafe { &mut *self.list.get() }
    }

    #[allow(clippy::mut_from_ref)]
    unsafe fn map_mut(&self) -> &mut HashMap<KeyRef<K>, (V, NodeHandle<K>)> {
        unsafe { &mut *self.map.get() }
    }
}

// Safety: No one besides us has the `UnsafeCell`. Therefore it is
// safe to transfer LruCache to other thread as long as K and V can both
// be as well.
unsafe impl<K, V> Send for LruCache<K, V>
where
    K: Send,
    V: Send,
    K: Eq + Hash,
{
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

    #[test]
    fn test_updating_existing_key_at_capacity_does_not_remove_any_keys() {
        let mut cache = LruCache::new(2);

        // evicted
        cache.put("key1", "value1");

        cache.put("key2", "value2");
        cache.put("key3", "value3");

        cache.put("key3", "value3 new");

        assert_eq!(cache.get(&"key1"), None);

        assert_eq!(cache.get(&"key2"), Some(&"value2"));
        assert_eq!(cache.get(&"key3"), Some(&"value3 new"));
    }
}
