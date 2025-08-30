//! A linked-list like data structure that allows for O(1) read/write access to nodes via a "handle"
//! returned when pushing to the list.
//!
//! # Example
//! ```ignore
//! let mut map = SlotMap::new();
//! let handle1 = map.push_front(10);
//! let handle2 = map.push_front(20);
//!
//! assert_eq!(map.get(handle1), Some(&10));
//! assert_eq!(map.get(handle2), Some(&20));
//! ```
//!
//! Handles are automatically invalidated when the node is removed from the list. The handles themselves
//! are safe to use after the node has been removed from the list.
//! The get operation will return None if the handle used is invalid.
//!
//! # Example
//! ```ignore
//! let mut map = SlotMap::new();
//! let handle = map.push_front(10);
//!
//! assert_eq!(map.get(handle), Some(&10));
//!
//! map.remove(handle);
//!
//! assert_eq!(map.get(handle), None);
//! ```
//!
//! The entire list can be iterated over in O(n) time.
//!
//! # Example
//! ```ignore
//! # use crate::datastructure::slotmap::SlotMap;
//!
//! let mut map = SlotMap::new();
//! map.push_front(10);
//! map.push_front(20);
//!
//! for value in map.iter() {
//!     println!("{}", value);
//! }
//! ```

use std::marker::PhantomData;

/// A handle to a node in the list.
///
/// This can be used to read/write the value of the node.
///
/// # Example
/// ```ignore
/// let mut map = SlotMap::new();
/// let handle = map.push_front(10);
///
/// assert_eq!(map.get(handle), Some(&10));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeHandle<T> {
    index: usize,
    generation: u64,

    phantom: PhantomData<T>,
}

#[derive(Default)]
struct Slot<T> {
    value: Option<T>,
    generation: u64,

    next: Option<usize>,
    prev: Option<usize>,
}

/// A linked-list like data structure that allows for O(1) read/write access to nodes via a "handle"
/// returned when pushing to the list.
///
/// # Example
/// ```ignore
/// let mut map = SlotMap::new();
/// let handle = map.push_front(10);
///
/// assert_eq!(map.get(handle), Some(&10));
/// ```
pub struct SlotMap<T> {
    slots: Vec<Slot<T>>,
    free_list: Vec<usize>,

    head: Option<usize>,
    tail: Option<usize>,
}

impl<T> SlotMap<T> {
    /// Creates a new empty `SlotMap`.
    pub fn new() -> Self {
        Self {
            slots: Vec::new(),
            free_list: Vec::new(),
            head: None,
            tail: None,
        }
    }

    /// Creates a new `SlotMap` with the given number of slots pre-allocated.
    pub fn new_with_capacity(capacity: usize) -> Self {
        Self {
            slots: Vec::with_capacity(capacity),
            free_list: Vec::with_capacity(capacity),
            head: None,
            tail: None,
        }
    }

    /// Returns a reference to the value of a node in the list.
    ///
    /// # Example
    /// ```ignore
    /// let mut map = SlotMap::new();
    /// let handle = map.push_front(10);
    ///
    /// assert_eq!(map.get(handle), Some(&10));
    /// ```
    ///
    /// If a handle to a node is used after the node has been removed from the list, the operation will
    /// return None.
    ///
    /// # Example
    /// ```ignore
    /// let mut map = SlotMap::new();
    /// let handle = map.push_front(10);
    ///
    /// assert_eq!(map.get(handle), Some(&10));
    ///
    /// map.remove(handle);
    ///
    /// assert_eq!(map.get(handle), None);
    /// ```
    pub fn get(&self, node: NodeHandle<T>) -> Option<&T> {
        if node.generation != self.slots[node.index].generation {
            return None;
        }

        self.slots[node.index].value.as_ref()
    }

    /// Push a new value to the front of the list.
    ///
    /// # Example
    /// ```ignore
    /// # use crate::datastructure::slotmap::SlotMap;
    ///
    /// let mut map = SlotMap::new();
    /// map.push_front(1);
    /// map.push_front(2);
    ///
    /// assert_eq!(map.iter().collect::<Vec<_>>(), vec![&2, &1]);
    /// ```
    pub fn push_front(&mut self, value: T) -> NodeHandle<T> {
        let index = self.free_list.pop();

        let generation = if let Some(index) = index {
            self.slots[index].generation + 1
        } else {
            0
        };

        let slot = Slot {
            value: Some(value),
            generation,
            next: self.head,
            prev: None,
        };

        if let Some(index) = index {
            self.slots[index] = slot;
        } else {
            self.slots.push(slot);
        }

        let index = index.unwrap_or(self.slots.len() - 1);

        if let Some(head) = self.head {
            assert!(self.slots[head].prev.is_none());

            self.slots[head].prev = Some(index);
        } else {
            // Absence of head implies the list was empty.
            // This new element is thererefore first and only element in the list.
            // Update tail to point to this new element.
            assert!(self.tail.is_none());
            self.tail = Some(index);
        }

        self.head = Some(index);

        NodeHandle {
            index,
            generation,
            phantom: PhantomData,
        }
    }

    /// Returns the handle to the tail of the list. If the list is empty, this will return None.
    ///
    /// # Example
    /// ```ignore
    /// let mut map = SlotMap::new();
    /// map.push_front(1);
    /// map.push_front(2);
    ///
    /// assert_eq!(map.get(map.tail().unwrap()), Some(&1));
    /// ```
    pub fn tail(&self) -> Option<NodeHandle<T>> {
        self.tail.map(|index| NodeHandle {
            index,
            generation: self.slots[index].generation,
            phantom: PhantomData,
        })
    }

    /// Removes the last element from the list and returns it.
    ///
    /// # Example
    /// ```ignore
    /// let mut map = SlotMap::new();
    /// map.push_front(1);
    /// map.push_front(2);
    ///
    /// assert_eq!(map.pop_back(), Some(1));
    /// ```
    pub fn pop_back(&mut self) -> Option<T> {
        let index = self.tail?;

        let value = self.slots[index].value.take();

        self.remove_from_slot_index(index);

        value
    }

    /// Removes a node from the list via a handle.
    ///
    /// If the handle is invalid, this will do nothing.
    ///
    /// # Example
    /// ```ignore
    /// let mut map = SlotMap::new();
    /// map.push_front(1);
    /// let handle = map.push_front(2);
    /// map.push_front(3);
    ///
    /// map.remove(handle);
    ///
    /// assert_eq!(map.iter().collect::<Vec<_>>(), vec![&3, &1]);
    /// ```
    pub fn remove(&mut self, node: NodeHandle<T>) {
        let slot = &mut self.slots[node.index];

        let generation = slot.generation;

        if slot.value.is_none() {
            return;
        }

        if generation != node.generation {
            return;
        }

        self.remove_from_slot_index(node.index);
    }

    /// Removes a node from the list at a slot index.
    /// Assumes that the slot is valid and has a value.
    ///
    /// This is an internal API and should not be used from outside.
    fn remove_from_slot_index(&mut self, index: usize) {
        let slot = &mut self.slots[index];

        let prev = slot.prev;
        let next = slot.next;

        slot.value = None;

        if let Some(next) = next {
            self.slots[next].prev = prev;
        }

        if let Some(prev) = prev {
            self.slots[prev].next = next;
        }

        if self.head == Some(index) {
            self.head = next;
        }

        if self.tail == Some(index) {
            self.tail = prev;
        }

        self.free_list.push(index);
        self.slots[index].value = None;
    }

    /// Creates an iterator over the list.
    ///
    /// # Example
    /// ```ignore
    /// let mut map = SlotMap::new();
    /// map.push_front(1);
    /// map.push_front(2);
    ///
    /// assert_eq!(map.iter().collect::<Vec<_>>(), vec![&2, &1]);
    /// ```
    fn iter(&self) -> impl Iterator<Item = &T> {
        std::iter::from_fn({
            let mut current = self.head;
            move || {
                if let Some(index) = current {
                    current = self.slots[index].next;
                    self.slots[index].value.as_ref()
                } else {
                    None
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pushed_values_are_in_correct_order() {
        let mut list = SlotMap::new();
        list.push_front(1);
        list.push_front(2);
        list.push_front(3);

        assert_eq!(list.iter().collect::<Vec<_>>(), vec![&3, &2, &1]);
    }

    #[test]
    fn test_can_remove_node_from_middle() {
        let mut list = SlotMap::new();
        list.push_front(1);
        list.push_front(2);
        let node3 = list.push_front(3);
        list.push_front(4);
        list.push_front(5);

        list.remove(node3);

        assert_eq!(list.iter().collect::<Vec<_>>(), vec![&5, &4, &2, &1]);
    }

    #[test]
    fn test_can_remove_node_from_front() {
        let mut list = SlotMap::new();
        list.push_front(1);
        list.push_front(2);
        let node = list.push_front(3);

        list.remove(node);
        assert_eq!(list.iter().collect::<Vec<_>>(), vec![&2, &1]);
    }

    #[test]
    fn test_can_remove_node_from_back() {
        let mut list = SlotMap::new();
        let node = list.push_front(1);
        list.push_front(2);
        list.push_front(3);

        list.remove(node);
        assert_eq!(list.iter().collect::<Vec<_>>(), vec![&3, &2]);
    }

    #[test]
    fn test_invalid_handle_does_not_remove_node() {
        let mut list = SlotMap::new();

        list.push_front(0);
        list.push_front(1);

        let node = list.push_front(2);
        list.remove(node);

        list.push_front(3);
        list.push_front(4);

        list.remove(node);

        assert_eq!(list.iter().collect::<Vec<_>>(), vec![&4, &3, &1, &0]);
    }
}
