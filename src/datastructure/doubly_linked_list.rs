use std::ptr::NonNull;
use std::marker::PhantomData;

pub struct NodeHandle<'a, T> {
    node: NonNull<Node<T>>,
    lifetime: PhantomData<&'a T>,
}

impl<'a, T> NodeHandle<'a, T> {
    pub fn value(&self) -> &'a T {
        unsafe { &self.node.as_ref().value }
    }
}

struct Node<T> {
    value: T,
    prev: Option<NonNull<Node<T>>>,
    next: Option<NonNull<Node<T>>>,
}

/// A doubly linked list.
///
/// Unlike std::collections::LinkedList, this gives you
/// access to the node pointers.
/// Useful for implementing LRU cache.
pub struct DoublyLinkedList<'a, T> {
    head: Option<NonNull<Node<T>>>,
    tail: Option<NonNull<Node<T>>>,
    lifetime: PhantomData<&'a T>,
}

impl<'a, T> DoublyLinkedList<'a, T> {
    pub fn new() -> Self {
        Self { head: None, tail: None, lifetime: PhantomData }
    }

    pub fn push_front(&mut self, value: T) -> NodeHandle<'a, T> {
        let node = Box::new(Node {
            value,
            prev: None,
            next: self.head,
        });

        let node = NonNull::new(Box::into_raw(node)).unwrap();

        if let Some(mut head) = self.head {
            unsafe {
                head.as_mut().prev = Some(node);
            }
        } else {
            self.tail = Some(node);
            self.head = Some(node);
        }

        self.head = Some(node);

        NodeHandle { node, lifetime: PhantomData }
    }

    pub fn tail(&self) -> Option<NodeHandle<'a, T>> {
        self.tail.map(|node| NodeHandle { node, lifetime: PhantomData })
    }

    pub fn remove(&mut self, node: NodeHandle<'a, T>) {
        let node = node.node;

        if let Some(mut next) = unsafe { node.as_ref().next } {
            unsafe {
                next.as_mut().prev = node.as_ref().prev;
            }
        }

        if let Some(mut prev) = unsafe { node.as_ref().prev } {
            unsafe {
                prev.as_mut().next = node.as_ref().next;
            }
        }

        if self.head == Some(node) {
            self.head = unsafe { node.as_ref().next };
        }

        if self.tail == Some(node) {
            self.tail = unsafe { node.as_ref().prev };
        }

        unsafe {
            drop(Box::from_raw(node.as_ptr()));
        }
    }

    #[cfg(test)]
    fn iter(&self) -> impl Iterator<Item = &'a T> {
        let mut cur = self.head;

        std::iter::from_fn(move || {
            cur.map(|node| {
                let node = unsafe { node.as_ref() };
                cur = node.next;
                &node.value
            })
        })
    }
}

impl<'a, T> Drop for DoublyLinkedList<'a, T> {
    fn drop(&mut self) {
        let mut cur = self.head;
        while let Some(node) = cur {
            unsafe {
                cur = node.as_ref().next;
                drop(Box::from_raw(node.as_ptr()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pushed_values_are_in_correct_order() {
        let mut list = DoublyLinkedList::new();
        list.push_front(1);
        list.push_front(2);
        list.push_front(3);

        assert_eq!(list.iter().collect::<Vec<_>>(), vec![&3, &2, &1]);
    }

    #[test]
    fn test_can_remove_node_from_middle() {
        let mut list = DoublyLinkedList::new();
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
        let mut list = DoublyLinkedList::new();
        list.push_front(1);
        list.push_front(2);
        let node = list.push_front(3);

        list.remove(node);
        assert_eq!(list.iter().collect::<Vec<_>>(), vec![&2, &1]);
    }

    #[test]
    fn test_can_remove_node_from_back() {
        let mut list = DoublyLinkedList::new();
        let node = list.push_front(1);
        list.push_front(2);
        list.push_front(3);

        list.remove(node);
        assert_eq!(list.iter().collect::<Vec<_>>(), vec![&3, &2]);
    }

}