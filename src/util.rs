use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

/// A wrapper around a key-value pair that implements Ord, PartialOrd, Eq, and PartialEq
/// based only on the key.
pub struct KeyOnlyOrd((String, Vec<u8>));

impl KeyOnlyOrd {
    pub fn new(key: String, value: Vec<u8>) -> Self {
        Self((key, value))
    }
}

impl From<(String, Vec<u8>)> for KeyOnlyOrd {
    fn from(value: (String, Vec<u8>)) -> Self {
        Self(value)
    }
}

impl From<KeyOnlyOrd> for (String, Vec<u8>) {
    fn from(value: KeyOnlyOrd) -> Self {
        value.0
    }
}

impl PartialOrd for KeyOnlyOrd {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.0.0.cmp(&other.0.0))
    }
}

impl Ord for KeyOnlyOrd {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.0.cmp(&other.0.0)
    }
}

impl PartialEq for KeyOnlyOrd {
    fn eq(&self, other: &Self) -> bool {
        self.0.0 == other.0.0
    }
}

impl Eq for KeyOnlyOrd {}

/// Merges multiple sorted iterators into a single sorted iterator, removing duplicates.
/// The iterators must be sorted.
///
/// When a duplicate is encountered, the one that appears in the earliest source is kept.
///
/// # Example
/// ```ignore
/// let iter1 = vec![1, 2, 3].into_iter();
/// let iter2 = vec![2, 3, 4].into_iter();
/// let iter3 = vec![3, 4, 5].into_iter();
///
/// let merged = merge_sorted_uniq(vec![iter1, iter2, iter3]);
///
/// assert_eq!(merged.collect::<Vec<_>>(), vec![1, 2, 3, 4, 5]);
/// ```
pub(crate) fn merge_sorted_uniq<I>(mut sources: Vec<I>) -> impl Iterator<Item = I::Item>
where
    I: Iterator,
    I::Item: Ord,
{
    let mut heap = BinaryHeap::new();

    for (idx, source) in sources.iter_mut().enumerate() {
        if let Some(item) = source.next() {
            heap.push(Reverse((item, idx)));
        }
    }

    let mut last: Option<I::Item> = None;

    std::iter::from_fn(move || {
        while let Some(Reverse((item, idx))) = heap.pop() {
            let next: Option<I::Item> = sources[idx].next();

            if let Some(next) = next {
                heap.push(Reverse((next, idx)));
            }

            if last.as_ref() == Some(&item) {
                continue;
            }

            match last.replace(item) {
                None => continue,
                Some(prev) => {
                    return Some(prev);
                }
            }
        }

        last.take()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_sorted() {
        let v1 = vec![1, 4, 7];
        let v2 = vec![2, 5, 8];
        let v3 = vec![2, 3, 6, 9];

        let merged = merge_sorted_uniq(vec![v1.into_iter(), v2.into_iter(), v3.into_iter()]);

        assert_eq!(merged.collect::<Vec<_>>(), vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_duplicates_are_dropped() {
        let v1 = vec![("foo".to_owned(), b"bar".to_vec())]
            .into_iter()
            .map(Into::<KeyOnlyOrd>::into);

        let v2 = vec![("foo".to_owned(), b"bar2".to_vec())]
            .into_iter()
            .map(Into::<KeyOnlyOrd>::into);

        let merged: Vec<_> = merge_sorted_uniq(vec![v2, v1])
            .map(Into::<(String, Vec<u8>)>::into)
            .collect();

        assert_eq!(merged, vec![("foo".to_owned(), b"bar2".to_vec())]);
    }
}
