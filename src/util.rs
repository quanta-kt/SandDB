use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use crate::store::Cursor;

/// A wrapper around a key-value pair that implements Ord, PartialOrd, Eq, and PartialEq
/// based only on the key.
struct KeyOnlyOrd((String, Vec<u8>));

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
pub(crate) fn merge_sorted_uniq_cursor<I>(mut sources: Vec<I>) -> impl Cursor 
where
    I: Cursor
{
    let mut heap = BinaryHeap::new();
    let mut error = None;
    let mut last: Option<KeyOnlyOrd> = None;
    let mut end = false;

    for (idx, source) in sources.iter_mut().enumerate() {
        match source.next() {
            Some(Ok(item)) => {
                heap.push(Reverse((KeyOnlyOrd(item), idx)))
            },
            Some(Err(e)) => {
                error = Some(e)
            },
            None => {},
        }
    }

    std::iter::from_fn(move || {
        if end {
            return None;
        }

        if let Some(error) = error.take() {
            end = true;
            return Some(Err(error));
        };

        while let Some(Reverse((item, idx))) = heap.pop() {
            let next: Option<I::Item> = sources[idx].next();

            match next {
                Some(Ok(next)) => heap.push(Reverse((KeyOnlyOrd(next), idx))),
                Some(e) => {
                    end = true;
                    return Some(e);
                }
                None => {},
            }

            if last.as_ref() == Some(&item) {
                continue;
            }

            match last.replace(item) {
                None => continue,
                Some(KeyOnlyOrd(prev)) => {
                    return Some(Ok(prev));
                }
            }
        }

        last.take().map(|KeyOnlyOrd(it)| Ok(it))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(n: i32) -> (String, Vec<u8>) {
        (format!("p{}", n), b"".to_vec())
    }

    #[test]
    fn test_merge_sorted() {

        let v1 = vec![Ok(p(1)), Ok(p(4)), Ok(p(7))];
        let v2 = vec![Ok(p(2)), Ok(p(5)), Ok(p(8))];
        let v3 = vec![Ok(p(2)), Ok(p(3)), Ok(p(6)), Ok(p(9))];

        let merged: Vec<_> = merge_sorted_uniq_cursor(vec![v1.into_iter(), v2.into_iter(), v3.into_iter()])
            .map(|it| it.unwrap())
            .collect();

        assert_eq!(merged, vec![p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9)]);
    }

    #[test]
    fn test_duplicates_are_dropped() {
        let v1 = vec![Ok(("foo".to_owned(), b"bar".to_vec()))]
            .into_iter();

        let v2 = vec![Ok(("foo".to_owned(), b"bar2".to_vec()))]
            .into_iter();

        let merged: Vec<_> = merge_sorted_uniq_cursor(vec![v2, v1])
            .map(|it| it.unwrap())
            .collect();

        assert_eq!(merged, vec![("foo".to_owned(), b"bar2".to_vec())]);
    }
}
