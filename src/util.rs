use std::collections::BinaryHeap;
use std::cmp::Reverse;

/// Merges multiple sorted iterators into a single sorted iterator, removing duplicates.
/// The iterators must be sorted.
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
}