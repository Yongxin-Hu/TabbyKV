use std::cmp;
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use crate::engines::lsm::iterators::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }.map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub struct MergeIterator<I: StorageIterator>{
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl <I: StorageIterator> MergeIterator<I>{
    pub fn create(iters: Vec<Box<I>>) -> MergeIterator<I> {

        if iters.is_empty() {
            return MergeIterator{
                iters: BinaryHeap::new(),
                current: None
            }
        }

        if iters.iter().all(|x| !x.is_valid()){
            let mut iters = iters;
            return MergeIterator{
                iters: BinaryHeap::new(),
                current: Some(HeapWrapper(0, iters.pop().unwrap())),
            }
        }
        let mut heap = BinaryHeap::new();
        for (idx, iter) in iters.into_iter().enumerate(){
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        }
        let current = heap.pop().unwrap();

        MergeIterator{
            iters: heap,
            current: Some(current)
        }
    }
}

impl <I: StorageIterator> StorageIterator for MergeIterator<I>{
    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn key(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.key()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|x| x.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> anyhow::Result<()> {
        let current = self.current.as_mut().unwrap();
        // Pop the item out of the heap if they have the same value.
        while let Some(mut inner_iter) = self.iters.peek_mut() {
            debug_assert!(
                inner_iter.1.key() >= current.1.key(),
                "heap invariant violated"
            );
            if inner_iter.1.key() == current.1.key() {
                // Case 1: an error occurred when calling `next`.
                if let e @ Err(_) = inner_iter.1.next() {
                    PeekMut::pop(inner_iter);
                    return e;
                }

                // Case 2: iter is no longer valid.
                if !inner_iter.1.is_valid() {
                    PeekMut::pop(inner_iter);
                }
            } else {
                break;
            }
        }

        current.1.next()?;

        // If the current iterator is invalid, pop it out of the heap and select the next one.
        if !current.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                *current = iter;
            }
            return Ok(());
        }

        // Otherwise, compare with heap top and swap if necessary.
        if let Some(mut inner_iter) = self.iters.peek_mut() {
            if *current < *inner_iter {
                std::mem::swap(&mut *inner_iter, current);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test{
    use anyhow::bail;
    use bytes::Bytes;
    use crate::engines::lsm::iterators::merge_iterator::MergeIterator;
    use crate::engines::lsm::iterators::StorageIterator;

    #[test]
    fn test_merge_iter_1(){
        let i1 = MockIterator::new(vec![
            (Bytes::from("a"), Bytes::from("1.1")),
            (Bytes::from("b"), Bytes::from("2.1")),
            (Bytes::from("c"), Bytes::from("3.1")),
            (Bytes::from("e"), Bytes::new()),
        ]);
        let i2 = MockIterator::new(vec![
            (Bytes::from("a"), Bytes::from("1.2")),
            (Bytes::from("b"), Bytes::from("2.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ]);
        let i3 = MockIterator::new(vec![
            (Bytes::from("b"), Bytes::from("2.3")),
            (Bytes::from("c"), Bytes::from("3.3")),
            (Bytes::from("d"), Bytes::from("4.3")),
        ]);

        let mut iter = MergeIterator::create(vec![
            Box::new(i1.clone()),
            Box::new(i2.clone()),
            Box::new(i3.clone()),
        ]);

        check_iter_result_by_key(
            &mut iter,
            vec![
                (Bytes::from("a"), Bytes::from("1.1")),
                (Bytes::from("b"), Bytes::from("2.1")),
                (Bytes::from("c"), Bytes::from("3.1")),
                (Bytes::from("d"), Bytes::from("4.2")),
                (Bytes::from("e"), Bytes::new()),
            ],
        );

        let mut iter = MergeIterator::create(vec![Box::new(i3), Box::new(i1), Box::new(i2)]);

        check_iter_result_by_key(
            &mut iter,
            vec![
                (Bytes::from("a"), Bytes::from("1.1")),
                (Bytes::from("b"), Bytes::from("2.3")),
                (Bytes::from("c"), Bytes::from("3.3")),
                (Bytes::from("d"), Bytes::from("4.3")),
                (Bytes::from("e"), Bytes::new()),
            ],
        );
    }

    #[derive(Clone)]
    pub struct MockIterator {
        pub data: Vec<(Bytes, Bytes)>,
        pub error_when: Option<usize>,
        pub index: usize,
    }

    impl MockIterator {
        pub fn new(data: Vec<(Bytes, Bytes)>) -> Self {
            Self {
                data,
                index: 0,
                error_when: None,
            }
        }

        pub fn new_with_error(data: Vec<(Bytes, Bytes)>, error_when: usize) -> Self {
            Self {
                data,
                index: 0,
                error_when: Some(error_when),
            }
        }
    }

    impl StorageIterator for MockIterator {
        fn value(&self) -> &[u8] {
            if let Some(error_when) = self.error_when {
                if self.index >= error_when {
                    panic!("invalid access after next returns an error!");
                }
            }
            self.data[self.index].1.as_ref()
        }

        fn key(&self) -> &[u8] {
            if let Some(error_when) = self.error_when {
                if self.index >= error_when {
                    panic!("invalid access after next returns an error!");
                }
            }
            self.data[self.index].0.as_ref()
        }

        fn is_valid(&self) -> bool {
            if let Some(error_when) = self.error_when {
                if self.index >= error_when {
                    panic!("invalid access after next returns an error!");
                }
            }
            self.index < self.data.len()
        }

        fn next(&mut self) -> anyhow::Result<()> {
            if self.index < self.data.len() {
                self.index += 1;
            }
            if let Some(error_when) = self.error_when {
                if self.index == error_when {
                    bail!("fake error!");
                }
            }
            Ok(())
        }
    }

    pub fn as_bytes(x: &[u8]) -> Bytes {
        Bytes::copy_from_slice(x)
    }

    pub fn check_iter_result_by_key<I>(iter: &mut I, expected: Vec<(Bytes, Bytes)>)
        where
            I: StorageIterator,
    {
        for (k, v) in expected {
            assert!(iter.is_valid());
            assert_eq!(
                k,
                iter.key(),
                "expected key: {:?}, actual key: {:?}",
                k,
                as_bytes(iter.key()),
            );
            assert_eq!(
                v,
                iter.value(),
                "expected value: {:?}, actual value: {:?}",
                v,
                as_bytes(iter.value()),
            );
            iter.next().unwrap();
        }
        assert!(!iter.is_valid());
    }
}