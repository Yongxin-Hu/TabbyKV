use crate::engines::lsm::iterators::StorageIterator;
use anyhow::Result;

// 合并 memtable_iterator 和 sstable_iterator
pub struct TwoMergeIterator<T: StorageIterator, E: StorageIterator> {
    // 用于 MergeIterator<memtable_iterator>
    first: T,
    // 用于 MergeIterator<sstable_iterator>
    second: E,
    use_first: bool
}

impl <T: 'static + StorageIterator,
      E: 'static + for<'a> StorageIterator<KeyType<'a> = T::KeyType<'a>>>
TwoMergeIterator<T, E>{
    fn choose_iterator(first: &T, second: &E) -> bool{
        if !first.is_valid() {
            return false;
        }
        if !second.is_valid() {
            return true
        }
        first.key() < second.key()
    }

    fn try_skip_second(&mut self) -> Result<()>{
        if self.first.is_valid() && self.second.is_valid() && self.first.key() == self.second.key() {
            self.second.next()?;
        }
        Ok(())
    }

    pub fn create(first: T, second: E) -> Result<Self>{
        let mut iter = Self{
            first,
            second,
            use_first: false
        };
        iter.try_skip_second()?;
        iter.use_first = Self::choose_iterator(&iter.first, &iter.second);
        Ok(iter)
    }
}

impl <T: 'static + StorageIterator,
    E: 'static + for <'a> StorageIterator<KeyType<'a> = T::KeyType<'a>>>
StorageIterator for TwoMergeIterator<T, E> {
    type KeyType<'a> = T::KeyType<'a>;

    fn value(&self) -> &[u8] {
        if self.use_first {
            self.first.value()
        } else {
            self.second.value()
        }
    }

    fn key(&self) -> T::KeyType<'_> {
        if self.use_first {
            self.first.key()
        } else {
            self.second.key()
        }
    }

    fn is_valid(&self) -> bool {
        if self.use_first {
            self.first.is_valid()
        }else{
            self.second.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.use_first {
            self.first.next()?;
        } else {
            self.second.next()?;
        }
        self.try_skip_second()?;
        self.use_first = Self::choose_iterator(&self.first, &self.second);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use crate::engines::lsm::iterators::two_merge_iterator::TwoMergeIterator;
    use crate::engines::lsm::utils::{check_iter_result_by_key, MockIterator};

    #[test]
    fn test_two_merge_iterator() {
        let i1 = MockIterator::new(vec![
            (Bytes::from("a"), Bytes::from("1.1")),
            (Bytes::from("b"), Bytes::from("2.1")),
            (Bytes::from("c"), Bytes::from("3.1")),
        ]);
        let i2 = MockIterator::new(vec![
            (Bytes::from("a"), Bytes::from("1.2")),
            (Bytes::from("b"), Bytes::from("2.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ]);
        let mut iter = TwoMergeIterator::create(i1, i2).unwrap();
        check_iter_result_by_key(
            &mut iter,
            vec![
                (Bytes::from("a"), Bytes::from("1.1")),
                (Bytes::from("b"), Bytes::from("2.1")),
                (Bytes::from("c"), Bytes::from("3.1")),
                (Bytes::from("d"), Bytes::from("4.2")),
            ],
        )
    }

    #[test]
    fn test_two_merge_iterator_2() {
        let i2 = MockIterator::new(vec![
            (Bytes::from("a"), Bytes::from("1.1")),
            (Bytes::from("b"), Bytes::from("2.1")),
            (Bytes::from("c"), Bytes::from("3.1")),
        ]);
        let i1 = MockIterator::new(vec![
            (Bytes::from("a"), Bytes::from("1.2")),
            (Bytes::from("b"), Bytes::from("2.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ]);
        let mut iter = TwoMergeIterator::create(i1, i2).unwrap();
        check_iter_result_by_key(
            &mut iter,
            vec![
                (Bytes::from("a"), Bytes::from("1.2")),
                (Bytes::from("b"), Bytes::from("2.2")),
                (Bytes::from("c"), Bytes::from("3.2")),
                (Bytes::from("d"), Bytes::from("4.2")),
            ],
        )
    }

    #[test]
    fn test_task1_merge_3() {
        let i2 = MockIterator::new(vec![
            (Bytes::from("a"), Bytes::from("1.1")),
            (Bytes::from("b"), Bytes::from("2.1")),
            (Bytes::from("c"), Bytes::from("3.1")),
        ]);
        let i1 = MockIterator::new(vec![
            (Bytes::from("b"), Bytes::from("2.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ]);
        let mut iter = TwoMergeIterator::create(i1, i2).unwrap();
        check_iter_result_by_key(
            &mut iter,
            vec![
                (Bytes::from("a"), Bytes::from("1.1")),
                (Bytes::from("b"), Bytes::from("2.2")),
                (Bytes::from("c"), Bytes::from("3.2")),
                (Bytes::from("d"), Bytes::from("4.2")),
            ],
        )
    }

    #[test]
    fn test_task1_merge_4() {
        let i2 = MockIterator::new(vec![]);
        let i1 = MockIterator::new(vec![
            (Bytes::from("b"), Bytes::from("2.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ]);
        let mut iter = TwoMergeIterator::create(i1, i2).unwrap();
        check_iter_result_by_key(
            &mut iter,
            vec![
                (Bytes::from("b"), Bytes::from("2.2")),
                (Bytes::from("c"), Bytes::from("3.2")),
                (Bytes::from("d"), Bytes::from("4.2")),
            ],
        );
        let i1 = MockIterator::new(vec![]);
        let i2 = MockIterator::new(vec![
            (Bytes::from("b"), Bytes::from("2.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ]);
        let mut iter = TwoMergeIterator::create(i1, i2).unwrap();
        check_iter_result_by_key(
            &mut iter,
            vec![
                (Bytes::from("b"), Bytes::from("2.2")),
                (Bytes::from("c"), Bytes::from("3.2")),
                (Bytes::from("d"), Bytes::from("4.2")),
            ],
        );
    }

    #[test]
    fn test_task1_merge_5() {
        let i2 = MockIterator::new(vec![]);
        let i1 = MockIterator::new(vec![]);
        let mut iter = TwoMergeIterator::create(i1, i2).unwrap();
        check_iter_result_by_key(&mut iter, vec![])
    }
}