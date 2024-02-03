use crate::engines::lsm::iterators::StorageIterator;

pub struct MergeIterator<I: StorageIterator>;

impl <I: StorageIterator> MergeIterator<I>{
    pub fn create(iters: Vec<I>) -> MergeIterator<I> {
        unimplemented!()
    }
}

impl <I: StorageIterator> StorageIterator for MergeIterator<I>{
    fn value(&self) -> &[u8] {
        todo!()
    }

    fn key(&self) -> &[u8] {
        todo!()
    }

    fn is_valid(&self) -> bool {
        todo!()
    }

    fn next(&mut self) -> anyhow::Result<()> {
        todo!()
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