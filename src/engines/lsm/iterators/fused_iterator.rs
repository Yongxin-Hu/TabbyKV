use anyhow::bail;
use crate::engines::lsm::iterators::StorageIterator;

// A safe wrapper for StorageIterator
pub struct FusedIterator<I: StorageIterator>{
    iter: I,
    occur_error: bool
}

impl <I: StorageIterator> FusedIterator<I>{
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            occur_error: false
        }
    }
}
impl <I: StorageIterator> StorageIterator for FusedIterator<I>{
    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("some error occur.")
        }
        self.iter.value()
    }

    fn key(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("some error occur.")
        }
        self.iter.key()
    }

    fn is_valid(&self) -> bool{
        !self.occur_error && self.iter.is_valid()
    }

    fn next(&mut self) -> anyhow::Result<()> {
        if self.occur_error {
            bail!("some error occur.")
        }

        if self.iter.is_valid(){
            if let Err(e) = self.iter.next() {
                self.occur_error = true;
                return Err(e);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test{
    use anyhow::bail;
    use bytes::Bytes;
    use crate::engines::lsm::iterators::fused_iterator::FusedIterator;
    use crate::engines::lsm::iterators::StorageIterator;

    #[test]
    pub fn test_fused_iterator(){
        let iter = MockIterator::new(vec![]);
        let mut fused_iter = FusedIterator::new(iter);
        assert!(!fused_iter.is_valid());
        fused_iter.next().unwrap();
        fused_iter.next().unwrap();
        fused_iter.next().unwrap();
        assert!(!fused_iter.is_valid());

        let iter = MockIterator::new_with_error(
            vec![
                (Bytes::from("a"), Bytes::from("1.1")),
                (Bytes::from("a"), Bytes::from("1.1")),
            ],
            1,
        );
        let mut fused_iter = FusedIterator::new(iter);
        assert!(fused_iter.is_valid());
        assert!(fused_iter.next().is_err());
        assert!(!fused_iter.is_valid());
        assert!(fused_iter.next().is_err());
        assert!(fused_iter.next().is_err());
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