use anyhow::bail;
use crate::engines::lsm::iterators::StorageIterator;
use crate::engines::lsm::key::KeySlice;

/// 确保 iter 调用时是 valid 的
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
    type KeyType<'a> = I::KeyType<'a>
        where Self: 'a;

    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("some error occur.")
        }
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
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
    use bytes::Bytes;
    use crate::engines::lsm::iterators::fused_iterator::FusedIterator;
    use crate::engines::lsm::iterators::StorageIterator;
    use crate::engines::lsm::utils::MockIterator;

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
}