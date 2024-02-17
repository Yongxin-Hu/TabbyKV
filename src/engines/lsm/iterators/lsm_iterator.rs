use std::ops::Bound;
use bytes::Bytes;
use crate::engines::lsm::iterators::merge_iterator::MergeIterator;
use crate::engines::lsm::iterators::StorageIterator;
use crate::engines::lsm::iterators::two_merge_iterator::TwoMergeIterator;
use crate::engines::lsm::mem_table::MemTableIterator;
use crate::engines::lsm::table::iterator::SsTableIterator;
use anyhow::Result;

type LsmIteratorInner = TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    is_valid: bool
}

impl LsmIterator {
    pub fn new(iter: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<LsmIterator> {
        let mut iter = Self {
            is_valid: iter.is_valid(),
            inner: iter,
            end_bound
        };
        iter.skip_delete_key()?;
        Ok(iter)
    }

    fn next_inner(&mut self) -> Result<()> {
        self.inner.next()?;
        if !self.inner.is_valid(){
            self.is_valid = false;
            return Ok(());
        }
        match &self.end_bound{
            Bound::Included(key) => self.is_valid = self.inner.key() <= key,
            Bound::Excluded(key) => self.is_valid = self.inner.key() < key,
            Bound::Unbounded => {}
        };
        Ok(())
    }

    fn skip_delete_key(&mut self) -> Result<()> {
        if self.is_valid() && self.value().is_empty() {
            self.next_inner()?;
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn key(&self) -> &[u8] {
        self.inner.key()
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn next(&mut self) -> Result<()> {
        self.next_inner()?;
        self.skip_delete_key()?;
        Ok(())
    }
}