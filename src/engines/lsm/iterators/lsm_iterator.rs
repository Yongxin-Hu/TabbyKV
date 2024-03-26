use std::ops::Bound;
use bytes::Bytes;
use crate::engines::lsm::iterators::merge_iterator::MergeIterator;
use crate::engines::lsm::iterators::StorageIterator;
use crate::engines::lsm::iterators::two_merge_iterator::TwoMergeIterator;
use crate::engines::lsm::mem_table::MemTableIterator;
use crate::engines::lsm::table::iterator::SsTableIterator;
use anyhow::Result;
use crate::engines::lsm::iterators::concat_iterator::SstConcatIterator;

type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>>;


pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    is_valid: bool,
    read_ts: u64,
    prev_key: Vec<u8>,
}

impl LsmIterator {
    pub fn new(iter: LsmIteratorInner,
               read_ts: u64,
               end_bound: Bound<Bytes>,
    ) -> Result<LsmIterator> {
        let mut iter = Self {
            is_valid: iter.is_valid(),
            inner: iter,
            read_ts,
            end_bound,
            prev_key: Vec::new()
        };
        iter.move_to_key()?;

        Ok(iter)
    }

    fn next_inner(&mut self) -> Result<()> {
        self.inner.next()?;
        if !self.inner.is_valid(){
            self.is_valid = false;
            return Ok(());
        }
        match &self.end_bound{
            Bound::Included(key) => self.is_valid = self.inner.key().key_ref() <= key.as_ref(),
            Bound::Excluded(key) => self.is_valid = self.inner.key().key_ref() < key.as_ref(),
            Bound::Unbounded => {}
        };
        Ok(())
    }

    fn move_to_key(&mut self) -> Result<()> {
        loop {
            while self.inner.is_valid() && self.inner.key().key_ref() == self.prev_key {
                self.next_inner()?;
            }
            if !self.inner.is_valid() {
                break;
            }
            self.prev_key.clear();
            self.prev_key.extend(self.inner.key().key_ref());
            while self.inner.is_valid()
                && self.inner.key().key_ref() == self.prev_key
                && self.inner.key().ts() > self.read_ts
            {
                self.next_inner()?;
            }
            if !self.inner.is_valid() {
                break;
            }
            if self.inner.key().key_ref() != self.prev_key {
                continue;
            }
            if !self.inner.value().is_empty() {
                break;
            }
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn next(&mut self) -> Result<()> {
        self.next_inner()?;
        self.move_to_key()?;
        Ok(())
    }
}