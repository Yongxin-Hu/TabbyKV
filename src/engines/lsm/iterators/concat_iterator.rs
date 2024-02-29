use std::sync::Arc;
use crate::engines::lsm::iterators::StorageIterator;
use crate::engines::lsm::table::iterator::SsTableIterator;
use crate::engines::lsm::table::SsTable;
use anyhow::Result;
use bytes::Bytes;

// 合并 key 没有重叠的 sstable_iterator, 减少不必要的 IO
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        unimplemented!()
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: Bytes) -> Result<Self> {
        unimplemented!()
    }
}

impl StorageIterator for SstConcatIterator{
    fn value(&self) -> &[u8] {
        unimplemented!()
    }

    fn key(&self) -> &[u8] {
        unimplemented!()
    }

    fn is_valid(&self) -> bool {
        unimplemented!()
    }

    fn next(&mut self) -> Result<()> {
        unimplemented!()
    }
}