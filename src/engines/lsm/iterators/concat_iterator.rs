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
    // 检查 sstables 是否满足 key 不重叠
    fn check_sst_valid(sstables: &[Arc<SsTable>]) {
        for sst in sstables {
            assert!(sst.first_key() <= sst.last_key());
        }
        if !sstables.is_empty() {
            for i in 0..(sstables.len() - 1) {
                assert!(sstables[i].last_key() < sstables[i + 1].first_key());
            }
        }
    }

    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        Self::check_sst_valid(&sstables);
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables
            })
        }
        let mut iter = Self {
                current: Some(SsTableIterator::create_and_seek_to_first(sstables[0].clone())?),
                next_sst_idx: 1,
                sstables
            };
        iter.move_to_valid()?;
        Ok(iter)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: Bytes) -> Result<Self> {
        Self::check_sst_valid(&sstables);
        let idx: usize = sstables
            .partition_point(|table| table.first_key() <= key)
            .saturating_sub(1);
        if idx >= sstables.len() {
            return Ok(Self {
                current: None,
                next_sst_idx: sstables.len(),
                sstables,
            });
        }
        let mut iter = Self {
            current: Some(SsTableIterator::create_and_seek_to_key(
                sstables[idx].clone(),
                key,
            )?),
            next_sst_idx: idx + 1,
            sstables,
        };
        iter.move_to_valid()?;
        Ok(iter)
    }

    fn move_to_valid(&mut self) -> Result<()>{
        while let Some(iter) = self.current.as_mut() {
            if iter.is_valid() {
                break;
            }
            if self.next_sst_idx >= self.sstables.len() {
                self.current = None;
            } else {
                self.current = Some(SsTableIterator::create_and_seek_to_first(
                    self.sstables[self.next_sst_idx].clone(),
                )?);
                self.next_sst_idx += 1;
            }
        }
        Ok(())
    }
}

impl StorageIterator for SstConcatIterator{
    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn key(&self) -> &[u8] {
        self.current.as_ref().unwrap().key()
    }

    fn is_valid(&self) -> bool {
        if let Some(current) = &self.current {
            assert!(current.is_valid());
            true
        } else {
            false
        }
    }

    fn next(&mut self) -> Result<()> {
        self.current.as_mut().unwrap().next()?;
        self.move_to_valid()?;
        Ok(())
    }
}