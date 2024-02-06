use std::sync::Arc;
use anyhow::bail;
use crate::engines::lsm::iterators::merge_iterator::MergeIterator;
use crate::engines::lsm::mem_table::MemTableIterator;

pub mod merge_iterator;
pub mod fused_iterator;

pub trait StorageIterator {
    /// 获取当前 value
    fn value(&self) -> &[u8];

    /// 获取当前 key.
    fn key(&self) -> &[u8];

    /// 检查当前 iterator 是否有效
    fn is_valid(&self) -> bool;

    /// 移动到下一个位置
    fn next(&mut self) -> anyhow::Result<()>;
}

type LsmIteratorInner = MergeIterator<MemTableIterator>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
}

impl LsmIterator{
    pub fn new(iter: LsmIteratorInner) -> LsmIterator{
        Self{ inner: iter }
    }
}

impl StorageIterator for LsmIterator{
    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn key(&self) -> &[u8] {
        self.inner.key()
    }

    fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    fn next(&mut self) -> anyhow::Result<()> {
        self.inner.next()
    }
}