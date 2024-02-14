use crate::engines::lsm::iterators::merge_iterator::MergeIterator;
use crate::engines::lsm::iterators::StorageIterator;
use crate::engines::lsm::mem_table::MemTableIterator;

type LsmIteratorInner = MergeIterator<MemTableIterator>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
}

impl LsmIterator {
    pub fn new(iter: LsmIteratorInner) -> LsmIterator {
        Self{ inner: iter }
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
        self.inner.is_valid()
    }

    fn next(&mut self) -> anyhow::Result<()> {
        self.inner.next()
    }
}