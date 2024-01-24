use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use crate::engines::lsm::wal::Wal;
use anyhow::Result;

pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

impl MemTable {
    /// 创建新的 mem_table
    pub fn create(id: usize) -> Self {
        MemTable {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            approximate_size: Arc::new(AtomicUsize::new(0))
        }
    }

    /// 根据 key 获取 value
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        match self.map.get(key){
            None => None,
            Some(t) => Some(t.value().clone())
        }
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.map.insert(Bytes::copy_from_slice(key),Bytes::copy_from_slice(value));
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }
}

#[cfg(test)]
mod test{
    use crate::engines::lsm::mem_table::MemTable;

    #[test]
    fn test_mem_table_get() {
        let mem_table = MemTable::create(0);
        mem_table.put(b"key1", b"value1").unwrap();
        mem_table.put(b"key2", b"value2").unwrap();
        mem_table.put(b"key3", b"value3").unwrap();
        assert_eq!(&mem_table.get(b"key1").unwrap()[..], b"value1");
        assert_eq!(&mem_table.get(b"key2").unwrap()[..], b"value2");
        assert_eq!(&mem_table.get(b"key3").unwrap()[..], b"value3");
    }

    #[test]
    fn test_mem_table_overwrite() {
        let mem_table = MemTable::create(0);
        mem_table.put(b"key1", b"value1").unwrap();
        mem_table.put(b"key2", b"value2").unwrap();
        mem_table.put(b"key3", b"value3").unwrap();
        mem_table.put(b"key1", b"value11").unwrap();
        mem_table.put(b"key2", b"value22").unwrap();
        mem_table.put(b"key3", b"value33").unwrap();
        assert_eq!(&mem_table.get(b"key1").unwrap()[..], b"value11");
        assert_eq!(&mem_table.get(b"key2").unwrap()[..], b"value22");
        assert_eq!(&mem_table.get(b"key3").unwrap()[..], b"value33");
    }
}