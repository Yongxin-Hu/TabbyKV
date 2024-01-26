use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use crate::engines::lsm::wal::Wal;
use anyhow::Result;
use crossbeam_skiplist::map::Entry;
use crate::engines::lsm::iterators::StorageIterator;

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

    /// 将 KV对 放入mem_table
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let estimated_size = key.len() + value.len();
        self.map.insert(Bytes::copy_from_slice(key),Bytes::copy_from_slice(value));
        self.approximate_size
            .fetch_add(estimated_size, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemTableIterator {
        let map_bound = |source: Bound<&[u8]>| {
            match source {
                Bound::Included(t) => Bound::Included(Bytes::copy_from_slice(t)),
                Bound::Excluded(t) => Bound::Excluded(Bytes::copy_from_slice(t)),
                Bound::Unbounded => Bound::Unbounded
            }
        };
        let (lower, upper) = (map_bound(lower), map_bound(upper));
        let current = self.map.lower_bound(lower.as_ref());
        MemTableIterator{
            map: Arc::clone(&self.map),
            upper,
            lower,
            current
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

pub struct MemTableIterator<'a> {
    map: Arc<SkipMap<Bytes, Bytes>>,
    lower: Bound<Bytes>,
    upper: Bound<Bytes>,
    current: Option<Entry<'a, Bytes, Bytes>>,
}

impl StorageIterator for MemTableIterator<'_> {
    fn value(&self) -> &[u8] {
        self.current
            .as_ref()
            .map_or_else(|| &[][..], |entry| entry.value().as_ref())
    }

    fn key(&self) -> &[u8] {
        self.current
            .as_ref()
            .map_or_else(|| &[][..], |entry| entry.key().as_ref())
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        self.current = self.current.clone().and_then(|entry| {
            entry.next().filter(|next| {
                match &self.upper {
                    Bound::Unbounded => true,
                    Bound::Included(upp) => upp >= next.key(),
                    Bound::Excluded(upp) => upp > next.key(),
                }
            })
        });
        Ok(())
    }
}

#[cfg(test)]
mod test{
    use crate::engines::lsm::iterators::StorageIterator;
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

    #[test]
    fn test_memtable_iter() {
        use std::ops::Bound;
        let memtable = MemTable::create(0);
        memtable.put(b"key1", b"value1").unwrap();
        memtable.put(b"key2", b"value2").unwrap();
        memtable.put(b"key3", b"value3").unwrap();

        {
            let mut iter = memtable.scan(Bound::Unbounded, Bound::Unbounded);
            assert_eq!(iter.key(), b"key1");
            assert_eq!(iter.value(), b"value1");
            assert!(iter.is_valid());
            iter.next().unwrap();
            assert_eq!(iter.key(), b"key2");
            assert_eq!(iter.value(), b"value2");
            assert!(iter.is_valid());
            iter.next().unwrap();
            assert_eq!(iter.key(), b"key3");
            assert_eq!(iter.value(), b"value3");
            assert!(iter.is_valid());
            iter.next().unwrap();
            assert!(!iter.is_valid());
        }

        {
            let mut iter = memtable.scan(Bound::Included(b"key1"), Bound::Included(b"key2"));
            assert_eq!(iter.key(), b"key1");
            assert_eq!(iter.value(), b"value1");
            assert!(iter.is_valid());
            iter.next().unwrap();
            assert_eq!(iter.key(), b"key2");
            assert_eq!(iter.value(), b"value2");
            assert!(iter.is_valid());
            iter.next().unwrap();
            assert!(!iter.is_valid());
        }

        {
            let mut iter = memtable.scan(Bound::Excluded(b"key1"), Bound::Excluded(b"key3"));
            assert_eq!(iter.key(), b"key2");
            assert_eq!(iter.value(), b"value2");
            assert!(iter.is_valid());
            iter.next().unwrap();
            assert!(!iter.is_valid());
        }
    }

    #[test]
    fn test_task1_empty_memtable_iter() {
        use std::ops::Bound;
        let memtable = MemTable::create(0);
        {
            let iter = memtable.scan(Bound::Excluded(b"key1"), Bound::Excluded(b"key3"));
            assert!(!iter.is_valid());
        }
        {
            let iter = memtable.scan(Bound::Included(b"key1"), Bound::Included(b"key2"));
            assert!(!iter.is_valid());
        }
        {
            let iter = memtable.scan(Bound::Unbounded, Bound::Unbounded);
            assert!(!iter.is_valid());
        }
    }
}