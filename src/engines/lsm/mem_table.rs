use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use crate::engines::lsm::wal::Wal;
use anyhow::Result;
use crossbeam_skiplist::map::Entry;
use ouroboros::self_referencing;
use crate::engines::lsm::iterators::StorageIterator;
use crate::engines::lsm::key::{KeyBytes, KeySlice};
use crate::engines::lsm::table::builder::SsTableBuilder;
use crate::engines::lsm::utils::{map_bound, map_key_bound};

pub struct MemTable {
    // 实际存储 KV-pair 的 SkipMap
    // TODO 用自己写的 SkipMap 替换
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    // WAL 预写日志
    wal: Option<Wal>,
    id: usize,
    // mem_table 预估大小
    approximate_size: Arc<AtomicUsize>,
}

impl MemTable {
    /// 创建新的 mem_table
    /// mem_table的`id`是 LSMStorageInner 中原子性递增的 next_sst_id
    pub fn create(id: usize) -> Self {
        MemTable {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            approximate_size: Arc::new(AtomicUsize::new(0))
        }
    }

    /// 创建带有 WAL 的 mem_table
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            id,
            map: Arc::new(SkipMap::new()),
            wal: Some(Wal::create(path.as_ref())?),
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// 使用 WAL 恢复 memtable 的 skipmap
    /// # 参数
    /// * `id` memtable 的 id
    /// * `path` wal 文件路径
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let map = Arc::new(SkipMap::new());
        Ok(Self {
            id,
            wal: Some(Wal::recover(path.as_ref(), &map)?),
            map,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// 根据 key 获取 value
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        match self.map.get(&KeyBytes::for_testing_from_bytes_no_ts(Bytes::copy_from_slice(key))){
            None => None,
            Some(t) => Some(t.value().clone())
        }
    }

    /// 将 KV-pair 放入 mem_table
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        let estimated_size = key.key_len() + value.len();
        self.map.insert(key.to_key_vec().into_key_bytes(),Bytes::copy_from_slice(value));
        self.approximate_size
            .fetch_add(estimated_size, std::sync::atomic::Ordering::Relaxed);
        if let Some(wal) = &self.wal {
            wal.put(key.key_ref(), value)?;
        }
        self.sync_wal()?;
        Ok(())
    }

    // 将 memtable 中的 kv-pair 加入 ss_table_builder
    pub fn flush(&self, ss_table_builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter(){
            ss_table_builder.add(entry.key().as_key_slice(), &entry.value()[..]);
        }
        Ok(())
    }

    pub fn scan(&self, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> MemTableIterator {
        let (lower, upper) = (map_key_bound(lower), map_key_bound(upper));
        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range((lower, upper)),
            item: (KeyBytes::new(), Bytes::new()),
        }.build();
        iter.next().unwrap();
        iter
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
}

impl MemTableIterator {
    fn entry_to_item(entry: Option<Entry<'_, KeyBytes, Bytes>>) -> (KeyBytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (KeyBytes::new(), Bytes::new()))
    }
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn key(&self) -> KeySlice {
        self.borrow_item().0.as_key_slice()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }
}

#[cfg(test)]
mod test{
    use crate::engines::lsm::iterators::StorageIterator;
    use crate::engines::lsm::key::KeySlice;
    use crate::engines::lsm::mem_table::MemTable;

    #[test]
    fn test_mem_table_get() {
        let mem_table = MemTable::create(0);
        mem_table.put(KeySlice::for_testing_from_slice_no_ts(b"key1"), b"value1").unwrap();
        mem_table.put(KeySlice::for_testing_from_slice_no_ts(b"key2"), b"value2").unwrap();
        mem_table.put(KeySlice::for_testing_from_slice_no_ts(b"key3"), b"value3").unwrap();
        assert_eq!(&mem_table.get(b"key1").unwrap()[..], b"value1");
        assert_eq!(&mem_table.get(b"key2").unwrap()[..], b"value2");
        assert_eq!(&mem_table.get(b"key3").unwrap()[..], b"value3");
    }

    #[test]
    fn test_mem_table_overwrite() {
        let mem_table = MemTable::create(0);
        mem_table.put(KeySlice::for_testing_from_slice_no_ts(b"key1"), b"value1").unwrap();
        mem_table.put(KeySlice::for_testing_from_slice_no_ts(b"key2"), b"value2").unwrap();
        mem_table.put(KeySlice::for_testing_from_slice_no_ts(b"key3"), b"value3").unwrap();
        mem_table.put(KeySlice::for_testing_from_slice_no_ts(b"key1"), b"value11").unwrap();
        mem_table.put(KeySlice::for_testing_from_slice_no_ts(b"key2"), b"value22").unwrap();
        mem_table.put(KeySlice::for_testing_from_slice_no_ts(b"key3"), b"value33").unwrap();
        assert_eq!(&mem_table.get(b"key1").unwrap()[..], b"value11");
        assert_eq!(&mem_table.get(b"key2").unwrap()[..], b"value22");
        assert_eq!(&mem_table.get(b"key3").unwrap()[..], b"value33");
    }

    #[test]
    fn test_memtable_iter() {
        use std::ops::Bound;
        let memtable = MemTable::create(0);
        memtable.put(KeySlice::for_testing_from_slice_no_ts(b"key1"), b"value1").unwrap();
        memtable.put(KeySlice::for_testing_from_slice_no_ts(b"key2"), b"value2").unwrap();
        memtable.put(KeySlice::for_testing_from_slice_no_ts(b"key3"), b"value3").unwrap();

        {
            let mut iter = memtable.scan(Bound::Unbounded, Bound::Unbounded);
            assert_eq!(iter.key().key_ref(), b"key1");
            assert_eq!(iter.value(), b"value1");
            assert!(iter.is_valid());
            iter.next().unwrap();
            assert_eq!(iter.key().key_ref(), b"key2");
            assert_eq!(iter.value(), b"value2");
            assert!(iter.is_valid());
            iter.next().unwrap();
            assert_eq!(iter.key().key_ref(), b"key3");
            assert_eq!(iter.value(), b"value3");
            assert!(iter.is_valid());
            iter.next().unwrap();
            assert!(!iter.is_valid());
        }

        {
            let mut iter = memtable.scan(Bound::Included(KeySlice::for_testing_from_slice_no_ts(b"key1")), Bound::Included(KeySlice::for_testing_from_slice_no_ts(b"key2")));
            assert_eq!(iter.key().key_ref(), b"key1");
            assert_eq!(iter.value(), b"value1");
            assert!(iter.is_valid());
            iter.next().unwrap();
            assert_eq!(iter.key().key_ref(), b"key2");
            assert_eq!(iter.value(), b"value2");
            assert!(iter.is_valid());
            iter.next().unwrap();
            assert!(!iter.is_valid());
        }

        {
            let mut iter = memtable.scan(Bound::Excluded(KeySlice::for_testing_from_slice_no_ts(b"key1")), Bound::Excluded(KeySlice::for_testing_from_slice_no_ts(b"key3")));
            assert_eq!(iter.key().key_ref(), b"key2");
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
            let iter = memtable.scan(Bound::Excluded(KeySlice::for_testing_from_slice_no_ts(b"key1")), Bound::Excluded(KeySlice::for_testing_from_slice_no_ts(b"key3")));
            assert!(!iter.is_valid());
        }
        {
            let iter = memtable.scan(Bound::Included(KeySlice::for_testing_from_slice_no_ts(b"key1")), Bound::Included(KeySlice::for_testing_from_slice_no_ts(b"key2")));
            assert!(!iter.is_valid());
        }
        {
            let iter = memtable.scan(Bound::Unbounded, Bound::Unbounded);
            assert!(!iter.is_valid());
        }
    }
}