#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{atomic::AtomicBool, Arc},
};
use std::sync::atomic::Ordering;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;
use crate::common::Code::OK;
use crate::engines::lsm::iterators::fused_iterator::FusedIterator;
use crate::engines::lsm::iterators::lsm_iterator::LsmIterator;
use crate::engines::lsm::iterators::StorageIterator;
use crate::engines::lsm::iterators::two_merge_iterator::TwoMergeIterator;
use crate::engines::lsm::storage::{LsmStorageInner, WriteBatchRecord};
use crate::engines::lsm::utils::map_bound;

pub struct Transaction {
    /// 事务开始时的 ts
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    /// 事务新数据缓存
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    /// 事务是否已经被提交
    pub(crate) committed: Arc<AtomicBool>,
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.committed.load(Ordering::SeqCst){
            panic!("Transaction already committed!");
        }
        if let Some(entry) = self.local_storage.get(key) {
            if entry.value().is_empty() {
                return Ok(None);
            } else {
                return Ok(Some(entry.value().clone()));
            }
        }
        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        if self.committed.load(Ordering::SeqCst){
            panic!("Transaction already committed!");
        }
        let mut local_storage_iterator = TxnLocalIteratorBuilder{
            map: Arc::clone(&self.local_storage),
            iter_builder: |map| map.range((map_bound(lower), map_bound(upper))),
            item: (Bytes::new(), Bytes::new()),
        }.build();
        let entry = local_storage_iterator.with_iter_mut(|iter| TxnLocalIterator::entry_to_item(iter.next()));
        local_storage_iterator.with_mut(|x| *x.item = entry);

        TxnIterator::create(
            self.clone(),
            TwoMergeIterator::create(
                local_storage_iterator,
                self.inner.scan_with_ts(lower, upper, self.read_ts)?,
            )?
        )
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        if self.committed.load(Ordering::SeqCst){
            panic!("Transaction already committed!");
        }
        self.local_storage.insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        if self.committed.load(Ordering::SeqCst){
            panic!("Transaction already committed!");
        }
        self.local_storage.insert(Bytes::copy_from_slice(key), Bytes::new());
    }

    /// 提交事务
    pub fn commit(&self) -> Result<()> {
        // 设置提交状态 阻止其他操作
        self.committed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .expect("Transaction already committed!");
        let mut batch = Vec::new();
        for entry in self.local_storage.iter(){
            if entry.value().is_empty() {
                batch.push(WriteBatchRecord::Del(entry.key().clone()));
            } else {
                batch.push(WriteBatchRecord::Put(entry.key().clone(), entry.value().clone()))
            }
        }
        self.inner.write_batch(batch.as_slice())?;
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts)
    }
}

type SkipMapRangeIter<'a> =
crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// Txn本地提交数据缓存迭代器
#[self_referencing]
pub struct TxnLocalIterator {
    /// 存储事务内部的修改数据缓存
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// 当前的 KV-Pair
    item: (Bytes, Bytes),
}

impl TxnLocalIterator {
    fn entry_to_item(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (Bytes::new(), Bytes::new()))
    }
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn key(&self) -> &[u8] {
        &self.borrow_item().0[..]
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| TxnLocalIterator::entry_to_item(iter.next()));
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }
}

pub struct TxnIterator {
    _txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>
    ) -> Result<Self> {
        let mut iter = Self { _txn: txn, iter };
        iter.skip_deletes()?;
        Ok(iter)
    }

    fn skip_deletes(&mut self) -> Result<()> {
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.iter.next()?;
        }
        Ok(())
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a> = &'a [u8] where Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        self.skip_deletes()?;
        Ok(())
    }
}
