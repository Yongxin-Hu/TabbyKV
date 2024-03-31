#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub mod txn;
pub mod watermark;

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};
use std::sync::atomic::AtomicBool;
use crossbeam_skiplist::SkipMap;

use parking_lot::Mutex;
use crate::engines::lsm::mvcc::txn::Transaction;
use crate::engines::lsm::mvcc::watermark::Watermark;
use crate::engines::lsm::storage::LsmStorageInner;

pub(crate) struct CommittedTxnData {
    pub(crate) key_hashes: HashSet<u32>,
    #[allow(dead_code)]
    pub(crate) read_ts: u64,
    #[allow(dead_code)]
    pub(crate) commit_ts: u64,
}

pub(crate) struct LsmMvccInner {
    pub(crate) write_lock: Mutex<()>,
    pub(crate) commit_lock: Mutex<()>,
    pub(crate) ts: Arc<Mutex<(u64, Watermark)>>,
    pub(crate) committed_txns: Arc<Mutex<BTreeMap<u64, CommittedTxnData>>>,
}

impl LsmMvccInner {
    pub fn new(initial_ts: u64) -> Self {
        Self {
            write_lock: Mutex::new(()),
            commit_lock: Mutex::new(()),
            ts: Arc::new(Mutex::new((initial_ts, Watermark::new()))),
            committed_txns: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// 最新提交的 time_stamp
    pub fn latest_commit_ts(&self) -> u64 {
        self.ts.lock().0
    }

    pub fn update_commit_ts(&self, ts: u64) {
        self.ts.lock().0 = ts;
    }

    /// 可以回收 time_stamp 小于此 time_stamp 的 key
    pub fn watermark(&self) -> u64 {
        let ts = self.ts.lock();
        ts.1.watermark().unwrap_or(ts.0)
    }

    /// 创建一个事务
    /// # 参数
    /// * inner: LsmStorageInner
    /// * serializable:
    /// # 返回值
    /// * Transaction
    pub fn new_txn(&self, inner: Arc<LsmStorageInner>, serializable: bool) -> Arc<Transaction> {
        let mut ts = self.ts.lock();
        let read_ts = ts.0;
        ts.1.add_reader(read_ts);
        Arc::new(Transaction {
            inner,
            read_ts,
            local_storage: Arc::new(SkipMap::new()),
            committed: Arc::new(AtomicBool::new(false)),
            key_hashes: None,
        })
    }
}
