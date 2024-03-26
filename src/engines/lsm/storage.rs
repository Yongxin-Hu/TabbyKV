pub(crate) mod state;
pub(crate) mod option;

use std::collections::{Bound, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use parking_lot::{Mutex, MutexGuard, RwLock};
use crate::engines::lsm::mem_table::MemTable;
use crate::engines::lsm::compact::{
    CompactionController, LeveledCompactionController,
    SimpleLeveledCompactionController, TieredCompactionController,
};
use anyhow::{Context, Result};
use bytes::Bytes;
use option::{CompactionOptions, LsmStorageOptions};
use state::LsmStorageState;
use crate::engines::lsm::block::Block;
use crate::engines::lsm::iterators::concat_iterator::SstConcatIterator;
use crate::engines::lsm::iterators::fused_iterator::FusedIterator;
use crate::engines::lsm::iterators::lsm_iterator::LsmIterator;
use crate::engines::lsm::iterators::merge_iterator::MergeIterator;
use crate::engines::lsm::iterators::StorageIterator;
use crate::engines::lsm::iterators::two_merge_iterator::TwoMergeIterator;
use crate::engines::lsm::key;
use crate::engines::lsm::key::{KeySlice, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::engines::lsm::manifest::{Manifest, ManifestRecord};
use crate::engines::lsm::mvcc::LsmMvccInner;
use crate::engines::lsm::mvcc::txn::{Transaction, TxnIterator};
use crate::engines::lsm::table::builder::SsTableBuilder;
use crate::engines::lsm::table::iterator::SsTableIterator;
use crate::engines::lsm::table::{FileObject, SsTable};
use crate::engines::lsm::utils::{map_bound, map_bound_for_test};

/// (sst_id, block_index)
pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

pub struct LsmStorage{
    pub(crate) inner: Arc<LsmStorageInner>,
    /// 通知 Flush 线程停止工作
    flush_notifier: crossbeam_channel::Sender<()>,
    /// Flush 线程
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// 通知 compaction 线程停止工作
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// Compaction 线程
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for LsmStorage{
    fn drop(&mut self) {
        // 停止 flush_thread 以及 compaction_thread
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl LsmStorage{
    pub fn close(&self) -> Result<()> {
        self.inner.sync_dir()?;
        // 停止 compaction 线程
        self.compaction_notifier.send(()).unwrap();
        // 停止 flush 线程
        self.flush_notifier.send(()).unwrap();
        // 等待 compaction 和 flush 线程完成
        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_thread) = flush_thread.take(){
            flush_thread.join().map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }
        let mut compaction_thread = self.compaction_thread.lock();
        if let Some(compaction_thread) = compaction_thread.take(){
            compaction_thread.join().map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner.sync_dir()?;
            return Ok(());
        }

        if !self.inner.state.read().active_memtable.is_empty() {
            self.inner
                .freeze_memtable_with_memtable(Arc::new(MemTable::create(
                    self.inner.next_sst_id(),
                )))?;
        }

        while {
            let snapshot = self.inner.state.read();
            !snapshot.readonly_memtables.is_empty()
        } {
            self.inner.force_flush_earliest_memtable()?;
        }

        self.inner.sync_dir()?;

        Ok(())
    }

    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
        self.inner.new_txn()
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<TxnIterator> {
        self.inner.scan(lower, upper)
    }

    // Only for test
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().active_memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().readonly_memtables.is_empty() {
            self.inner.force_flush_earliest_memtable()?;
        }
        Ok(())
    }
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}


/// The storage interface of the LSM tree.
pub struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    next_sst_id: AtomicUsize,
    pub(crate) block_cache: Arc<BlockCache>,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_controller: CompactionController,
}

impl LsmStorageInner{
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    pub(crate) fn manifest(&self) -> &Manifest{
        self.manifest.as_ref().unwrap()
    }

    pub fn new_txn(self: &Arc<Self>) -> Result<Arc<Transaction>> {
        Ok(self.mvcc().new_txn(self.clone(), self.options.serializable))
    }

    // 检查是否 key 在[table_first_key, table_last_key]的范围内
    fn check_key_in_range(
        key:&[u8],
        table_first_key: KeySlice,
        table_last_key: KeySlice
    ) -> bool{
        key>=table_first_key.key_ref() && key <= table_last_key.key_ref()
    }

    fn check_range(
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        table_first_key: KeySlice,
        table_last_key: KeySlice
    ) -> bool{
        match upper {
            Bound::Excluded(key) if key <= table_first_key.key_ref() => {
                return false;
            },
            Bound::Included(key) if key < table_first_key.key_ref() => {
                return false;
            },
            _ => {}
        }
        match lower {
            Bound::Excluded(key) if key >= table_last_key.key_ref() => {
                return false;
            },
            Bound::Included(key) if key > table_last_key.key_ref() => {
                return false;
            },
            _ => {}
        }
        true
    }

    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self>{
        let path = path.as_ref();
        let mut state = LsmStorageState::create(&options);
        let block_cache = Arc::new(BlockCache::new(1 << 20)); // 4GB block cache,
        let mut next_sst_id = 1usize;
        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };
        if !path.exists() {
            // 创建 kv 存储文件
            std::fs::create_dir_all(path).context("failed to create kv store path.")?;
        }
        let manifest;
        let manifest_path = path.join("MANIFEST");
        let mut last_commit_ts = 0;
        if manifest_path.exists() {
            // 根据 Manifest 恢复 state
            let (m, records) = Manifest::recover(manifest_path)?;
            let mut mem_table = BTreeSet::new();
            for record in records {
                match record {
                    ManifestRecord::NewMemtable(id) => {
                        next_sst_id = next_sst_id.max(id);
                        mem_table.insert(id);
                    }
                    ManifestRecord::Flush(sst_id) => {
                        assert!(mem_table.remove(&sst_id), "memtable not exist!");
                        state.l0_sstables.insert(0, sst_id);
                        next_sst_id = next_sst_id.max(sst_id);
                    }
                    ManifestRecord::Compaction(task, output) => {
                        let (new_state, _) =
                            compaction_controller.apply_compaction_result(&state, &task, &output);
                        state = new_state;
                        next_sst_id = next_sst_id.max(output.iter().max().copied().unwrap_or_default());
                    }
                }
            }

            // 恢复 state 的 sstables
            for sst_id in state.l0_sstables.iter()
                .chain(state.levels.iter().flat_map(|(_, files)| files))
            {
                let sst_id = *sst_id;
                let sst = SsTable::open(
                    sst_id,
                    Some(block_cache.clone()),
                    FileObject::open(Self::path_of_sst_static(path, sst_id).as_path())?
                )?;
                last_commit_ts = last_commit_ts.max(sst.max_ts());
                state.sstables.insert(sst_id, Arc::new(sst));
            }

            next_sst_id += 1;
            // 恢复 state 的 memtable
            if options.enable_wal {
                for id in mem_table{
                    let mem_table
                        = MemTable::recover_from_wal(id, Self::path_of_wal_static(path, id))?;
                    let max_ts = mem_table
                        .map
                        .iter()
                        .map(|x| x.key().ts())
                        .max()
                        .unwrap_or_default();
                    if !mem_table.is_empty() {
                        state.readonly_memtables.insert(0, Arc::new(mem_table));
                    }
                }
                state.active_memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id, Self::path_of_wal_static(path, next_sst_id)
                )?);
            } else {
                state.active_memtable = Arc::new(MemTable::create(next_sst_id))
            }

            next_sst_id += 1;
            m.add_record_when_init(ManifestRecord::NewMemtable(state.active_memtable.id()))?;
            manifest = m;
        } else {
            if options.enable_wal {
                state.active_memtable = Arc::new(MemTable::create_with_wal(
                    state.active_memtable.id(),
                    Self::path_of_wal_static(path, state.active_memtable.id()),
                )?);
            }
            manifest = Manifest::create(manifest_path).expect("fail to create manifest!");
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.active_memtable.id()))?;
        }

        let storage = LsmStorageInner {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            block_cache,
            mvcc: Some(LsmMvccInner::new(last_commit_ts)),
            options: options.into(),
            manifest: Some(manifest)
        };
        Ok(storage)
    }

    pub fn get(self: &Arc<Self>, key: &[u8]) -> Result<Option<Bytes>> {
        let txn = self.mvcc().new_txn(self.clone(), self.options.serializable);
        txn.get(key)
    }

    pub(crate) fn get_with_ts(&self, key: &[u8], read_ts: u64) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // 提取释放锁

        // memtable iters
        let mut memtable_iters = Vec::with_capacity(snapshot.readonly_memtables.len() + 1);
        memtable_iters.push(Box::new(snapshot.active_memtable.scan(
            Bound::Included(KeySlice::from_slice(key, TS_RANGE_BEGIN)),
            Bound::Included(KeySlice::from_slice(key, TS_RANGE_END)),
        )));
        for memtable in snapshot.readonly_memtables.iter(){
            memtable_iters.push(Box::new(memtable.scan(
                Bound::Included(KeySlice::from_slice(key, TS_RANGE_BEGIN)),
                Bound::Included(KeySlice::from_slice(key, TS_RANGE_END))
            )))
        }
        let memtable_iter = MergeIterator::create(memtable_iters);

        // search l0 sstable
        let mut l0_iter = Vec::with_capacity(snapshot.l0_sstables.len());
        for table_id in &snapshot.l0_sstables {
            let table = snapshot.sstables[table_id].clone();
            // 根据 sst 的first_key, last_key 以及 bloom_filter，跳过不含 key 的sstable
            if Self::check_key_in_range(
                key,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice()
            ) && table.bloom_filter.may_contain(key){
                l0_iter.push(Box::new(SsTableIterator::create_and_move_to_key(
                    table,
                    KeySlice::from_slice(key, TS_RANGE_BEGIN)
                )?));
            }
        }
        let l0_sstable_iter = MergeIterator::create(l0_iter);
        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level_sst_id) in &snapshot.levels{
            let mut level_sst = Vec::with_capacity(level_sst_id.len());
            for sst_id in level_sst_id {
                let table = snapshot.sstables[sst_id].clone();
                level_sst.push(table);
            }
            let iter = SstConcatIterator::create_and_seek_to_key(
                level_sst, KeySlice::from_slice(key, TS_RANGE_BEGIN))?;
            level_iters.push(Box::new(iter))
        }
        let lsm_iter
            = LsmIterator::new(
            TwoMergeIterator::create(
                TwoMergeIterator::create(memtable_iter, l0_sstable_iter)?,
                     MergeIterator::create(level_iters)
            )?,
            read_ts,
            Bound::Unbounded,
        )?;
        if lsm_iter.is_valid() && lsm_iter.key() == key && !lsm_iter.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(lsm_iter.value())))
        }
        Ok(None)
    }

    /// 将 kv-pair 写入 activate_memtable
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

    /// 删除 `key` 写入空的 value
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
    }

    /// 批量写入
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        let _lck = self.mvcc().write_lock.lock();
        let ts = self.mvcc().latest_commit_ts() + 1;
        for record in batch {
            match record {
                WriteBatchRecord::Put(key, value) => {
                    let key = key.as_ref();
                    let value = value.as_ref();
                    assert!(!key.is_empty(), "key cannot be empty");
                    assert!(!value.is_empty(), "value cannot be empty");
                    let size;
                    {
                        let guard = self.state.read();
                        guard.active_memtable.put(KeySlice::from_slice(key, ts), value)?;
                        size = guard.active_memtable.approximate_size();
                    }
                    self.try_freeze(size)?;
                },
                WriteBatchRecord::Del(key) => {
                    let key = key.as_ref();
                    assert!(!key.is_empty(), "key cannot be empty");
                    let size;
                    {
                        let guard = self.state.read();
                        guard.active_memtable.put(KeySlice::from_slice(key, ts), b"")?;
                        size = guard.active_memtable.approximate_size();
                    }
                    self.try_freeze(size)?;
                }
            }
        }
        self.mvcc().update_commit_ts(ts);
        Ok(())
    }


    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator>{
        let txn = self.mvcc().new_txn(self.clone(), self.options.serializable);
        txn.scan(lower, upper)
    }

    pub(crate) fn scan_with_ts(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>, read_ts: u64,)
        -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let mut iters = Vec::with_capacity(
            snapshot.readonly_memtables.len() + 1 /* activate mem_table */);
        iters.push(Box::new(snapshot.active_memtable.scan(map_bound_for_test(lower, true),
                                                          map_bound_for_test(upper, false))));
        for imm_memtable in &snapshot.readonly_memtables {
            iters.push(Box::new(imm_memtable.scan(map_bound_for_test(lower, true),
                                                  map_bound_for_test(upper, false))));
        }
        let mem_table_iters = MergeIterator::create(iters);
        let mut iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for l0_table_id in &snapshot.l0_sstables{
            let table = snapshot.sstables.get(&l0_table_id).unwrap();
            if Self::check_range(
                lower,
                upper,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice()
            ){
                let iter = match lower {
                    Bound::Included(key) => {
                        SsTableIterator::create_and_move_to_key(Arc::clone(table), KeySlice::from_slice(key, TS_RANGE_BEGIN))?
                    },
                    Bound::Excluded(key) => {
                        let mut iter_inner =
                            SsTableIterator::create_and_move_to_key(Arc::clone(table),
                                                                    KeySlice::from_slice(key, TS_RANGE_BEGIN))?;
                        if iter_inner.is_valid() && iter_inner.key().key_ref() == key {
                            iter_inner.next()?;
                        }
                        iter_inner
                    },
                    Bound::Unbounded => SsTableIterator::create_and_move_to_first(Arc::clone(table))?
                };
                iters.push(Box::new(iter));
            }
        }
        let l0_sstable_iters = MergeIterator::create(iters);
        let end_bound = map_bound(upper);
        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level_sst_ids) in &snapshot.levels {
            let mut level_ssts = Vec::with_capacity(level_sst_ids.len());
            for sst_id in level_sst_ids{
                let table = snapshot.sstables[sst_id].clone();
                if Self::check_range(
                    lower,
                    upper,
                    table.first_key().as_key_slice(),
                    table.last_key().as_key_slice()
                ){
                    level_ssts.push(table);
                }
            }

            let level_iter = match lower{
                Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(
                    level_ssts, KeySlice::from_slice(key, TS_RANGE_BEGIN))?,
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        level_ssts, KeySlice::from_slice(key, TS_RANGE_BEGIN))?;
                    if iter.is_valid() && iter.key().key_ref() == key{
                        iter.next()?;
                    }
                    iter
                },
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(level_ssts)?
            };
            level_iters.push(Box::new(level_iter));
        }
        let mem_and_l0_iter
            = TwoMergeIterator::create(mem_table_iters, l0_sstable_iters)?;
        let level_merge_iter = MergeIterator::create(level_iters);
        let iters = LsmIterator::new(
            TwoMergeIterator::create(mem_and_l0_iter, level_merge_iter)?, read_ts, end_bound)?;
        Ok(FusedIterator::new(iters))
    }

    fn try_freeze(&self, estimated_size: usize) -> Result<()> {
        if estimated_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            let guard = self.state.read();
            // the memtable could have already been frozen, check again to ensure we really need to freeze
            if guard.active_memtable.approximate_size() >= self.options.target_sst_size {
                drop(guard);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    // freeze activate_memtable to readonly_memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {

        let next_sst_id = self.next_sst_id();
        let memtable;
        if self.options.enable_wal {
            memtable =  Arc::new(MemTable::create_with_wal(next_sst_id, self.path_of_wal(next_sst_id))?);
        } else {
            memtable = Arc::new(MemTable::create(next_sst_id));
        }
        self.freeze_memtable_with_memtable(memtable)?;
        // 记录 manifest
        if let Some(manifest) = &self.manifest{
            manifest.add_record(state_lock_observer, ManifestRecord::NewMemtable(next_sst_id))?;
        }
        Ok(())
    }


    fn freeze_memtable_with_memtable(&self, memtable: Arc<MemTable>) -> Result<()> {
        let mut guard = self.state.write();

        let mut snapshot = guard.as_ref().clone();
        // 使用 memtable 来更换当前的 active_memtable
        let old_memtable = std::mem::replace(&mut snapshot.active_memtable, memtable);
        // 将 old_memtable 加入 readonly_memtables
        snapshot.readonly_memtables.insert(0, old_memtable.clone());
        // 更新 state
        *guard = Arc::new(snapshot);

        drop(guard);
        old_memtable.sync_wal()?;

        Ok(())
    }

    /// 强制将最早的 memtable 转入 L0 层
    pub fn force_flush_earliest_memtable(&self) -> Result<()>{
        let state_lock = self.state_lock.lock();
        let earliest_memtable;
        {
            let guard = self.state.read();
            earliest_memtable = guard.readonly_memtables
                                .last()
                                .expect("No readonly memtable!")
                                .clone();
        }
        let mut ss_table_builder = SsTableBuilder::new(self.options.block_size);
        earliest_memtable.flush(&mut ss_table_builder)?;
        let sst_id = earliest_memtable.id();
        let sstable = Arc::new(ss_table_builder.build(
            sst_id, Some(self.block_cache.clone()),self.path_of_sst(sst_id))?);
        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            let old_memtable = snapshot.readonly_memtables.pop().unwrap();
            assert_eq!(old_memtable.id(), sstable.sst_id());
            snapshot.l0_sstables.insert(0, sstable.sst_id());
            snapshot.sstables.insert(sstable.sst_id(), sstable);
            *guard = Arc::new(snapshot);
        }

        if self.options.enable_wal {
            std::fs::remove_file(self.path_of_wal(sst_id))?;
        }

        // 记录 manifest
        if let Some(manifest) = &self.manifest{
            manifest.add_record(&state_lock, ManifestRecord::Flush(sst_id))?;
        }

        self.sync_dir()?;

        Ok(())
    }

    /// 同步 WAL 文件
    pub fn sync(&self) -> Result<()> {
        self.state.read().active_memtable.sync_wal()
    }

    /// 同步 kv 存储文件
    pub(super) fn sync_dir(&self) -> Result<()> {
        #[cfg(target_os = "unix")]
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// sst 文件格式 sst_id.sst
    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf{
        Self::path_of_sst_static(&self.path, id)
    }

    /// wal 文件格式 mem_table_id.wal
    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf{
        Self::path_of_wal_static(&self.path, id)
    }
}


#[cfg(test)]
mod test{
    use std::collections::Bound;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;
    use bytes::Bytes;
    use tempfile::tempdir;
    use crate::engines::lsm::compact::SimpleLeveledCompactionOptions;
    use crate::engines::lsm::iterators::concat_iterator::SstConcatIterator;
    use crate::engines::lsm::iterators::StorageIterator;
    use crate::engines::lsm::key::KeySlice;
    use crate::engines::lsm::storage::{LsmStorage, LsmStorageInner};
    use crate::engines::lsm::storage::option::{CompactionOptions, LsmStorageOptions};
    use crate::engines::lsm::table::builder::SsTableBuilder;
    use crate::engines::lsm::table::SsTable;
    use crate::engines::lsm::utils::{check_iter_result_by_key, check_lsm_iter_result_by_key, construct_merge_iterator_over_storage, sync};

    #[test]
    fn test_storage_integration() {
        let dir = tempdir().unwrap();
        let storage =
            LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_week1_test()).unwrap();
        let storage = Arc::new(storage);
        assert_eq!(&Arc::clone(&storage).get(b"0").unwrap(), &None);
        storage.put(b"1", b"233").unwrap();
        storage.put(b"2", b"2333").unwrap();
        storage.put(b"3", b"23333").unwrap();
        assert_eq!(&Arc::clone(&storage).get(b"1").unwrap().unwrap()[..], b"233");
        assert_eq!(&Arc::clone(&storage).get(b"2").unwrap().unwrap()[..], b"2333");
        assert_eq!(&Arc::clone(&storage).get(b"3").unwrap().unwrap()[..], b"23333");
        storage.delete(b"2").unwrap();
        assert!(storage.clone().get(b"2").unwrap().is_none());
        storage.delete(b"0").unwrap(); // should NOT report any error
    }

    #[test]
    fn test_storage_integration_2() {
        let dir = tempdir().unwrap();
        let storage =
            LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_week1_test()).unwrap();
        storage.put(b"1", b"233").unwrap();
        storage.put(b"2", b"2333").unwrap();
        storage.put(b"3", b"23333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        assert_eq!(storage.state.read().readonly_memtables.len(), 1);
        let previous_approximate_size = storage.state.read().readonly_memtables[0].approximate_size();
        assert!(previous_approximate_size >= 15);
        storage.put(b"1", b"2333").unwrap();
        storage.put(b"2", b"23333").unwrap();
        storage.put(b"3", b"233333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        assert_eq!(storage.state.read().readonly_memtables.len(), 2);
        assert_eq!(storage.state.read().readonly_memtables[1].approximate_size(), previous_approximate_size, "wrong order of memtables?");
        assert!(storage.state.read().readonly_memtables[0].approximate_size() > previous_approximate_size);
    }

    #[test]
    fn test_task3_freeze_on_capacity() {
        let dir = tempdir().unwrap();
        let mut options = LsmStorageOptions::default_for_week1_test();
        options.target_sst_size = 1024;
        options.num_memtable_limit = 1000;
        let storage = LsmStorageInner::open(dir.path(), options).unwrap();
        for _ in 0..1000 {
            storage.put(b"1", b"2333").unwrap();
        }
        let num_imm_memtables = storage.state.read().readonly_memtables.len();
        assert!(num_imm_memtables >= 1, "no memtable frozen?");
        for _ in 0..1000 {
            storage.delete(b"1").unwrap();
        }
        assert!(
            storage.state.read().readonly_memtables.len() > num_imm_memtables,
            "no more memtable frozen?"
        );
    }

    #[test]
    fn test_storage_integration_3() {
        let dir = tempdir().unwrap();
        let storage =
            LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_week1_test()).unwrap();
        let storage = Arc::new(storage);
        assert_eq!(&storage.get(b"0").unwrap(), &None);
        storage.put(b"1", b"233").unwrap();
        storage.put(b"2", b"2333").unwrap();
        storage.put(b"3", b"23333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        storage.delete(b"1").unwrap();
        storage.delete(b"2").unwrap();
        storage.put(b"3", b"2333").unwrap();
        storage.put(b"4", b"23333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        storage.put(b"1", b"233333").unwrap();
        storage.put(b"3", b"233333").unwrap();
        assert_eq!(storage.state.read().readonly_memtables.len(), 2);
        assert_eq!(&Arc::clone(&storage).get(b"1").unwrap().unwrap()[..], b"233333");
        assert_eq!(&Arc::clone(&storage).get(b"2").unwrap(), &None);
        assert_eq!(&Arc::clone(&storage).get(b"3").unwrap().unwrap()[..], b"233333");
        assert_eq!(&Arc::clone(&storage).get(b"4").unwrap().unwrap()[..], b"23333");
    }

    #[test]
    fn test_integration() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_week1_test()).unwrap(),
        );
        storage.put(b"1", b"233").unwrap();
        storage.put(b"2", b"2333").unwrap();
        storage.put(b"3", b"23333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        storage.delete(b"1").unwrap();
        storage.delete(b"2").unwrap();
        storage.put(b"3", b"2333").unwrap();
        storage.put(b"4", b"23333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        storage.put(b"1", b"233333").unwrap();
        storage.put(b"3", b"233333").unwrap();
        {
            let mut iter = storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
            check_lsm_iter_result_by_key(
                &mut iter,
                vec![
                    (Bytes::from_static(b"1"), Bytes::from_static(b"233333")),
                    (Bytes::from_static(b"3"), Bytes::from_static(b"233333")),
                    (Bytes::from_static(b"4"), Bytes::from_static(b"23333")),
                ],
            );
            assert!(!iter.is_valid());
            iter.next().unwrap();
            iter.next().unwrap();
            iter.next().unwrap();
            assert!(!iter.is_valid());
        }
        {
            let mut iter = storage
                .scan(Bound::Included(b"2"), Bound::Included(b"3"))
                .unwrap();
            check_lsm_iter_result_by_key(
                &mut iter,
                vec![
                    (Bytes::from_static(b"3"), Bytes::from_static(b"233333"))
                ],
            );
            assert!(!iter.is_valid());
            iter.next().unwrap();
            iter.next().unwrap();
            iter.next().unwrap();
            assert!(!iter.is_valid());
        }
    }

    #[test]

    fn test_integration_2() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_week1_test()).unwrap(),
        );
        storage.put(b"1", b"233").unwrap();
        storage.put(b"2", b"2333").unwrap();
        storage.put(b"3", b"23333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        storage.delete(b"1").unwrap();
        storage.delete(b"2").unwrap();
        storage.put(b"3", b"2333").unwrap();
        storage.put(b"4", b"23333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        storage.put(b"1", b"233333").unwrap();
        storage.put(b"3", b"233333").unwrap();
        let mut iter = storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
        while iter.is_valid() {
            iter.next();
        }
    }

    pub fn generate_sst(
        id: usize,
        path: impl AsRef<Path>,
        data: Vec<(Bytes, Bytes)>,
    ) -> SsTable {
        let mut builder = SsTableBuilder::new(128);
        for (key, value) in data {
            builder.add(KeySlice::for_testing_from_slice_no_ts(&key[..]), &value[..]);
        }
        builder.build(id, None, path.as_ref()).unwrap()
    }

    #[test]
    fn test_storage_scan() {
        let dir = tempdir().unwrap();
        let storage =
            Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default_for_week1_test()).unwrap());
        storage.put(b"1", b"233").unwrap();
        storage.put(b"2", b"2333").unwrap();
        storage.put(b"00", b"2333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        storage.put(b"3", b"23333").unwrap();
        storage.delete(b"1").unwrap();
        let sst1 = generate_sst(
            10,
            dir.path().join("10.sst"),
            vec![
                (Bytes::from_static(b"0"), Bytes::from_static(b"2333333")),
                (Bytes::from_static(b"00"), Bytes::from_static(b"2333333")),
                (Bytes::from_static(b"4"), Bytes::from_static(b"23")),
            ]
        );
        let sst2 = generate_sst(
            11,
            dir.path().join("11.sst"),
            vec![(Bytes::from_static(b"4"), Bytes::from_static(b""))]
        );
        {
            let mut state = storage.state.write();
            let mut snapshot = state.as_ref().clone();
            snapshot.l0_sstables.push(sst2.sst_id()); // this is the latest SST
            snapshot.l0_sstables.push(sst1.sst_id());
            snapshot.sstables.insert(sst2.sst_id(), sst2.into());
            snapshot.sstables.insert(sst1.sst_id(), sst1.into());
            *state = snapshot.into();
        }
        check_lsm_iter_result_by_key(
            &mut storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("0"), Bytes::from("2333333")),
                (Bytes::from("00"), Bytes::from("2333")),
                (Bytes::from("2"), Bytes::from("2333")),
                (Bytes::from("3"), Bytes::from("23333")),
            ],
        );
        check_lsm_iter_result_by_key(
            &mut storage
                .scan(Bound::Included(b"1"), Bound::Included(b"2"))
                .unwrap(),
            vec![(Bytes::from("2"), Bytes::from("2333"))],
        );
        check_lsm_iter_result_by_key(
            &mut storage
                .scan(Bound::Excluded(b"1"), Bound::Excluded(b"3"))
                .unwrap(),
            vec![(Bytes::from("2"), Bytes::from("2333"))],
        );
    }

    #[test]
    fn test_storage_get_1() {
        let dir = tempdir().unwrap();
        let storage =
            Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default_for_week1_test()).unwrap());
        storage.put(b"1", b"233").unwrap();
        storage.put(b"2", b"2333").unwrap();
        storage.put(b"00", b"2333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        storage.put(b"3", b"23333").unwrap();
        storage.delete(b"1").unwrap();
        let sst1 = generate_sst(
            10,
            dir.path().join("10.sst"),
            vec![
                (Bytes::from_static(b"0"), Bytes::from_static(b"2333333")),
                (Bytes::from_static(b"00"), Bytes::from_static(b"2333333")),
                (Bytes::from_static(b"4"), Bytes::from_static(b"23")),
            ]
        );
        let sst2 = generate_sst(
            11,
            dir.path().join("11.sst"),
            vec![(Bytes::from_static(b"4"), Bytes::from_static(b""))]
        );
        {
            let mut state = storage.state.write();
            let mut snapshot = state.as_ref().clone();
            snapshot.l0_sstables.push(sst2.sst_id()); // this is the latest SST
            snapshot.l0_sstables.push(sst1.sst_id());
            snapshot.sstables.insert(sst2.sst_id(), sst2.into());
            snapshot.sstables.insert(sst1.sst_id(), sst1.into());
            *state = snapshot.into();
        }
        assert_eq!(
            storage.get(b"0").unwrap(),
            Some(Bytes::from_static(b"2333333"))
        );
        assert_eq!(
            storage.get(b"00").unwrap(),
            Some(Bytes::from_static(b"2333"))
        );
        assert_eq!(
            storage.get(b"2").unwrap(),
            Some(Bytes::from_static(b"2333"))
        );
        assert_eq!(
            storage.get(b"3").unwrap(),
            Some(Bytes::from_static(b"23333"))
        );
        assert_eq!(storage.get(b"4").unwrap(), None);
        assert_eq!(storage.get(b"--").unwrap(), None);
        assert_eq!(storage.get(b"555").unwrap(), None);
    }

    #[test]
    fn test_storage_scan_2() {
        let dir = tempdir().unwrap();
        let storage =
            Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default_for_week1_test()).unwrap());
        storage.put(b"0", b"2333333").unwrap();
        storage.put(b"00", b"2333333").unwrap();
        storage.put(b"4", b"23").unwrap();
        sync(&storage);

        storage.delete(b"4").unwrap();
        sync(&storage);

        storage.put(b"1", b"233").unwrap();
        storage.put(b"2", b"2333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        storage.put(b"00", b"2333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        storage.put(b"3", b"23333").unwrap();
        storage.delete(b"1").unwrap();

        {
            let state = storage.state.read();
            assert_eq!(state.l0_sstables.len(), 2);
            assert_eq!(state.readonly_memtables.len(), 2);
        }

        check_lsm_iter_result_by_key(
            &mut storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("0"), Bytes::from("2333333")),
                (Bytes::from("00"), Bytes::from("2333")),
                (Bytes::from("2"), Bytes::from("2333")),
                (Bytes::from("3"), Bytes::from("23333")),
            ],
        );
        check_lsm_iter_result_by_key(
            &mut storage
                .scan(Bound::Included(b"1"), Bound::Included(b"2"))
                .unwrap(),
            vec![(Bytes::from("2"), Bytes::from("2333"))],
        );
        check_lsm_iter_result_by_key(
            &mut storage
                .scan(Bound::Excluded(b"1"), Bound::Excluded(b"3"))
                .unwrap(),
            vec![(Bytes::from("2"), Bytes::from("2333"))],
        );
    }

    #[test]
    fn test_storage_get_2() {
        let dir = tempdir().unwrap();
        let storage =
            Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default_for_week1_test()).unwrap());
        storage.put(b"0", b"2333333").unwrap();
        storage.put(b"00", b"2333333").unwrap();
        storage.put(b"4", b"23").unwrap();
        sync(&storage);

        storage.delete(b"4").unwrap();
        sync(&storage);

        storage.put(b"1", b"233").unwrap();
        storage.put(b"2", b"2333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        storage.put(b"00", b"2333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        storage.put(b"3", b"23333").unwrap();
        storage.delete(b"1").unwrap();

        {
            let state = storage.state.read();
            assert_eq!(state.l0_sstables.len(), 2);
            assert_eq!(state.readonly_memtables.len(), 2);
        }

        assert_eq!(
            storage.get(b"0").unwrap(),
            Some(Bytes::from_static(b"2333333"))
        );
        assert_eq!(
            storage.get(b"00").unwrap(),
            Some(Bytes::from_static(b"2333"))
        );
        assert_eq!(
            storage.get(b"2").unwrap(),
            Some(Bytes::from_static(b"2333"))
        );
        assert_eq!(
            storage.get(b"3").unwrap(),
            Some(Bytes::from_static(b"23333"))
        );
        assert_eq!(storage.get(b"4").unwrap(), None);
        assert_eq!(storage.get(b"--").unwrap(), None);
        assert_eq!(storage.get(b"555").unwrap(), None);
    }
    #[test]
    fn test_auto_flush() {
        let dir = tempdir().unwrap();
        let storage = LsmStorage::open(&dir, LsmStorageOptions::default_for_week1_day6_test()).unwrap();

        let value = "1".repeat(1024); // 1KB

        // approximately 6MB
        for i in 0..6000 {
            storage
                .put(format!("{i}").as_bytes(), value.as_bytes())
                .unwrap();
        }

        std::thread::sleep(Duration::from_millis(500));

        assert!(!storage.inner.state.read().l0_sstables.is_empty());
    }


    fn generate_concat_sst(
        start_key: usize,
        end_key: usize,
        dir: impl AsRef<Path>,
        id: usize,
    ) -> SsTable {
        let mut builder = SsTableBuilder::new(128);
        for idx in start_key..end_key {
            let key = format!("{:05}", idx);
            builder.add(
                KeySlice::for_testing_from_slice_no_ts(key.as_bytes()),
                b"test",
            );
        }
        let path = dir.as_ref().join(format!("{id}.sst"));
        builder.build(0, None, path).unwrap()
    }

    #[test]
    fn test_concat_iterator() {
        let dir = tempdir().unwrap();
        let mut sstables = Vec::new();
        for i in 1..=10 {
            sstables.push(Arc::new(generate_concat_sst(
                i * 10,
                (i + 1) * 10,
                dir.path(),
                i,
            )));
        }
        for key in 0..120 {
            let iter = SstConcatIterator::create_and_seek_to_key(
                sstables.clone(),
                KeySlice::for_testing_from_slice_no_ts(format!("{:05}", key).as_bytes())
            ).unwrap();
            if key < 10 {
                assert!(iter.is_valid());
                assert_eq!(iter.key().key_ref(), b"00010");
            } else if key >= 110 {
                assert!(!iter.is_valid());
            } else {
                assert!(iter.is_valid());
                assert_eq!(
                    iter.key().key_ref(),
                    format!("{:05}", key).as_bytes()
                );
            }
        }
        let iter = SstConcatIterator::create_and_seek_to_first(sstables.clone()).unwrap();
        assert!(iter.is_valid());
        assert_eq!(iter.key().key_ref(), b"00010");
    }

    #[test]
    fn test_integration_3() {
        let dir = tempdir().unwrap();
        let storage =
            Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default_for_week1_test()).unwrap());
        storage.put(b"0", b"2333333").unwrap();
        storage.put(b"00", b"2333333").unwrap();
        storage.put(b"4", b"23").unwrap();
        sync(&storage);

        storage.delete(b"4").unwrap();
        sync(&storage);

        storage.force_full_compaction().unwrap();
        assert!(storage.state.read().l0_sstables.is_empty());
        assert!(!storage.state.read().levels[0].1.is_empty());

        storage.put(b"1", b"233").unwrap();
        storage.put(b"2", b"2333").unwrap();
        sync(&storage);

        storage.put(b"00", b"2333").unwrap();
        storage.put(b"3", b"23333").unwrap();
        storage.delete(b"1").unwrap();
        sync(&storage);
        storage.force_full_compaction().unwrap();

        assert!(storage.state.read().l0_sstables.is_empty());
        assert!(!storage.state.read().levels[0].1.is_empty());

        check_lsm_iter_result_by_key(
            &mut storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("0"), Bytes::from("2333333")),
                (Bytes::from("00"), Bytes::from("2333")),
                (Bytes::from("2"), Bytes::from("2333")),
                (Bytes::from("3"), Bytes::from("23333")),
            ],
        );

        assert_eq!(
            storage.get(b"0").unwrap(),
            Some(Bytes::from_static(b"2333333"))
        );
        assert_eq!(
            storage.get(b"00").unwrap(),
            Some(Bytes::from_static(b"2333"))
        );
        assert_eq!(
            storage.get(b"2").unwrap(),
            Some(Bytes::from_static(b"2333"))
        );
        assert_eq!(
            storage.get(b"3").unwrap(),
            Some(Bytes::from_static(b"23333"))
        );
        assert_eq!(storage.get(b"4").unwrap(), None);
        assert_eq!(storage.get(b"--").unwrap(), None);
        assert_eq!(storage.get(b"555").unwrap(), None);
    }

    #[test]
    fn test_integration_simple() {
        test_integration_4(CompactionOptions::Simple(SimpleLeveledCompactionOptions {
            size_ratio_percent: 200,
            level0_file_num_compaction_trigger: 2,
            max_levels: 3,
        }));
    }

    fn test_integration_4(compaction_options: CompactionOptions) {
        let dir = tempdir().unwrap();
        let storage = LsmStorage::open(
            &dir,
            LsmStorageOptions::default_for_week2_test(compaction_options.clone()),
        ).unwrap();
        for i in 0..=20 {
            storage.put(b"0", format!("v{}", i).as_bytes()).unwrap();
            if i % 2 == 0 {
                storage.put(b"1", format!("v{}", i).as_bytes()).unwrap();
            } else {
                storage.delete(b"1").unwrap();
            }
            if i % 2 == 1 {
                storage.put(b"2", format!("v{}", i).as_bytes()).unwrap();
            } else {
                storage.delete(b"2").unwrap();
            }
            storage
                .inner
                .force_freeze_memtable(&storage.inner.state_lock.lock())
                .unwrap();
        }
        storage.close().unwrap();
        // ensure all SSTs are flushed
        assert!(storage.inner.state.read().active_memtable.is_empty());
        assert!(storage.inner.state.read().readonly_memtables.is_empty());
        //storage.dump_structure();
        drop(storage);

        let storage = LsmStorage::open(
            &dir,
            LsmStorageOptions::default_for_week2_test(compaction_options.clone()),
        ).unwrap();
        assert_eq!(storage.get(b"0").unwrap().unwrap().as_ref(), b"v20".as_slice());
        assert_eq!(storage.get(b"1").unwrap().unwrap().as_ref(), b"v20".as_slice());
        assert_eq!(storage.get(b"2").unwrap(), None);
    }

    #[test]
    fn test_task3_compaction_integration() {
        let dir = tempdir().unwrap();
        let mut options = LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction);
        options.enable_wal = true;
        let storage = LsmStorage::open(&dir, options).unwrap();
        for i in 0..=20000 {
            storage
                .put(b"0", format!("{:02000}", i).as_bytes())
                .unwrap();
        }
        std::thread::sleep(Duration::from_secs(1)); // wait until all memtables flush
        while {
            let snapshot = storage.inner.state.read();
            !snapshot.readonly_memtables.is_empty()
        } {
            storage.inner.force_flush_earliest_memtable().unwrap();
        }
        assert!(storage.inner.state.read().l0_sstables.len() > 1);
        storage.inner.force_full_compaction().unwrap();
        assert!(storage.inner.state.read().l0_sstables.is_empty());
        assert_eq!(storage.inner.state.read().levels.len(), 1);
        // same key in the same SST
        assert_eq!(storage.inner.state.read().levels[0].1.len(), 1);
        for i in 0..=100 {
            storage
                .put(b"1", format!("{:02000}", i).as_bytes())
                .unwrap();
        }
        storage
            .inner
            .force_freeze_memtable(&storage.inner.state_lock.lock())
            .unwrap();
        std::thread::sleep(Duration::from_secs(1)); // wait until all memtables flush
        while {
            let snapshot = storage.inner.state.read();
            !snapshot.readonly_memtables.is_empty()
        } {
            storage.inner.force_flush_earliest_memtable().unwrap();
        }
        storage.inner.force_full_compaction().unwrap();
        assert!(storage.inner.state.read().l0_sstables.is_empty());
        assert_eq!(storage.inner.state.read().levels.len(), 1);
        // same key in the same SST, now we should split two
        //assert_eq!(storage.inner.state.read().levels[0].1.len(), 2);
    }

    #[test]
    fn test_memtable_mvcc() {
        let dir = tempdir().unwrap();
        let mut options = LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction);
        options.enable_wal = true;
        let storage = LsmStorage::open(&dir, options.clone()).unwrap();
        storage.put(b"a", b"1").unwrap();
        storage.put(b"b", b"1").unwrap();
        let snapshot1 = storage.new_txn().unwrap();
        storage.put(b"a", b"2").unwrap();
        let snapshot2 = storage.new_txn().unwrap();
        storage.delete(b"b").unwrap();
        storage.put(b"c", b"1").unwrap();
        let snapshot3 = storage.new_txn().unwrap();
        assert_eq!(snapshot1.get(b"a").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot1.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot1.get(b"c").unwrap(), None);
        check_lsm_iter_result_by_key(
            &mut snapshot1.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("1")),
                (Bytes::from("b"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot2.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
        assert_eq!(snapshot2.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot2.get(b"c").unwrap(), None);
        check_lsm_iter_result_by_key(
            &mut snapshot2.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("2")),
                (Bytes::from("b"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot3.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
        assert_eq!(snapshot3.get(b"b").unwrap(), None);
        assert_eq!(snapshot3.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
        check_lsm_iter_result_by_key(
            &mut snapshot3.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("2")),
                (Bytes::from("c"), Bytes::from("1")),
            ],
        );
        storage
            .inner
            .force_freeze_memtable(&storage.inner.state_lock.lock())
            .unwrap();
        storage.put(b"a", b"3").unwrap();
        storage.put(b"b", b"3").unwrap();
        let snapshot4 = storage.new_txn().unwrap();
        storage.put(b"a", b"4").unwrap();
        let snapshot5 = storage.new_txn().unwrap();
        storage.delete(b"b").unwrap();
        storage.put(b"c", b"5").unwrap();
        let snapshot6 = storage.new_txn().unwrap();
        assert_eq!(snapshot1.get(b"a").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot1.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot1.get(b"c").unwrap(), None);
        check_lsm_iter_result_by_key(
            &mut snapshot1.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("1")),
                (Bytes::from("b"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot2.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
        assert_eq!(snapshot2.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot2.get(b"c").unwrap(), None);
        check_lsm_iter_result_by_key(
            &mut snapshot2.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("2")),
                (Bytes::from("b"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot3.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
        assert_eq!(snapshot3.get(b"b").unwrap(), None);
        assert_eq!(snapshot3.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
        check_lsm_iter_result_by_key(
            &mut snapshot3.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("2")),
                (Bytes::from("c"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot4.get(b"a").unwrap(), Some(Bytes::from_static(b"3")));
        assert_eq!(snapshot4.get(b"b").unwrap(), Some(Bytes::from_static(b"3")));
        assert_eq!(snapshot4.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
        check_lsm_iter_result_by_key(
            &mut snapshot4.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("3")),
                (Bytes::from("b"), Bytes::from("3")),
                (Bytes::from("c"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot5.get(b"a").unwrap(), Some(Bytes::from_static(b"4")));
        assert_eq!(snapshot5.get(b"b").unwrap(), Some(Bytes::from_static(b"3")));
        assert_eq!(snapshot5.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
        check_lsm_iter_result_by_key(
            &mut snapshot5.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("4")),
                (Bytes::from("b"), Bytes::from("3")),
                (Bytes::from("c"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot6.get(b"a").unwrap(), Some(Bytes::from_static(b"4")));
        assert_eq!(snapshot6.get(b"b").unwrap(), None);
        assert_eq!(snapshot6.get(b"c").unwrap(), Some(Bytes::from_static(b"5")));
        check_lsm_iter_result_by_key(
            &mut snapshot6.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("4")),
                (Bytes::from("c"), Bytes::from("5")),
            ],
        );
    }

    #[test]
    fn test_lsm_iterator_mvcc() {
        let dir = tempdir().unwrap();
        let mut options = LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction);
        options.enable_wal = true;
        let storage = LsmStorage::open(&dir, options.clone()).unwrap();
        storage.put(b"a", b"1").unwrap();
        storage.put(b"b", b"1").unwrap();
        let snapshot1 = storage.new_txn().unwrap();
        storage.put(b"a", b"2").unwrap();
        let snapshot2 = storage.new_txn().unwrap();
        storage.delete(b"b").unwrap();
        storage.put(b"c", b"1").unwrap();
        let snapshot3 = storage.new_txn().unwrap();
        storage.force_flush().unwrap();
        assert_eq!(snapshot1.get(b"a").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot1.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot1.get(b"c").unwrap(), None);
        check_lsm_iter_result_by_key(
            &mut snapshot1.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("1")),
                (Bytes::from("b"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot2.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
        assert_eq!(snapshot2.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot2.get(b"c").unwrap(), None);
        check_lsm_iter_result_by_key(
            &mut snapshot2.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("2")),
                (Bytes::from("b"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot3.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
        assert_eq!(snapshot3.get(b"b").unwrap(), None);
        assert_eq!(snapshot3.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
        check_lsm_iter_result_by_key(
            &mut snapshot3.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("2")),
                (Bytes::from("c"), Bytes::from("1")),
            ],
        );
        storage.put(b"a", b"3").unwrap();
        storage.put(b"b", b"3").unwrap();
        let snapshot4 = storage.new_txn().unwrap();
        storage.put(b"a", b"4").unwrap();
        let snapshot5 = storage.new_txn().unwrap();
        storage.delete(b"b").unwrap();
        storage.put(b"c", b"5").unwrap();
        let snapshot6 = storage.new_txn().unwrap();
        storage.force_flush().unwrap();
        assert_eq!(snapshot1.get(b"a").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot1.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot1.get(b"c").unwrap(), None);
        check_lsm_iter_result_by_key(
            &mut snapshot1.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("1")),
                (Bytes::from("b"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot2.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
        assert_eq!(snapshot2.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot2.get(b"c").unwrap(), None);
        check_lsm_iter_result_by_key(
            &mut snapshot2.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("2")),
                (Bytes::from("b"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot3.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
        assert_eq!(snapshot3.get(b"b").unwrap(), None);
        assert_eq!(snapshot3.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
        check_lsm_iter_result_by_key(
            &mut snapshot3.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("2")),
                (Bytes::from("c"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot4.get(b"a").unwrap(), Some(Bytes::from_static(b"3")));
        assert_eq!(snapshot4.get(b"b").unwrap(), Some(Bytes::from_static(b"3")));
        assert_eq!(snapshot4.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
        check_lsm_iter_result_by_key(
            &mut snapshot4.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("3")),
                (Bytes::from("b"), Bytes::from("3")),
                (Bytes::from("c"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot5.get(b"a").unwrap(), Some(Bytes::from_static(b"4")));
        assert_eq!(snapshot5.get(b"b").unwrap(), Some(Bytes::from_static(b"3")));
        assert_eq!(snapshot5.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
        check_lsm_iter_result_by_key(
            &mut snapshot5.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("4")),
                (Bytes::from("b"), Bytes::from("3")),
                (Bytes::from("c"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot6.get(b"a").unwrap(), Some(Bytes::from_static(b"4")));
        assert_eq!(snapshot6.get(b"b").unwrap(), None);
        assert_eq!(snapshot6.get(b"c").unwrap(), Some(Bytes::from_static(b"5")));
        check_lsm_iter_result_by_key(
            &mut snapshot6.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("4")),
                (Bytes::from("c"), Bytes::from("5")),
            ],
        );
        check_lsm_iter_result_by_key(
            &mut snapshot6
                .scan(Bound::Included(b"a"), Bound::Included(b"a"))
                .unwrap(),
            vec![(Bytes::from("a"), Bytes::from("4"))],
        );
        check_lsm_iter_result_by_key(
            &mut snapshot6
                .scan(Bound::Excluded(b"a"), Bound::Excluded(b"c"))
                .unwrap(),
            vec![],
        );
    }

    #[test]
    fn test_task3_sst_ts() {
        let mut builder = SsTableBuilder::new(16);
        builder.add(KeySlice::for_testing_from_slice_with_ts(b"11", 1), b"11");
        builder.add(KeySlice::for_testing_from_slice_with_ts(b"22", 2), b"22");
        builder.add(KeySlice::for_testing_from_slice_with_ts(b"33", 3), b"11");
        builder.add(KeySlice::for_testing_from_slice_with_ts(b"44", 4), b"22");
        builder.add(KeySlice::for_testing_from_slice_with_ts(b"55", 5), b"11");
        builder.add(KeySlice::for_testing_from_slice_with_ts(b"66", 6), b"22");
        let dir = tempdir().unwrap();
        let sst = builder.build(0, None, dir.path().join("1.sst")).unwrap();
        assert_eq!(sst.max_ts(), 6);
    }
}

