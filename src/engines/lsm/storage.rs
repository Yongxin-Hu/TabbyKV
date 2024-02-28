pub(crate) mod state;
mod option;

use std::collections::Bound;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use parking_lot::{Mutex, MutexGuard, RwLock};
use crate::engines::lsm::mem_table::MemTable;
use crate::engines::lsm::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController,
    SimpleLeveledCompactionController, TieredCompactionController,
};
use anyhow::{Context, Result};
use bytes::Bytes;
use option::LsmStorageOptions;
use state::LsmStorageState;
use crate::engines::lsm::iterators::fused_iterator::FusedIterator;
use crate::engines::lsm::iterators::lsm_iterator::LsmIterator;
use crate::engines::lsm::iterators::merge_iterator::MergeIterator;
use crate::engines::lsm::iterators::StorageIterator;
use crate::engines::lsm::iterators::two_merge_iterator::TwoMergeIterator;
use crate::engines::lsm::table::builder::SsTableBuilder;
use crate::engines::lsm::table::iterator::SsTableIterator;
use crate::engines::lsm::utils::map_bound;


struct LsmStorage{
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working.
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread.
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working.
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread.
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for LsmStorage{
    fn drop(&mut self) {
        // 停止 flush_thread 以及 compaction_thread
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).unwrap();
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
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }
}

/// The storage interface of the LSM tree.
pub struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
}

impl LsmStorageInner{
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    // 检查是否 key 在[table_first_key, table_last_key]的范围内
    fn check_key_in_range(
        key:Bytes,
        table_first_key: Bytes,
        table_last_key: Bytes
    ) -> bool{
        key>=table_first_key && key <= table_last_key
    }

    fn check_range(
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        table_first_key: Bytes,
        table_last_key: Bytes
    ) -> bool{
        match upper {
            Bound::Excluded(key) if key <= table_first_key => {
                return false;
            },
            Bound::Included(key) if key < table_first_key => {
                return false;
            },
            _ => {}
        }
        match lower {
            Bound::Excluded(key) if key >= table_last_key => {
                return false;
            },
            Bound::Included(key) if key > table_last_key => {
                return false;
            },
            _ => {}
        }
        true
    }

    // TODO
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self>{
        let path = path.as_ref();
        if !path.exists() {
            // 创建 kv 存储文件
            std::fs::create_dir_all(path).context("failed to create kv store path.")?;
        }
        let state = LsmStorageState::create(&options);
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

        let storage = LsmStorageInner {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            options: options.into(),
        };
        Ok(storage)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // 提取释放锁

        // search active_memtable
        if let Some(value) = snapshot.active_memtable.get(key){
            if value.len() == 0 { return Ok(None)}
            return Ok(Some(value))
        }

        // search readonly_memtables
        for memtable in snapshot.readonly_memtables.iter(){
            if let Some(value) = memtable.get(key){
                if value.len() == 0 { return Ok(None)}
                return Ok(Some(value))
            }
        }

        // search l0 sstable
        let mut l0_iter = Vec::with_capacity(snapshot.l0_sstables.len());
        for table_id in &snapshot.l0_sstables {
            let table = snapshot.sstables[table_id].clone();
            // 跳过不含 key 的sstable
            if Self::check_key_in_range(
                Bytes::copy_from_slice(key),
                table.first_key(),
                table.last_key()
            ) && table.bloom_filter.may_contain(key){
                l0_iter.push(Box::new(SsTableIterator::create_and_seek_to_key(
                    table,
                    Bytes::copy_from_slice(key)
                )?));
            }
        }
        let l0_sstable_iter = MergeIterator::create(l0_iter);
        if l0_sstable_iter.is_valid() && l0_sstable_iter.key() == key && !l0_sstable_iter.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(l0_sstable_iter.value())))
        }
        Ok(None)
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!value.is_empty(), "value cannot be empty");
        assert!(!key.is_empty(), "key cannot be empty");
        let size;
        {
            let guard = self.state.read();
            guard.active_memtable.put(key, value)?;
            size = guard.active_memtable.approximate_size();
        }
        self.try_freeze(size)?;
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        assert!(!key.is_empty(), "key cannot be empty");
        let size;
        {
            let guard = self.state.read();
            guard.active_memtable.put(key, b"")?;
            size = guard.active_memtable.approximate_size();
        }
        self.try_freeze(size)?;

        Ok(())
    }


    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<FusedIterator<LsmIterator>>{
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let mut iters = Vec::with_capacity(
            snapshot.readonly_memtables.len() + 1 /* activate mem_table */);
        iters.push(Box::new(snapshot.active_memtable.scan(lower, upper)));
        for imm_memtable in &snapshot.readonly_memtables {
            iters.push(Box::new(imm_memtable.scan(lower, upper)));
        }
        let mem_table_iters = MergeIterator::create(iters);
        let mut iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for l0_table_id in &snapshot.l0_sstables{
            let table = snapshot.sstables.get(&l0_table_id).unwrap();
            if Self::check_range(
                lower,
                upper,
                table.first_key(),
                table.last_key()
            ){
                let iter = match lower {
                    Bound::Included(key) => {
                        SsTableIterator::create_and_seek_to_key(Arc::clone(table), Bytes::copy_from_slice(key))?
                    },
                    Bound::Excluded(key) => {
                        let mut iter_inner =
                            SsTableIterator::create_and_seek_to_key(Arc::clone(table),
                                                                    Bytes::copy_from_slice(key))?;
                        if iter_inner.is_valid() && iter_inner.key() == key {
                            iter_inner.next()?;
                        }
                        iter_inner
                    },
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(Arc::clone(table))?
                };
                iters.push(Box::new(iter));
            }
        }
        let l0_sstable_iters = MergeIterator::create(iters);
        let end_bound = map_bound(upper);
        Ok(FusedIterator::new(LsmIterator::new(TwoMergeIterator::create(mem_table_iters, l0_sstable_iters)?, end_bound)?))
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
        //let mut guard = self.state.write();
        let memtable = Arc::new(MemTable::create(self.next_sst_id()));
        self.freeze_memtable_with_memtable(memtable)?;
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
        //old_memtable.sync_wal()?;

        Ok(())
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf{
        Self::path_of_sst_static(&self.path, id)
    }

    // 强制将最早的 memtable 转入 L0 层
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
        let sstable = Arc::new(ss_table_builder.build(sst_id, self.path_of_sst(sst_id))?);
        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            let old_memtable = snapshot.readonly_memtables.pop().unwrap();
            assert_eq!(old_memtable.id(), sstable.sst_id());
            snapshot.l0_sstables.insert(0, sstable.sst_id());
            snapshot.sstables.insert(sstable.sst_id(), sstable);
            *guard = Arc::new(snapshot);
        }
        Ok(())
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
    use crate::engines::lsm::iterators::StorageIterator;
    use crate::engines::lsm::storage::{LsmStorage, LsmStorageInner};
    use crate::engines::lsm::storage::option::LsmStorageOptions;
    use crate::engines::lsm::table::builder::SsTableBuilder;
    use crate::engines::lsm::table::SsTable;
    use crate::engines::lsm::utils::{check_iter_result_by_key, check_lsm_iter_result_by_key, construct_merge_iterator_over_storage, sync};

    #[test]
    fn test_storage_integration() {
        let dir = tempdir().unwrap();
        let storage =
            LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_week1_test()).unwrap();
        assert_eq!(&storage.get(b"0").unwrap(), &None);
        storage.put(b"1", b"233").unwrap();
        storage.put(b"2", b"2333").unwrap();
        storage.put(b"3", b"23333").unwrap();
        assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"233");
        assert_eq!(&storage.get(b"2").unwrap().unwrap()[..], b"2333");
        assert_eq!(&storage.get(b"3").unwrap().unwrap()[..], b"23333");
        storage.delete(b"2").unwrap();
        assert!(storage.get(b"2").unwrap().is_none());
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
        assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"233333");
        assert_eq!(&storage.get(b"2").unwrap(), &None);
        assert_eq!(&storage.get(b"3").unwrap().unwrap()[..], b"233333");
        assert_eq!(&storage.get(b"4").unwrap().unwrap()[..], b"23333");
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
            println!("key:{:?}, value: {:?}", String::from_utf8(iter.key().to_vec()), String::from_utf8(iter.value().to_vec()));
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
            builder.add(&key[..], &value[..]);
        }
        builder.build(id, path.as_ref()).unwrap()
    }

    #[test]
    fn test_task2_storage_scan() {
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
    fn test_task3_storage_get() {
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
    fn test_task1_storage_scan() {
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
    fn test_task1_storage_get() {
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
    fn test_task2_auto_flush() {
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

    #[test]
    fn test_full_compaction() {
        let dir = tempdir().unwrap();
        let storage =
            Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default_for_week1_test()).unwrap());
        storage.put(b"0", b"v1").unwrap();
        sync(&storage);
        storage.put(b"0", b"v2").unwrap();
        storage.put(b"1", b"v2").unwrap();
        storage.put(b"2", b"v2").unwrap();
        sync(&storage);
        storage.delete(b"0").unwrap();
        storage.delete(b"2").unwrap();
        sync(&storage);
        assert_eq!(storage.state.read().l0_sstables.len(), 3);
        let mut iter = construct_merge_iterator_over_storage(&storage.state.read());
        check_iter_result_by_key(
            &mut iter,
            vec![
                (Bytes::from_static(b"0"), Bytes::from_static(b"")),
                (Bytes::from_static(b"1"), Bytes::from_static(b"v2")),
                (Bytes::from_static(b"2"), Bytes::from_static(b"")),
            ],
        );
        storage.force_full_compaction().unwrap();
        assert!(storage.state.read().l0_sstables.is_empty());
        let mut iter = construct_merge_iterator_over_storage(&storage.state.read());
        check_iter_result_by_key(
            &mut iter,
            vec![(Bytes::from_static(b"1"), Bytes::from_static(b"v2"))],
        );
        storage.put(b"0", b"v3").unwrap();
        storage.put(b"2", b"v3").unwrap();
        sync(&storage);
        storage.delete(b"1").unwrap();
        sync(&storage);
        let mut iter = construct_merge_iterator_over_storage(&storage.state.read());
        check_iter_result_by_key(
            &mut iter,
            vec![
                (Bytes::from_static(b"0"), Bytes::from_static(b"v3")),
                (Bytes::from_static(b"1"), Bytes::from_static(b"")),
                (Bytes::from_static(b"2"), Bytes::from_static(b"v3")),
            ],
        );
        storage.force_full_compaction().unwrap();
        assert!(storage.state.read().l0_sstables.is_empty());
        let mut iter = construct_merge_iterator_over_storage(&storage.state.read());
        check_iter_result_by_key(
            &mut iter,
            vec![
                (Bytes::from_static(b"0"), Bytes::from_static(b"v3")),
                (Bytes::from_static(b"2"), Bytes::from_static(b"v3")),
            ],
        );
    }
}

