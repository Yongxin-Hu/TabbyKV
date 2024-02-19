use std::collections::{Bound, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use parking_lot::{Mutex, MutexGuard, RwLock};
use crate::engines::lsm::mem_table::MemTable;
use crate::engines::lsm::table::SsTable;
use crate::engines::lsm::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use anyhow::{Context, Result};
use bytes::Bytes;
use crate::engines::lsm::iterators::fused_iterator::FusedIterator;
use crate::engines::lsm::iterators::lsm_iterator::LsmIterator;
use crate::engines::lsm::iterators::merge_iterator::MergeIterator;
use crate::engines::lsm::iterators::StorageIterator;
use crate::engines::lsm::iterators::two_merge_iterator::TwoMergeIterator;
use crate::engines::lsm::table::builder::SsTableBuilder;
use crate::engines::lsm::table::iterator::SsTableIterator;
use crate::engines::lsm::utils::map_bound;

// LSM-tree 状态
#[derive(Clone)]
pub struct LsmStorageState {
    /// 当前活跃的 mem_table
    pub active_memtable: Arc<MemTable>,
    /// 只读的 mem_table
    pub readonly_memtables: Vec<Arc<MemTable>>,
    /// L0 layer sstables's id
    pub l0_sstables: Vec<usize>,
    /// L1 - Lmax layer sstables, (layer, Vec(sstable id))
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

impl LsmStorageState{
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            active_memtable: Arc::new(MemTable::create(0)),
            readonly_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
        }
    }
}
/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
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
            l0_iter.push(Box::new(SsTableIterator::create_and_seek_to_key(
                table,
                Bytes::copy_from_slice(key)
            )?));
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
        // Swap the current memtable with a new one.
        let mut snapshot = guard.as_ref().clone();
        let old_memtable = std::mem::replace(&mut snapshot.active_memtable, memtable);
        // Add the memtable to the immutable memtables.
        snapshot.readonly_memtables.insert(0, old_memtable.clone());
        // Update the snapshot.
        *guard = Arc::new(snapshot);

        drop(guard);
        //old_memtable.sync_wal()?;

        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    fn path_of_sst(&self, id: usize) -> PathBuf{
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
    use bytes::Bytes;
    use tempfile::tempdir;
    use crate::engines::lsm::iterators::StorageIterator;
    use crate::engines::lsm::lsm_storage::{LsmStorageInner, LsmStorageOptions};
    use crate::engines::lsm::table::builder::SsTableBuilder;
    use crate::engines::lsm::table::SsTable;
    use crate::engines::lsm::utils::{check_lsm_iter_result_by_key, sync};

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
                    (Bytes::from_static(b"2"), Bytes::from_static(b"")),
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
                    (Bytes::from_static(b"2"), Bytes::from_static(b"")),
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


}

