use std::sync::Arc;
use std::collections::HashMap;
use crate::engines::lsm::compact::{CompactionOptions, LeveledCompactionOptions, SimpleLeveledCompactionOptions};
use crate::engines::lsm::mem_table::MemTable;
use crate::engines::lsm::storage::option::LsmStorageOptions;
use crate::engines::lsm::table::SsTable;

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
    pub fn create(options: &LsmStorageOptions) -> Self {
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
