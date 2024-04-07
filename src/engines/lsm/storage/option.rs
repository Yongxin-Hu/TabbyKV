use crate::engines::lsm::compact::{LeveledCompactionOptions, SimpleLeveledCompactionOptions, TieredCompactionOptions};

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block 大小 (Byte)
    pub block_size: usize,
    // SST 文件大小 (Byte)，同时也是 memtable的大小
    pub target_sst_size: usize,
    // 内存中的 memtable 的数量限制(activate+read_only)
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    // 是否启用 WAL
    pub enable_wal: bool,
    pub serializable: bool,
}

impl Default for LsmStorageOptions{
    fn default() -> Self {
        LsmStorageOptions {
            block_size: 4096,   // 4M
            target_sst_size: 2 << 20,   // 2M
            num_memtable_limit: 100,
            compaction_options: Default::default(),
            enable_wal: true,
            serializable: false,
        }
    }
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// 仅 flush 到 L0 层
    NoCompaction,
}

impl Default for CompactionOptions {
    fn default() -> Self {
        CompactionOptions::Simple(SimpleLeveledCompactionOptions{
            size_ratio_percent: 10,
            level0_file_num_compaction_trigger: 10,
            max_levels: 7
        })
    }
}
