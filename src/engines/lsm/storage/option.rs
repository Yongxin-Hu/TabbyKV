use crate::engines::lsm::compact::CompactionOptions;

pub struct LsmStorageOptions {
    // Block 大小 (Byte)
    pub block_size: usize,
    // SST 文件大小 (Byte)，同时也是 memtable的大小
    pub target_sst_size: usize,
    // 内存中的 memtable 的数量限制(activate+read_only)
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
