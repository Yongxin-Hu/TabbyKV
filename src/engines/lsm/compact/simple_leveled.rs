use serde::{Deserialize, Serialize};

use crate::engines::lsm::storage::state::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    // （低一层的文件数量 / 高一层的文件数量）
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        // 记录每一层的 sst 文件数量
        let mut layer_size = Vec::with_capacity(snapshot.levels.len() + 1);
        layer_size.push(snapshot.l0_sstables.len());
        for (_, level_sst_id) in &snapshot.levels{
            layer_size.push(level_sst_id.len());
        }

        for layer in 0..self.options.max_levels {
            // L0 层需要达到 level0_file_num_compaction_trigger
            if layer == 0 && layer_size[layer] < self.options.level0_file_num_compaction_trigger{
                continue;
            }

            let lower_layer = layer + 1;
            let ratio = layer_size[lower_layer] as f64 / layer_size[layer] as f64;
            if ratio < self.options.size_ratio_percent as f64 / 100.0 {
                return Some(SimpleLeveledCompactionTask {
                    upper_level: if layer == 0 { None } else { Some(i) },
                    upper_level_sst_ids: if layer == 0 {
                        snapshot.l0_sstables.clone()
                    } else {
                        snapshot.levels[layer - 1].1.clone()
                    },
                    lower_level,
                    lower_level_sst_ids: snapshot.levels[lower_layer - 1].1.clone(),
                    is_lower_level_bottom_level: lower_layer == self.options.max_levels,
                });
            }
        }
        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &SimpleLeveledCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        unimplemented!()
    }
}
