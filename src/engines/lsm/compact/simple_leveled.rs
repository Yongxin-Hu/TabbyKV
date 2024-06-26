use std::collections::HashSet;
use serde::{Deserialize, Serialize};

use crate::engines::lsm::storage::state::LsmStorageState;

#[derive(Debug, Clone, Serialize, Deserialize)]
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

    /// 根据当前 state 快照生成合并任务
    /// # 参数
    /// * `snapshot` state快照
    /// # 返回值
    /// None : 无需 compaction
    /// Some(task) : 需要执行 SimpleLeveledCompactionTask
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
                    upper_level: if layer == 0 { None } else { Some(layer) },
                    upper_level_sst_ids: if layer == 0 {
                        snapshot.l0_sstables.clone()
                    } else {
                        snapshot.levels[layer - 1].1.clone()
                    },
                    lower_level: lower_layer,
                    lower_level_sst_ids: snapshot.levels[lower_layer - 1].1.clone(),
                    is_lower_level_bottom_level: lower_layer == self.options.max_levels,
                });
            }
        }
        None
    }

    /// 应用合并的结果，修改 l0_sstable 和 level
    /// # 参数
    /// * `snapshot`: state快照
    /// * `task`: SimpleLeveledCompactionTask
    /// * `output`: compaction 后生成的 sst_ids
    /// # 返回值
    /// (LsmStorageState, Vec<usize>) : 修改后的state, 需要移除的 sst_ids
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut files_to_remove = Vec::new();
        match task.upper_level{
            // L0->L1 compaction
            None => {
                files_to_remove.extend(&task.upper_level_sst_ids);
                // 不直接使用 snapshot的 l0_sstables, 避免对新 flush 到 L0 的 sstable 误操作
                let mut l0_ssts_compacted = task
                    .upper_level_sst_ids
                    .iter()
                    .copied()
                    .collect::<HashSet<usize>>();
                let new_l0_sstables = snapshot
                    .l0_sstables
                    .iter()
                    .copied()
                    .filter(|x| !l0_ssts_compacted.remove(x))
                    .collect::<Vec<usize>>();
                assert!(l0_ssts_compacted.is_empty());
                snapshot.l0_sstables = new_l0_sstables;
            },
            Some(upper_layer) => {
                assert_eq!(
                    task.upper_level_sst_ids,
                    snapshot.levels[upper_layer - 1].1,
                    "sst mismatched"
                );
                files_to_remove.extend(&snapshot.levels[upper_layer - 1].1);
                snapshot.levels[upper_layer - 1].1.clear();
            }
        }
        assert_eq!(
            task.lower_level_sst_ids,
            snapshot.levels[task.lower_level - 1].1,
            "sst mismatched"
        );
        files_to_remove.extend(&snapshot.levels[task.lower_level - 1].1);
        snapshot.levels[task.lower_level - 1].1 = output.to_vec();
        (snapshot, files_to_remove)
    }
}
