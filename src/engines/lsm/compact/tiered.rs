use serde::{Deserialize, Serialize};

use crate::engines::lsm::storage::state::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    ///
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    /// 根据当前 state 快照生成合并任务
    /// # 参数
    /// * `snapshot` state快照
    /// # 返回值
    /// None : 无需 compaction
    /// Some(task) : 需要执行 TieredCompactionTask
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {

        assert!(snapshot.l0_sstables.is_empty(), "tiered compaction should not use l0 level!");
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }
        // 对于 sorted run [R1, R2, R3, ..., Rn]
        // size amplification ratio = (size(R1) + size(R2) + ... size(Rn-1)) / size(Rn)

        let mut size = 0;
        for id in 0..(snapshot.levels.len() - 1) {
            size += snapshot.levels[id].1.len();
        }
        let space_amp_ratio =
            (size as f64) / (snapshot.levels.last().unwrap().1.len() as f64) * 100.0;
        if space_amp_ratio >= self.options.max_size_amplification_percent as f64 {
            println!(
                "compaction triggered by space amplification ratio: {}",
                space_amp_ratio
            );
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        unimplemented!()
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &TieredCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        unimplemented!()
    }
}
