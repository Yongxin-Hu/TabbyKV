#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};
use crate::engines::lsm::iterators::concat_iterator::SstConcatIterator;
use crate::engines::lsm::iterators::merge_iterator::MergeIterator;
use crate::engines::lsm::iterators::StorageIterator;
use crate::engines::lsm::iterators::two_merge_iterator::TwoMergeIterator;
use crate::engines::lsm::key::KeySlice;
use crate::engines::lsm::manifest::ManifestRecord;
use crate::engines::lsm::storage::LsmStorageInner;
use crate::engines::lsm::storage::option::CompactionOptions;
use crate::engines::lsm::storage::state::LsmStorageState;
use crate::engines::lsm::table::builder::SsTableBuilder;
use crate::engines::lsm::table::iterator::SsTableIterator;
use crate::engines::lsm::table::SsTable;

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(&snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(&snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(&snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(&snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(&snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(&snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        if let Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction = self {
            true
        } else {
            false
        }
    }
}

impl LsmStorageInner {

    fn compact_sst_by_merge_iter(&self,
                                    mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
                                    compact_to_bottom_level: bool)
    -> Result<Vec<Arc<SsTable>>>
    {
        let mut sst_builder = None;
        let mut sst = Vec::new();

        while iter.is_valid() {
            if sst_builder.is_none(){
                sst_builder = Some(SsTableBuilder::new(self.options.block_size));
            }
            let builder_inner = sst_builder.as_mut().unwrap();
            if compact_to_bottom_level{
                if !iter.value().is_empty(){
                    builder_inner.add(iter.key(), iter.value());
                }
            } else {
                builder_inner.add(iter.key(), iter.value());
            }

            iter.next()?;

            if builder_inner.estimated_size() >= self.options.target_sst_size {
                let sst_id = self.next_sst_id();
                let builder = sst_builder.take().unwrap();
                sst.push(
                    Arc::new(builder.build(
                        sst_id, Some(self.block_cache.clone()),self.path_of_sst(sst_id)
                    )?)
                )
            }
        }
        if let Some(builder) = sst_builder{
            let sst_id = self.next_sst_id();
            sst.push(
                Arc::new(builder.build(
                    sst_id, Some(self.block_cache.clone()), self.path_of_sst(sst_id)
                )?)
            )
        }
        Ok(sst)
    }

    /// 根据 CompactionTask 执行 compact, 返回新生成的 sstables
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables
            } => {
                let mut l0_iters = Vec::with_capacity(l0_sstables.len());
                for sst_id in l0_sstables{
                    l0_iters.push(Box::new(SsTableIterator::create_and_move_to_first(
                        snapshot.sstables.get(sst_id).unwrap().clone(),
                    )?));
                }
                let mut l1_ssts = Vec::with_capacity(l1_sstables.len());
                for sst_id in l1_sstables{
                    l1_ssts.push(snapshot.sstables.get(sst_id).unwrap().clone());
                }
                // 由于 L1 层 sstable 的 Key 已经没有重叠了，使用 SstConcatIterator 来减少不必要的 read_block
                let iters = TwoMergeIterator::create(
                    MergeIterator::create(l0_iters), SstConcatIterator::create_and_seek_to_first(l1_ssts)?)?;
                self.compact_sst_by_merge_iter(iters, task.compact_to_bottom_level())
            },
            CompactionTask::Simple(SimpleLeveledCompactionTask{
                                       upper_level,
                                       upper_level_sst_ids,
                                       lower_level,
                                       lower_level_sst_ids,
                                       ..
            }) => {
                match upper_level {
                    // L0 -> L1 compaction
                    None => {
                        let mut l0_iters = Vec::with_capacity(upper_level_sst_ids.len());
                        for sst_id in upper_level_sst_ids{
                            l0_iters.push(Box::new(SsTableIterator::create_and_move_to_first(
                                snapshot.sstables.get(sst_id).unwrap().clone(),
                            )?));
                        }
                        let mut l1_ssts = Vec::with_capacity(lower_level_sst_ids.len());
                        for sst_id in lower_level_sst_ids{
                            l1_ssts.push(snapshot.sstables.get(sst_id).unwrap().clone());
                        }
                        let iters = TwoMergeIterator::create(
                            MergeIterator::create(l0_iters), SstConcatIterator::create_and_seek_to_first(l1_ssts)?)?;
                        self.compact_sst_by_merge_iter(iters, task.compact_to_bottom_level())
                    },
                    Some(_) => {
                        let mut upper_ssts = Vec::with_capacity(upper_level_sst_ids.len());
                        for sst_id in upper_level_sst_ids{
                            upper_ssts.push(snapshot.sstables.get(sst_id).unwrap().clone());
                        }
                        let mut lower_ssts = Vec::with_capacity(lower_level_sst_ids.len());
                        for sst_id in lower_level_sst_ids{
                            lower_ssts.push(snapshot.sstables.get(sst_id).unwrap().clone());
                        }
                        let iters = TwoMergeIterator::create(
                            SstConcatIterator::create_and_seek_to_first(upper_ssts)?,
                            SstConcatIterator::create_and_seek_to_first(lower_ssts)?
                        )?;
                        self.compact_sst_by_merge_iter(iters, task.compact_to_bottom_level())
                    }
                }
            },
            _ => unreachable!()
        }
    }

    ///
    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let l0_sstables = snapshot.l0_sstables.clone();
        let l1_sstables = snapshot.levels[0].1.clone();
        // 进行 L0 和 L1 的 FullCompaction
        let compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone()
        };

        let sstables = self.compact(&compaction_task)?;
        let mut new_l1_sst_ids = Vec::with_capacity(sstables.len());

        // 开始写入 LsmStorageState
        {
            let state_lock = self.state_lock.lock();
            // 使用 read 防止阻塞其他 state 读取
            let mut state = self.state.read().as_ref().clone();
            // 删除旧的 L0 层和 L1 层的 sstables
            for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
                let result = state.sstables.remove(sst);
                assert!(result.is_some());
            }
            // 将 compact 后的 sstables 加入 LsmStorageState
            for sst in sstables {
                new_l1_sst_ids.push(sst.sst_id());
                let r = state.sstables.insert(sst.sst_id(), sst);
                assert!(r.is_none())
            }
            // 确保执行 compact 时,LsmStorageState的 level1 没有被修改
            assert_eq!(state.levels[0].1, l1_sstables);
            // 将新的 L1 sstable id 写入 state
            state.levels[0].1 = new_l1_sst_ids.clone();
            // 确保 flush 线程没有改变 state.l0_sstables
            let mut l0_sstables_map = l0_sstables.iter().copied().collect::<HashSet<_>>();
            state.l0_sstables = state
                .l0_sstables
                .iter()
                .filter(|x| !l0_sstables_map.remove(x))
                .copied()
                .collect::<Vec<_>>();
            assert!(l0_sstables_map.is_empty());

            *self.state.write() = Arc::new(state);
            self.sync_dir()?;
        }
        // 删除旧的 sstables 文件
        for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*sst))?;
        }
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let generated_task = self.compaction_controller.generate_compaction_task(&snapshot);
        let Some(task) = generated_task else {
            return Ok(());
        };

        let sstables = self.compact(&task)?;
        let sst_ids: Vec<usize> = sstables.iter().map(|table| table.sst_id()).collect();

        // 移除旧的 sstable 以及 修改 state 中的记录
        let sst_to_remove = {
            let state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();
            let mut new_sst_ids = Vec::new();
            // 更新 snapshot 中的 level，sstable，[l0_sstables]，修改文件，删除旧的文件
            for table in sstables {
                new_sst_ids.push(table.sst_id());
                // 向 snapshot 的 sstables 加入新的 sstable
                let old_sst = snapshot.sstables.insert(table.sst_id(), table);
                // sstables 中不应该已经存在相同 sst_id 的 sstable
                assert!(old_sst.is_none());
            }
            // 更新 snapshot 中的 level，[l0_sstables]
            let (mut snapshot, sst_id_to_remove) =
                self.compaction_controller.apply_compaction_result(&snapshot, &task, sst_ids.as_slice());
            // 删除 snapshot 的 sstables 中旧的 sstable
            let mut sst_to_remove = Vec::with_capacity(sst_id_to_remove.len());
            for old_sst_id in sst_id_to_remove{
                let old_sst = snapshot.sstables.remove(&old_sst_id);
                assert!(old_sst.is_some());
                sst_to_remove.push(old_sst.unwrap());
            }
            // 将 snapshot 写回 self.state
            let mut state = self.state.write();
            *state = Arc::new(snapshot);
            // 释放写锁
            drop(state);
            self.sync_dir()?;
            // 记录 manifest
            if let Some(manifest) = &self.manifest{
                manifest.add_record(&state_lock, ManifestRecord::Compaction(task, new_sst_ids))?;
            }
            sst_to_remove
        };
        // 删除旧的 sst 文件
        for old_sst in sst_to_remove{
            std::fs::remove_file(self.path_of_sst(old_sst.sst_id()))?;
        }
        self.sync_dir()?;
        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        if {
            let state = self.state.read();
            state.readonly_memtables.len() >= self.options.num_memtable_limit
        }{
            self.force_flush_earliest_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        return Ok(Some(handle));
    }
}
