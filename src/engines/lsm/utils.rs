use std::collections::Bound;
use std::os::windows::fs::MetadataExt;
use std::path::Path;
use anyhow::bail;
use bytes::Bytes;
use crate::engines::lsm::iterators::merge_iterator::MergeIterator;
use crate::engines::lsm::iterators::StorageIterator;
use crate::engines::lsm::storage::LsmStorageInner;
use crate::engines::lsm::storage::state::LsmStorageState;
use crate::engines::lsm::table::iterator::SsTableIterator;

pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

#[derive(Clone)]
pub struct MockIterator {
    pub data: Vec<(Bytes, Bytes)>,
    pub error_when: Option<usize>,
    pub index: usize,
}

impl MockIterator {
    pub fn new(data: Vec<(Bytes, Bytes)>) -> Self {
        Self {
            data,
            index: 0,
            error_when: None,
        }
    }

    pub fn new_with_error(data: Vec<(Bytes, Bytes)>, error_when: usize) -> Self {
        Self {
            data,
            index: 0,
            error_when: Some(error_when),
        }
    }
}

impl StorageIterator for MockIterator {
    fn value(&self) -> &[u8] {
        if let Some(error_when) = self.error_when {
            if self.index >= error_when {
                panic!("invalid access after next returns an error!");
            }
        }
        self.data[self.index].1.as_ref()
    }

    fn key(&self) -> &[u8] {
        if let Some(error_when) = self.error_when {
            if self.index >= error_when {
                panic!("invalid access after next returns an error!");
            }
        }
        self.data[self.index].0.as_ref()
    }

    fn is_valid(&self) -> bool {
        if let Some(error_when) = self.error_when {
            if self.index >= error_when {
                panic!("invalid access after next returns an error!");
            }
        }
        self.index < self.data.len()
    }

    fn next(&mut self) -> anyhow::Result<()> {
        if self.index < self.data.len() {
            self.index += 1;
        }
        if let Some(error_when) = self.error_when {
            if self.index == error_when {
                bail!("fake error!");
            }
        }
        Ok(())
    }
}

pub fn as_bytes(x: &[u8]) -> Bytes {
    Bytes::copy_from_slice(x)
}

pub fn check_iter_result_by_key<I>(iter: &mut I, expected: Vec<(Bytes, Bytes)>)
    where
        I: StorageIterator,
{
    for (k, v) in expected {
        assert!(iter.is_valid());
        assert_eq!(
            k,
            iter.key(),
            "expected key: {:?}, actual key: {:?}",
            k,
            as_bytes(iter.key()),
        );
        assert_eq!(
            v,
            iter.value(),
            "expected value: {:?}, actual value: {:?}",
            v,
            as_bytes(iter.value()),
        );
        iter.next().unwrap();
    }
    assert!(!iter.is_valid());
}

pub fn check_lsm_iter_result_by_key<I>(iter: &mut I, expected: Vec<(Bytes, Bytes)>)
    where
        I: StorageIterator,
{
    for (k, v) in expected {
        assert!(iter.is_valid());
        assert_eq!(
            k,
            iter.key(),
            "expected key: {:?}, actual key: {:?}, value:{:?}",
            k,
            as_bytes(iter.key()),
            as_bytes(iter.value())
        );
        assert_eq!(
            v,
            iter.value(),
            "expected value: {:?}, actual value: {:?}",
            v,
            as_bytes(iter.value()),
        );
        iter.next().unwrap();
    }
    assert!(!iter.is_valid());
}

pub fn sync(storage: &LsmStorageInner) {
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();
    storage.force_flush_earliest_memtable().unwrap();
}

pub fn construct_merge_iterator_over_storage(
    state: &LsmStorageState,
) -> MergeIterator<SsTableIterator> {
    let mut iters = Vec::new();
    for t in &state.l0_sstables {
        iters.push(Box::new(
            SsTableIterator::create_and_seek_to_first(state.sstables.get(t).cloned().unwrap())
                .unwrap(),
        ));
    }
    for (_, files) in &state.levels {
        for f in files {
            iters.push(Box::new(
                SsTableIterator::create_and_seek_to_first(state.sstables.get(f).cloned().unwrap())
                    .unwrap(),
            ));
        }
    }
    MergeIterator::create(iters)
}

pub fn dump_files_in_dir(path: impl AsRef<Path>) {
    println!("--- DIR DUMP ---");
    for f in path.as_ref().read_dir().unwrap() {
        let f = f.unwrap();
        print!("{}", f.path().display());
        println!(
            ", size={:.3}KB",
            f.metadata().unwrap().file_size() as f64 / 1024.0
        );
    }
}