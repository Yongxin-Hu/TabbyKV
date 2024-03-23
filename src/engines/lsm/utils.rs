use std::collections::Bound;
use anyhow::bail;
use bytes::Bytes;
use crate::engines::lsm::iterators::merge_iterator::MergeIterator;
use crate::engines::lsm::iterators::StorageIterator;
use crate::engines::lsm::key::{KeyBytes, KeySlice, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::engines::lsm::storage::LsmStorageInner;
use crate::engines::lsm::storage::state::LsmStorageState;
use crate::engines::lsm::table::iterator::SsTableIterator;
use anyhow::Result;
#[inline]
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

// TODO will be remove
pub(crate) fn map_bound_for_test(bound: Bound<&[u8]>, is_lower: bool) -> Bound<KeySlice> {
    match bound {
        Bound::Included(x) => {
            if is_lower{
                return Bound::Included(KeySlice::from_slice(x, TS_RANGE_BEGIN));
            }else{
                return Bound::Included(KeySlice::from_slice(x, TS_RANGE_END));
            }
        },
        Bound::Excluded(x) => {
            if is_lower{
                return Bound::Excluded(KeySlice::from_slice(x, TS_RANGE_BEGIN));
            }else{
                return Bound::Excluded(KeySlice::from_slice(x, TS_RANGE_END));
            }
        },
        Bound::Unbounded => Bound::Unbounded,
    }
}


pub(crate) fn map_key_bound(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(x) => Bound::Included(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x.key_ref()),
            x.ts(),
        )),
        Bound::Excluded(x) => Bound::Excluded(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x.key_ref()),
            x.ts(),
        )),
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
    type KeyType<'a> = KeySlice<'a>;

    fn next(&mut self) -> Result<()> {
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

    fn key(&self) -> KeySlice {
        if let Some(error_when) = self.error_when {
            if self.index >= error_when {
                panic!("invalid access after next returns an error!");
            }
        }
        KeySlice::for_testing_from_slice_no_ts(self.data[self.index].0.as_ref())
    }

    fn value(&self) -> &[u8] {
        if let Some(error_when) = self.error_when {
            if self.index >= error_when {
                panic!("invalid access after next returns an error!");
            }
        }
        self.data[self.index].1.as_ref()
    }

    fn is_valid(&self) -> bool {
        if let Some(error_when) = self.error_when {
            if self.index >= error_when {
                panic!("invalid access after next returns an error!");
            }
        }
        self.index < self.data.len()
    }
}

pub fn as_bytes(x: &[u8]) -> Bytes {
    Bytes::copy_from_slice(x)
}

pub fn check_iter_result_by_key<I>(iter: &mut I, expected: Vec<(Bytes, Bytes)>)
    where
        I: for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
{
    for (k, v) in expected {
        assert!(iter.is_valid());
        assert_eq!(
            k,
            iter.key().for_testing_key_ref(),
            "expected key: {:?}, actual key: {:?}",
            k,
            as_bytes(iter.key().key_ref()),
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
        I: for<'a> StorageIterator<KeyType<'a> = &'a [u8]>,
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
            SsTableIterator::create_and_move_to_first(state.sstables.get(t).cloned().unwrap())
                .unwrap(),
        ));
    }
    for (_, files) in &state.levels {
        for f in files {
            iters.push(Box::new(
                SsTableIterator::create_and_move_to_first(state.sstables.get(f).cloned().unwrap())
                    .unwrap(),
            ));
        }
    }
    MergeIterator::create(iters)
}