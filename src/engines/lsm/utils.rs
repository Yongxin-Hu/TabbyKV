use std::collections::Bound;
use anyhow::bail;
use bytes::Bytes;
use crate::engines::lsm::iterators::StorageIterator;

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
