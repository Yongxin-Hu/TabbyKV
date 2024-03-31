#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::BTreeMap;

pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        *self.readers.entry(ts).or_default() += 1;
    }

    pub fn remove_reader(&mut self, ts: u64) {
        let count = self.readers.get_mut(&ts).unwrap();
        *count -= 1;
        if *count == 0 {
            self.readers.remove(&ts);
        }
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }

    pub fn watermark(&self) -> Option<u64> {
        self.readers.first_key_value().map(|(ts, _)| *ts)
    }
}

#[cfg(test)]
mod test {
    use crate::engines::lsm::mvcc::watermark::Watermark;

    #[test]
    fn test_watermark() {
        let mut watermark = Watermark::new();
        watermark.add_reader(0);
        for i in 1..=1000 {
            watermark.add_reader(i);
            assert_eq!(watermark.watermark(), Some(0));
            assert_eq!(watermark.num_retained_snapshots(), i as usize + 1);
        }
        let mut cnt = 1001;
        for i in 0..500 {
            watermark.remove_reader(i);
            assert_eq!(watermark.watermark(), Some(i + 1));
            cnt -= 1;
            assert_eq!(watermark.num_retained_snapshots(), cnt);
        }
        for i in (501..=1000).rev() {
            watermark.remove_reader(i);
            assert_eq!(watermark.watermark(), Some(500));
            cnt -= 1;
            assert_eq!(watermark.num_retained_snapshots(), cnt);
        }
        watermark.remove_reader(500);
        assert_eq!(watermark.watermark(), None);
        assert_eq!(watermark.num_retained_snapshots(), 0);
        watermark.add_reader(2000);
        watermark.add_reader(2000);
        watermark.add_reader(2001);
        assert_eq!(watermark.num_retained_snapshots(), 2);
        assert_eq!(watermark.watermark(), Some(2000));
        watermark.remove_reader(2000);
        assert_eq!(watermark.num_retained_snapshots(), 2);
        assert_eq!(watermark.watermark(), Some(2000));
        watermark.remove_reader(2000);
        assert_eq!(watermark.num_retained_snapshots(), 1);
        assert_eq!(watermark.watermark(), Some(2001));
    }
}
