use std::collections::Bound;
use std::sync::Arc;
use tempfile::tempdir;

fn test_task4_integration() {
    let dir = tempdir().unwrap();
    let storage = Arc::new(
        LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_week1_test()).unwrap(),
    );
    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage.put(b"3", b"23333").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();
    storage.delete(b"1").unwrap();
    storage.delete(b"2").unwrap();
    storage.put(b"3", b"2333").unwrap();
    storage.put(b"4", b"23333").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();
    storage.put(b"1", b"233333").unwrap();
    storage.put(b"3", b"233333").unwrap();
    {
        let mut iter = storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
        check_lsm_iter_result_by_key(
            &mut iter,
            vec![
                (Bytes::from_static(b"1"), Bytes::from_static(b"233333")),
                (Bytes::from_static(b"3"), Bytes::from_static(b"233333")),
                (Bytes::from_static(b"4"), Bytes::from_static(b"23333")),
            ],
        );
        assert!(!iter.is_valid());
        iter.next().unwrap();
        iter.next().unwrap();
        iter.next().unwrap();
        assert!(!iter.is_valid());
    }
    {
        let mut iter = storage
            .scan(Bound::Included(b"2"), Bound::Included(b"3"))
            .unwrap();
        check_lsm_iter_result_by_key(
            &mut iter,
            vec![(Bytes::from_static(b"3"), Bytes::from_static(b"233333"))],
        );
        assert!(!iter.is_valid());
        iter.next().unwrap();
        iter.next().unwrap();
        iter.next().unwrap();
        assert!(!iter.is_valid());
    }
}